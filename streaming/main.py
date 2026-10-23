"""Spark Structured Streaming pipeline: Kafka -> Redis user state."""
import json
import logging
import os
import time
from typing import Iterable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, expr, when
from pyspark.sql.types import StringType

from common.config import settings
from common.schemas import spark_event_schema
from common.redis_keys import user_recent, user_bought, encode_recent, TTL

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("pair2-streaming")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", settings.kafka_bootstrap)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", settings.kafka_topic_events)
REDIS_HOST = os.getenv("REDIS_HOST", settings.redis_host)
REDIS_PORT = int(os.getenv("REDIS_PORT", settings.redis_port))
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
MAX_OFFSETS = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000"))
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "5"))
CHECKPOINT = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/pair2-events")
RECENT_N = int(os.getenv("RECENT_N", str(getattr(settings, "recent_n", 20))))

def _redis_client():
    import redis
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=int(os.getenv("REDIS_DB", getattr(settings, "redis_db", 0))), decode_responses=True)

def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Idempotent-ish Redis sink: sorted-set score is event epoch; sets have TTL."""
    if batch_df.rdd.isEmpty():
        return
    client = _redis_client()
    rows = batch_df.select(
        "user_id", "product_id", "event_type", "event_epoch", "event_time"
    ).where(col("user_id").isNotNull() & col("product_id").isNotNull()).collect()

    # A retry may replay a batch. ZADD uses a stable member, so it does not duplicate entries.
    # Redis updates are grouped per user to reduce round trips.
    grouped = {}
    for row in rows:
        uid = str(row["user_id"])
        pid = str(row["product_id"])
        event_type = str(row["event_type"] or "")
        epoch = int(row["event_epoch"] or 0)
        grouped.setdefault(uid, []).append((pid, event_type, epoch))

    pipe = client.pipeline()
    for uid, events in grouped.items():
        recent_key = user_recent(uid)
        bought_key = user_bought(uid)
        for pid, event_type, epoch in events:
            member = encode_recent(pid, event_type, epoch)
            pipe.zadd(recent_key, {member: epoch})
            if event_type.lower() in ("purchase", "purchased", "buy"):
                pipe.zadd(bought_key, {pid: epoch})
        pipe.zremrangebyrank(recent_key, 0, -(RECENT_N + 1))
        pipe.expire(recent_key, int(TTL.get("user:recent", 86400)))
        pipe.expire(bought_key, int(TTL.get("user:bought", 2592000)))
    pipe.execute()
    client.close()
    logger.info("batch=%s users=%s events=%s", batch_id, len(grouped), len(rows))

def main():
    spark = (
        SparkSession.builder
        .appName("pair2-kafka-redis-streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    schema = spark_event_schema()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("event")
    ).select("event.*")

    # Validate required fields and derive event-time columns before watermarking.
    events = (
        parsed
        .withColumn("event_time", to_timestamp(col("event_time")))
        .where(col("user_id").isNotNull() & col("product_id").isNotNull() & col("event_time").isNotNull())
        .withWatermark("event_time", os.getenv("EVENT_WATERMARK", "10 minutes"))
        .dropDuplicates(["user_id", "product_id", "event_type", "event_time", "user_session"])
        .withColumn("event_epoch", col("event_time").cast("long"))
    )

    query = (
        events.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
    logger.info("Started Kafka topic=%s checkpoint=%s", KAFKA_TOPIC, CHECKPOINT)
    query.awaitTermination()

if __name__ == "__main__":
    main()
