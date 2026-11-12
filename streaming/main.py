"""Speed layer: Spark Structured Streaming, Kafka -> Redis (Pair 2).

Two streaming queries share one parsed source:

  events    parse -> validate -> watermark + dedup -> foreachBatch
              user:recent, user:bought   (sinks/user_state.py, week 3)
              sim:cooc                   (sinks/cooc.py, week 4)
  trending  parse -> validate -> watermark -> 1 h window sliding every 5 min,
            summed event weights per (category, product), output mode update
              trend:cat:*, trend:global  (sinks/trending.py, week 4)

Each query has its own checkpoint under CHECKPOINT_LOCATION. Writes go to
Redis from the executors (foreachPartition); the driver only coordinates.

Environment variables (defaults in brackets):
  STREAM_SOURCE [kafka]  kafka, or file = JSON lines in INPUT_DIR (dev/tests)
  KAFKA_BOOTSTRAP, KAFKA_TOPIC_EVENTS, REDIS_HOST, REDIS_PORT, REDIS_DB
  STARTING_OFFSETS [earliest]   MAX_OFFSETS_PER_TRIGGER [50000]
  TRIGGER_SECONDS [5]           CHECKPOINT_LOCATION [/checkpoints/pair2]
  EVENT_WATERMARK [10 minutes]  TREND_WINDOW [1 hour]  TREND_SLIDE [5 minutes]
  SHUFFLE_PARTITIONS [6]        AVAILABLE_NOW [0]  (1 = process what exists, then stop)
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import settings
from streaming.parsing import parse_events, valid_events, with_watermark_and_dedup, with_weight
from streaming.redis_conn import get_redis
from streaming.sinks.cooc import write_partition_cooc
from streaming.sinks.trending import current_window_end, duration_seconds, publish, write_window_totals
from streaming.sinks.user_state import write_user_state

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pair2-streaming")

META_STREAM = "rt:meta:stream"   # progress of both queries (internal key)


@dataclass
class Config:
    source: str = field(default_factory=lambda: os.getenv("STREAM_SOURCE", "kafka"))
    input_dir: str = field(default_factory=lambda: os.getenv("INPUT_DIR", "data/stream_in"))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", settings.kafka_bootstrap))
    topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_EVENTS", settings.kafka_topic_events))
    starting_offsets: str = field(default_factory=lambda: os.getenv("STARTING_OFFSETS", "earliest"))
    max_offsets: int = field(default_factory=lambda: int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")))
    trigger_seconds: int = field(default_factory=lambda: int(os.getenv("TRIGGER_SECONDS", "5")))
    checkpoint: str = field(default_factory=lambda: os.getenv("CHECKPOINT_LOCATION", "/checkpoints/pair2"))
    watermark: str = field(default_factory=lambda: os.getenv("EVENT_WATERMARK", "10 minutes"))
    trend_window: str = field(default_factory=lambda: os.getenv("TREND_WINDOW", "1 hour"))
    trend_slide: str = field(default_factory=lambda: os.getenv("TREND_SLIDE", "5 minutes"))
    shuffle_partitions: int = field(default_factory=lambda: int(os.getenv("SHUFFLE_PARTITIONS", "6")))
    available_now: bool = field(default_factory=lambda: os.getenv("AVAILABLE_NOW", "0") == "1")

    def checkpoint_for(self, query: str) -> str:
        return f"{self.checkpoint.rstrip('/')}/{query}"


def build_spark(cfg: Config) -> SparkSession:
    return (
        SparkSession.builder.appName("pair2-speed-layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )


def read_source(spark: SparkSession, cfg: Config) -> DataFrame:
    if cfg.source == "file":
        return spark.readStream.option("maxFilesPerTrigger", 1).text(cfg.input_dir)
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


# ---- Batch-id guard (the cooc counters are not idempotent) --------------

def read_query_id(checkpoint: str) -> str:
    """Spark writes {checkpoint}/metadata = {"id": ...} before the first batch.

    The id survives restarts and changes when the checkpoint is deleted,
    exactly like the batch ids it scopes.
    """
    path = checkpoint[len("file:"):] if checkpoint.startswith("file:") else checkpoint
    try:
        with open(os.path.join(path, "metadata"), encoding="utf-8") as f:
            return json.loads(f.readline())["id"]
    except (OSError, ValueError, KeyError):
        return "unknown"


def last_batch_key(query_id: str) -> str:
    return f"rt:meta:last_batch:{query_id}"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---- Query 1: events -> user state + co-occurrence ----------------------

def _write_events_partition(rows) -> None:
    r = get_redis()
    rows = list(rows)            # one partition of one micro-batch (bounded by MAX_OFFSETS_PER_TRIGGER)
    write_user_state(rows, r)
    write_partition_cooc(rows, r)


def make_events_writer(cfg: Config):
    state = {"query_id": None}

    def process(df: DataFrame, batch_id: int) -> None:
        r = get_redis()
        if state["query_id"] is None:
            state["query_id"] = read_query_id(cfg.checkpoint_for("events"))
        guard = last_batch_key(state["query_id"])
        last = r.get(guard)
        if last is not None and int(last) >= batch_id:
            logger.info("events batch=%s already written, skipped", batch_id)
            return

        started = time.time()
        df = df.select("user_id", "product_id", "event_type", "epoch", "user_session", "event_time").persist()
        try:
            rows = df.count()
            max_time = None
            if rows:
                (df.repartition(cfg.shuffle_partitions, "user_id")
                   .sortWithinPartitions("user_id", "epoch")
                   .foreachPartition(_write_events_partition))
                max_time = df.agg(F.max("event_time")).first()[0]
            ms = int((time.time() - started) * 1000)
            pipe = r.pipeline(transaction=False)
            pipe.set(guard, batch_id)
            pipe.hset(META_STREAM, mapping={
                "events_query_id": state["query_id"], "events_batch_id": batch_id,
                "events_batch_rows": rows, "events_batch_ms": ms, "events_processed_at": _now(),
                **({"events_max_event_time": max_time} if max_time else {}),
            })
            pipe.hincrby(META_STREAM, "events_total_rows", rows)
            pipe.execute()
            logger.info("events batch=%s rows=%s max_event_time=%s %sms", batch_id, rows, max_time, ms)
        finally:
            df.unpersist()

    return process


# ---- Query 2: windowed trending ------------------------------------------

def _write_trend_partition(rows) -> None:
    write_window_totals(rows, get_redis())


def make_trending_writer(cfg: Config):
    window_s, slide_s = duration_seconds(cfg.trend_window), duration_seconds(cfg.trend_slide)

    def process(df: DataFrame, batch_id: int) -> None:
        started = time.time()
        df = df.select(
            F.unix_timestamp(F.col("window.end")).alias("window_end"), "category_id", "product_id", "score"
        ).persist()
        try:
            rows = df.count()
            published = None
            if rows:
                df.foreachPartition(_write_trend_partition)
                end = current_window_end(df.agg(F.max("window_end")).first()[0], window_s, slide_s)
                categories = [x.category_id for x in
                              df.where(F.col("window_end") == end).select("category_id").distinct().collect()]
                published = publish(end, categories, get_redis())
            ms = int((time.time() - started) * 1000)
            r = get_redis()
            r.hset(META_STREAM, mapping={
                "trending_batch_id": batch_id, "trending_updated_rows": rows, "trending_batch_ms": ms,
                "trending_processed_at": _now(),
                **({"trending_window_end": datetime.fromtimestamp(published, timezone.utc)
                    .strftime("%Y-%m-%dT%H:%M:%SZ")} if published else {}),
            })
            logger.info("trending batch=%s updated_rows=%s published_window_end=%s %sms", batch_id, rows, published, ms)
        finally:
            df.unpersist()

    return process


# ---- Wiring ----------------------------------------------------------------

def build_queries(spark: SparkSession, cfg: Config):
    base = valid_events(parse_events(read_source(spark, cfg)))

    def start(df: DataFrame, name: str, writer, mode: str = "append"):
        w = (df.writeStream.queryName(name).outputMode(mode).foreachBatch(writer)
               .option("checkpointLocation", cfg.checkpoint_for(name)))
        w = w.trigger(availableNow=True) if cfg.available_now else w.trigger(processingTime=f"{cfg.trigger_seconds} seconds")
        return w.start()

    events = with_watermark_and_dedup(base, cfg.watermark)

    # No dedup before the window: Spark does not chain two stateful operators
    # in update mode. Exact duplicates are rare and only add a little weight.
    trending = (
        with_weight(base)
        .where((F.col("weight") > 0) & F.col("category_id").isNotNull())
        .withWatermark("event_ts", cfg.watermark)
        .groupBy(F.window("event_ts", cfg.trend_window, cfg.trend_slide), "category_id", "product_id")
        .agg(F.sum("weight").alias("score"))
    )
    return [
        start(events, "events", make_events_writer(cfg)),
        start(trending, "trending", make_trending_writer(cfg), mode="update"),
    ]


def main() -> None:
    cfg = Config()
    spark = build_spark(cfg)
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    logger.info("speed layer starting: %s", {f.name: getattr(cfg, f.name) for f in fields(cfg)})
    queries = build_queries(spark, cfg)
    if cfg.available_now:
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
