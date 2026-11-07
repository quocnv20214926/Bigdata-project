"""Kafka JSON -> typed, validated events (week 3, owner D).

Input : DataFrame with a column "value" (Kafka value, or one line of a JSON
        file in STREAM_SOURCE=file mode).
Output: every column of the Kafka contract (common.schemas.spark_event_schema)
        plus
          event_ts  TIMESTAMP  event_time parsed as UTC
          epoch     BIGINT     seconds since 1970 (used in Redis values)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.schemas import EVENT_TYPES, spark_event_schema
from common.weights import EVENT_WEIGHTS

DEDUP_COLUMNS = ["user_id", "product_id", "event_type", "event_time", "user_session"]


def parse_events(raw: DataFrame) -> DataFrame:
    """JSON value -> typed columns. Malformed input gives nulls, never an error."""
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), spark_event_schema()).alias("e")
    ).select("e.*")
    return (
        parsed
        # Spark 4 runs in ANSI mode: a plain cast/to_timestamp of a malformed
        # string raises an error and would stop the whole streaming query.
        # try_cast returns null instead; valid_events() then drops the row.
        .withColumn("event_ts", F.expr("try_cast(event_time AS TIMESTAMP)"))
        .withColumn("epoch", F.unix_timestamp("event_ts"))
    )


def valid_events(events: DataFrame) -> DataFrame:
    return events.where(
        F.col("event_ts").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("event_type").isin(*EVENT_TYPES)
    )


def with_watermark_and_dedup(events: DataFrame, watermark: str) -> DataFrame:
    """Event-time watermark + removal of exact duplicate events (REES46 has some).

    The watermark bounds the de-duplication state. Events older than the
    watermark are dropped, so a full replay from the start needs a fresh
    checkpoint.
    """
    return events.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(DEDUP_COLUMNS)


def with_weight(events: DataFrame) -> DataFrame:
    """Add column "weight" from common.weights (view 1, cart 3, purchase 5)."""
    mapping = F.create_map(*[x for k, v in EVENT_WEIGHTS.items() for x in (F.lit(k), F.lit(float(v)))])
    return events.withColumn("weight", F.coalesce(mapping[F.col("event_type")], F.lit(0.0)))
