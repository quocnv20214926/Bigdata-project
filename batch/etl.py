"""ETL: raw REES46 CSV -> clean Parquet partitioned by event_date (Pair 1, week 2).

Cleaning rules (findings in notebooks/01_eda.ipynb):
  - event_time "2019-10-01 00:00:00 UTC" -> timestamp (UTC); event_date = its date
  - a row is dropped, and counted by reason, when event_time cannot be parsed,
    event_type is not in common.schemas.EVENT_TYPES, product_id or user_id is missing
    or not a number, or event_time is outside --start/--end
  - category_code, brand, user_session: empty -> null, rows are kept
    (31.8% / 14.4% of October rows have no category_code / brand)
  - price 0 is kept (not used by ALS); no assumption that a cart precedes a purchase
  - exact duplicate rows are removed
Output (common/schemas.py): the raw columns, event_time as timestamp, plus event_date,
partitioned by event_date. Re-running replaces only the dates present in the input.

Usage (from the repository root):
  python -m batch.etl                                    # s3a://lake/raw/ -> s3a://lake/clean/events/
  python -m batch.etl --input data/raw/2019-Oct.csv --output data/clean/events --no-s3
  python -m batch.etl --report docs/etl-report.json      # also write the validation report
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from batch.spark_session import get_spark
from batch.split import TEST_END, TEST_START, TRAIN_END, TRAIN_START, split
from common.config import settings
from common.schemas import CLEAN_PARTITION_COLUMN, EVENT_TYPES, RAW_COLUMNS, spark_event_schema

RAW_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss 'UTC'"
DROP_REASON = "_drop_reason"


def read_raw(spark: SparkSession, paths: list[str]) -> DataFrame:
    # PERMISSIVE: a value that does not fit the schema becomes null and is caught below.
    return spark.read.csv(paths, header=True, schema=spark_event_schema(), mode="PERMISSIVE")


def annotate(raw: DataFrame, start: date | None = None, end: date | None = None) -> DataFrame:
    """Parse event_time and add the reason a row would be dropped (null = keep)."""
    df = raw.withColumn("event_time", F.to_timestamp("event_time", RAW_TIME_FORMAT))
    df = df.withColumn(CLEAN_PARTITION_COLUMN, F.to_date("event_time"))
    for c in ("category_code", "brand", "user_session"):
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))

    reason = (
        F.when(F.col("event_time").isNull(), "bad_event_time")
        .when(~F.col("event_type").isin(*EVENT_TYPES) | F.col("event_type").isNull(), "unknown_event_type")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("user_id").isNull(), "missing_user_id")
    )
    if start:
        reason = reason.when(F.col(CLEAN_PARTITION_COLUMN) < F.lit(start), "before_start")
    if end:
        reason = reason.when(F.col(CLEAN_PARTITION_COLUMN) > F.lit(end), "after_end")
    return df.withColumn(DROP_REASON, reason)


def count_drops(annotated: DataFrame) -> dict:
    rows = annotated.groupBy(DROP_REASON).count().collect()
    counts = {r[DROP_REASON] or "kept": r["count"] for r in rows}
    return {"rows_read": sum(counts.values()), "kept_before_dedup": counts.pop("kept", 0), "dropped": counts}


def clean(annotated: DataFrame) -> DataFrame:
    """Valid rows, without exact duplicates, one shuffle partition per date."""
    columns = list(RAW_COLUMNS) + [CLEAN_PARTITION_COLUMN]
    return (
        annotated.where(F.col(DROP_REASON).isNull())
        .select(*columns)
        # Hash-partitioning on event_date already clusters duplicates, so dropDuplicates
        # needs no second shuffle, and each date ends up in a single Parquet file.
        .repartition(CLEAN_PARTITION_COLUMN)
        .dropDuplicates()
        .sortWithinPartitions("event_time")
    )


def write_clean(df: DataFrame, output: str) -> None:
    df.write.mode("overwrite").partitionBy(CLEAN_PARTITION_COLUMN).parquet(output)


def summarize(df: DataFrame) -> dict:
    s = df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("product_id").alias("products"),
        F.min("event_time").cast("string").alias("first_event_time"),
        F.max("event_time").cast("string").alias("last_event_time"),
        F.sum(F.col("category_code").isNull().cast("int")).alias("null_category_code"),
        F.sum(F.col("brand").isNull().cast("int")).alias("null_brand"),
        F.sum(F.col("user_session").isNull().cast("int")).alias("null_user_session"),
        F.sum((F.col("price") == 0).cast("int")).alias("zero_price"),
    ).first().asDict()
    s["event_types"] = {r["event_type"]: r["count"] for r in df.groupBy("event_type").count().collect()}
    return s


def split_summary(df: DataFrame) -> dict:
    train, test = split(df)
    train_users = train.select("user_id").distinct()
    test_users = test.select("user_id").distinct()
    unseen = test_users.join(train_users, "user_id", "left_anti")
    n_test_users = test_users.count()
    n_unseen = unseen.count()
    test_buyers = test.where(F.col("event_type") == "purchase").select("user_id").distinct()
    n_buyers = test_buyers.count()
    n_unseen_buyers = test_buyers.join(train_users, "user_id", "left_anti").count()
    return {
        "train": {"from": str(TRAIN_START), "to": str(TRAIN_END), **summarize(train)},
        "test": {"from": str(TEST_START), "to": str(TEST_END), **summarize(test)},
        "test_users_not_in_train": n_unseen,
        "test_users_not_in_train_share": round(n_unseen / n_test_users, 4) if n_test_users else None,
        "test_buyers": n_buyers,
        "test_buyers_not_in_train": n_unseen_buyers,
    }


def run(spark: SparkSession, inputs: list[str], output: str, start: date | None = None,
        end: date | None = None, report: bool = True) -> dict:
    started = time.time()
    annotated = annotate(read_raw(spark, inputs), start, end)
    result: dict = {"input": inputs, "output": output, **count_drops(annotated)}
    write_clean(clean(annotated), output)
    result["write_seconds"] = round(time.time() - started, 1)

    written = spark.read.parquet(output)
    if start or end:
        lo, hi = start or date.min, end or date.max
        written = written.where(F.col(CLEAN_PARTITION_COLUMN).between(F.lit(lo), F.lit(hi)))
    result["clean"] = summarize(written)
    result["duplicates_removed"] = result["kept_before_dedup"] - result["clean"]["rows"]
    if report:
        result["split"] = split_summary(written)
    result["total_seconds"] = round(time.time() - started, 1)
    return result


def main() -> None:
    p = argparse.ArgumentParser(description="REES46 raw CSV -> clean Parquet partitioned by event_date")
    p.add_argument("--input", nargs="+", default=[settings.raw_path], help="CSV files or folders (local or s3a://)")
    p.add_argument("--output", default=settings.clean_path)
    p.add_argument("--start", type=date.fromisoformat, help="first event_date to keep (YYYY-MM-DD)")
    p.add_argument("--end", type=date.fromisoformat, help="last event_date to keep (YYYY-MM-DD)")
    p.add_argument("--report", help="write the validation report (JSON) to this path")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--no-split", action="store_true", help="skip the train/test summary")
    args = p.parse_args()

    spark = get_spark("batch-etl", s3=not args.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    result = run(spark, args.input, args.output, args.start, args.end, report=not args.no_split)
    text = json.dumps(result, indent=2, default=str)
    print(text)
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(text + "\n", encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
