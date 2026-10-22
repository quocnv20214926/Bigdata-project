"""Time-based train/test split (docs/plan.md, section 2).

train = 2019-10-01 .. 2019-11-15, test = 2019-11-16 .. 2019-11-30 (dates in UTC, inclusive).
The split is a filter on the event_date partition column of the clean Parquet,
so it reads only the needed partitions and never copies data.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, functions as F

from common.config import settings
from common.schemas import CLEAN_PARTITION_COLUMN

TRAIN_START = date(2019, 10, 1)
TRAIN_END = date(2019, 11, 15)
TEST_START = date(2019, 11, 16)
TEST_END = date(2019, 11, 30)


def is_train() -> Column:
    return F.col(CLEAN_PARTITION_COLUMN).between(F.lit(TRAIN_START), F.lit(TRAIN_END))


def is_test() -> Column:
    return F.col(CLEAN_PARTITION_COLUMN).between(F.lit(TEST_START), F.lit(TEST_END))


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    return df.where(is_train()), df.where(is_test())


def load_split(spark: SparkSession, path: str | None = None) -> tuple[DataFrame, DataFrame]:
    """(train, test) from the clean Parquet written by batch/etl.py."""
    return split(spark.read.parquet(path or settings.clean_path))
