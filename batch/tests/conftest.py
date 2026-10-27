"""Shared local SparkSession for the batch tests (no MinIO needed)."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def spark():
    from batch.spark_session import get_spark

    s = get_spark("test-batch", master="local[2]", driver_memory="1g", s3=False)
    s.conf.set("spark.sql.shuffle.partitions", "4")
    yield s
    s.stop()
