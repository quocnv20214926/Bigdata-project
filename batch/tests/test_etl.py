"""Unit tests for the batch ETL and the train/test split (local Spark, no MinIO needed).

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

from datetime import date

import pytest

from batch.etl import count_drops, run
from batch.spark_session import get_spark
from batch.split import split
from common.schemas import CLEAN_PARTITION_COLUMN, RAW_COLUMNS

ROWS = [
    # event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
    ("2019-10-01 00:00:00 UTC", "view", "44600062", "2103807459595387724", "", "shiseido", "35.79", "541312140", "s1"),
    ("2019-10-01 00:00:00 UTC", "view", "44600062", "2103807459595387724", "", "shiseido", "35.79", "541312140", "s1"),  # duplicate
    ("2019-10-01 00:00:10 UTC", "purchase", "3900821", "2053013552326770905", "appliances.environment.water_heater", "aqua", "0", "554748717", "s2"),  # price 0, no cart before
    ("2019-10-01 00:00:20 UTC", "view", "bad_id", "1", "", "", "1.0", "1", "s3"),  # malformed product_id
    ("not a time", "view", "1", "1", "", "", "1.0", "1", "s4"),
    ("2019-10-01 00:00:30 UTC", "click", "1", "1", "", "", "1.0", "1", "s5"),  # unknown event type
    ("2019-11-15 23:59:59 UTC", "cart", "17200506", "2053013559792632471", "furniture.living_room.sofa", "", "543.10", "519107250", ""),
    ("2019-11-16 00:00:00 UTC", "view", "1307067", "2053013558920217191", "computers.notebook", "lenovo", "251.74", "550050854", "s6"),
    ("2019-11-16 00:00:05 UTC", "purchase", "1307067", "2053013558920217191", "computers.notebook", "lenovo", "251.74", "519107250", "s7"),
    ("2019-12-01 00:00:00 UTC", "view", "1", "1", "", "", "1.0", "1", "s8"),  # after --end
]


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-etl", master="local[2]", driver_memory="1g", s3=False)
    s.conf.set("spark.sql.shuffle.partitions", "4")
    yield s
    s.stop()


@pytest.fixture
def raw_csv(tmp_path):
    path = tmp_path / "raw.csv"
    lines = [",".join(RAW_COLUMNS)] + [",".join(r) for r in ROWS]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def test_run_cleans_and_counts(spark, raw_csv, tmp_path):
    out = str(tmp_path / "clean")
    result = run(spark, [raw_csv], out, end=date(2019, 11, 30))

    assert result["rows_read"] == len(ROWS)
    assert result["dropped"] == {
        "missing_product_id": 1, "bad_event_time": 1, "unknown_event_type": 1, "after_end": 1,
    }
    assert result["kept_before_dedup"] == 6
    assert result["duplicates_removed"] == 1
    assert result["clean"]["rows"] == 5
    assert result["clean"]["zero_price"] == 1

    df = spark.read.parquet(out)
    assert set(df.columns) == set(RAW_COLUMNS) | {CLEAN_PARTITION_COLUMN}
    assert dict(df.dtypes)["event_time"] == "timestamp"
    first = (df.where("user_id = 541312140")
               .selectExpr("category_code", "brand", "date_format(event_time, 'yyyy-MM-dd HH:mm:ss') AS t")
               .first())
    assert first["category_code"] is None and first["brand"] == "shiseido"
    # formatted in Spark (UTC session): collecting a timestamp converts it to the machine's time zone
    assert first["t"] == "2019-10-01 00:00:00"
    assert df.where("user_session is null").count() == 1
    dates = sorted(str(r[0]) for r in df.select(CLEAN_PARTITION_COLUMN).distinct().collect())
    assert dates == ["2019-10-01", "2019-11-15", "2019-11-16"]


def test_split_boundaries_and_cold_start(spark, raw_csv, tmp_path):
    out = str(tmp_path / "clean")
    result = run(spark, [raw_csv], out, end=date(2019, 11, 30))
    train, test = split(spark.read.parquet(out))
    assert train.count() == 3   # Oct 1 (2 rows after dedup) + Nov 15
    assert test.count() == 2    # Nov 16
    s = result["split"]
    assert s["test"]["users"] == 2
    assert s["test_users_not_in_train"] == 1      # 550050854 only appears in test
    assert s["test_buyers"] == 1 and s["test_buyers_not_in_train"] == 0


def test_rerun_replaces_only_its_dates(spark, raw_csv, tmp_path):
    out = str(tmp_path / "clean")
    run(spark, [raw_csv], out, end=date(2019, 11, 30), report=False)
    run(spark, [raw_csv], out, start=date(2019, 11, 16), end=date(2019, 11, 30), report=False)
    df = spark.read.parquet(out)
    assert df.count() == 5      # October and Nov 15 partitions kept, Nov 16 not duplicated


def test_count_drops_without_limits(spark, raw_csv):
    from batch.etl import annotate, read_raw
    counts = count_drops(annotate(read_raw(spark, [raw_csv])))
    assert counts["kept_before_dedup"] == 7    # Dec 1 is kept when no --end is given
