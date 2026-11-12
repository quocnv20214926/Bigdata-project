"""The nightly pipeline, every step in order, on tiny data (local Spark, local folders).

load and validate need a Redis: database 15 of REDIS_TEST_HOST:REDIS_TEST_PORT
(default localhost:6379); they are skipped when it is unreachable or not empty.

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

import pytest

from batch import nightly
from common import redis_keys as rk

COLS = "event_time timestamp, event_type string, product_id long, category_id long, user_id long, event_date date"


def clean_events(spark, out):
    """Two groups of users with their own products, Oct 20 .. Nov 16 (Nov 16 must be ignored)."""
    rows, day0 = [], date(2019, 10, 20)
    for u in range(1, 31):
        base = 100 if u <= 15 else 200
        for d in range(0, 28, 3):
            day = day0 + timedelta(days=d + u % 3)
            for p in range(base, base + 6):
                if (u + p + d) % 4:
                    rows.append((datetime(day.year, day.month, day.day, 12), "view", p, base, u, day))
    rows.append((datetime(2019, 11, 16, 1), "purchase", 999, 9, 1, date(2019, 11, 16)))   # as_of day: not used
    spark.createDataFrame(rows, COLS).write.partitionBy("event_date").parquet(out)


def config(tmp_path, step, *extra):
    args = [step, "--run-id", "20261028", "--input", str(tmp_path / "clean"), "--root", str(tmp_path / "runs"),
            "--no-s3", "--stats-dir", str(tmp_path / "stats"), "--rank", "4", "--iter", "3",
            "--top-n", "5", "--pop-n", "3", "--sim-block-size", "4", "--writers", "2", "--redis-db", "15",
            "--redis-host", os.getenv("REDIS_TEST_HOST", "localhost"),
            "--redis-port", os.getenv("REDIS_TEST_PORT", "6379"), *extra]
    return nightly.parser().parse_args(args)


def redis_or_none(cfg):
    redis = pytest.importorskip("redis")
    client = nightly.target(cfg).client()
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        return None
    return None if client.dbsize() else client


def test_every_step(spark, tmp_path):
    clean_events(spark, str(tmp_path / "clean"))
    run = lambda step, *extra: nightly.run_step(spark, step, config(tmp_path, step, *extra))

    s = run("check")
    assert s["last_day"] == "2019-11-15" and s["window"] == [None, "2019-11-16"]
    assert run("popularity")["categories"] == 2                 # product 999 (Nov 16) is not seen
    t = run("train")
    assert t["als_matrix"]["users"] == 30 and t["als_matrix"]["products"] == 12
    r = run("recommend", "--max-users", "10")
    assert r["users"] == 10 and r["rows"] == 50 and r["als_users"] == 30
    sim = run("sim")
    assert sim["source"] == "item_knn" and sim["products_in_matrix"] == 12
    assert 0 < sim["products"] <= 12 and 0 < sim["rows"] <= 12 * 5      # only products of the same group meet
    als_sim = nightly.run_step(spark, "sim", config(tmp_path, "sim", "--sim-source", "als"))
    assert als_sim["products_in_matrix"] == 12 and 0 < als_sim["rows"] <= 12 * 11
    sim = run("sim")                                                    # back to the default table for load

    recs = spark.read.parquet(str(tmp_path / "runs" / "20261028" / "rec_batch"))
    assert {c for c in recs.columns} == {"user_id", "product_id", "rank", "score"}
    assert json.loads((tmp_path / "stats" / "20261028" / "recommend.json").read_text())["users"] == 10

    client = redis_or_none(config(tmp_path, "load"))
    if client is not None:
        try:
            loaded = run("load")
            assert loaded["families"][rk.rec_batch("")]["keys"] == 10
            v = run("validate")
            assert v["families"][rk.POP_GLOBAL]["keys"] == 1 and v["last_run"]
            assert client.zcard(rk.POP_GLOBAL) == 3
        finally:
            client.flushdb()

    for old in ("20261026", "20261027"):
        (tmp_path / "runs" / old).mkdir()
    c = run("cleanup")
    assert c == {**c, "kept": ["20261027", "20261028"], "deleted": ["20261026"]}


def test_check_fails_without_yesterday(spark, tmp_path):
    clean_events(spark, str(tmp_path / "clean"))
    with pytest.raises(RuntimeError, match="day before as_of"):
        nightly.run_step(spark, "check", config(tmp_path, "check", "--as-of", "2019-11-20"))
    s = nightly.run_step(spark, "check", config(tmp_path, "check", "--train-days", "7"))
    assert s["first_day"] == "2019-11-09" and s["window"] == ["2019-11-09", "2019-11-16"]
