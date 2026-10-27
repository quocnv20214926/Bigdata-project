"""Redis bulk loader against a real Redis (local Spark).

Uses database 15 of REDIS_TEST_HOST:REDIS_TEST_PORT (default localhost:6379) and is skipped when
no Redis answers or when that database is not empty (it never deletes data it did not write).

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from batch import redis_loader as rl
from common import redis_keys as rk

TARGET = rl.RedisTarget(host=os.getenv("REDIS_TEST_HOST", "localhost"),
                        port=int(os.getenv("REDIS_TEST_PORT", "6379")), db=15)


@pytest.fixture
def redis_db():
    redis = pytest.importorskip("redis")
    client = TARGET.client()
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip(f"no Redis at {TARGET.host}:{TARGET.port}")
    if client.dbsize():
        pytest.skip("Redis database 15 is not empty")
    yield client
    client.flushdb()
    client.close()


def test_load_writes_rounds_and_replaces(spark, redis_db):
    redis_db.zadd(rk.rec_batch(9), {1: 1.0})          # a user who is no longer in rec:batch
    redis_db.zadd(rk.rec_batch(1), {99: 5.0})         # an old list that must be replaced, not merged
    redis_db.set(rk.user_recent(9), "speed layer")    # someone else's key: untouched
    rec = spark.createDataFrame([(1, 10, 0.123456), (1, 11, 0.5), (2, 10, 0.7)], "user_id long, product_id long, score double")
    sim = spark.createDataFrame([(10, 11, 0.91234567)], "product_id long, neighbour_id long, sim float")
    pop = spark.createDataFrame([(10, 12.0), (11, 3.0)], "product_id long, score double")
    cat = spark.createDataFrame([(7, 10, 12.0)], "category_id long, product_id long, score double")

    when = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    stats = rl.load(TARGET, rec_batch=rec, sim_item=sim, pop_global=pop, pop_cat=cat, writers=2, batch_keys=1, when=when)

    assert redis_db.zrange(rk.rec_batch(1), 0, -1, withscores=True) == [("10", 0.1235), ("11", 0.5)]
    assert redis_db.zrange(rk.rec_batch(2), 0, -1, withscores=True) == [("10", 0.7)]
    assert not redis_db.exists(rk.rec_batch(9))
    assert redis_db.get(rk.user_recent(9)) == "speed layer"
    assert redis_db.zscore(rk.sim_item(10), 11) == 0.9123
    assert redis_db.zrevrange(rk.POP_GLOBAL, 0, -1) == ["10", "11"]
    assert redis_db.zscore(rk.pop_cat(7), 10) == 12.0
    assert redis_db.get(rk.META_BATCH_LAST_RUN) == "2026-10-28T02:00:00Z"
    assert stats["families"][rl.REC_BATCH.prefix] == {"keys": 2, "members": 3, "stale_deleted": 1}

    report = rl.inspect(TARGET)
    assert report["families"][rl.REC_BATCH.prefix]["keys"] == 2
    assert report["families"][rk.POP_GLOBAL]["keys"] == 1
    assert report["last_run"] == "2026-10-28T02:00:00Z"
    assert report["sample_rec_batch"]["top5"][0][1] in (0.5, 0.7)


def test_budget_is_checked_before_writing(spark, redis_db, monkeypatch):
    monkeypatch.setitem(rl.BUDGET_BYTES, "rec:batch", 100)
    rec = spark.createDataFrame([(1, 10, 0.1), (1, 11, 0.2)], "user_id long, product_id long, score double")
    with pytest.raises(rl.BudgetError):
        rl.load(TARGET, rec_batch=rec)
    assert redis_db.dbsize() == 0


def test_delete_stale_only_touches_numeric_ids_of_its_family(redis_db):
    for i in (1, 2, 3):
        redis_db.zadd(rk.sim_item(i), {5: 0.5})
    redis_db.set(rl.SIM_ITEM.prefix + "notes", "x")
    assert rl.delete_stale(redis_db, rl.SIM_ITEM, keep={2}) == 2
    assert sorted(redis_db.keys()) == [rk.sim_item(2), rl.SIM_ITEM.prefix + "notes"]
