"""rec:batch size rules: memory formula, budget, user selection (local Spark).

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

from datetime import datetime

from batch import rec_batch
from batch.redis_sizing import choose, sample_zset


def test_formula_matches_the_redis_8_measurements():
    # used_memory per key measured on Redis 8.10.2, scores rounded to 4 decimals
    for members, measured in [(20, 376), (50, 824), (100, 1594)]:
        assert abs(rec_batch.estimate_bytes(1, members) - measured) / measured < 0.02


def test_budget_split_and_user_count():
    assert sum(rec_batch.BUDGET_BYTES.values()) == rec_batch.BATCH_BUDGET_BYTES
    assert rec_batch.max_users(100) == 566_441      # 860 MiB / 1,592 B
    assert rec_batch.max_users(50) > 2 * rec_batch.max_users(100) - 100_000


def test_most_recent_users(spark):
    t = lambda d, h: datetime(2019, 11, d, h)
    events = spark.createDataFrame(
        [(1, t(1, 0)), (1, t(9, 0)), (2, t(5, 0)), (3, t(9, 0)), (4, t(15, 0))],
        "user_id long, event_time timestamp")
    users = spark.createDataFrame([(1,), (2,), (3,)], ["user_id"])          # user 4 is not an ALS user
    activity = rec_batch.last_activity(events, users)
    ranked = {r.user_id: r.recency_rank for r in rec_batch.with_recency_rank(activity).collect()}
    assert ranked == {1: 1, 3: 2, 2: 3}                                      # tie on Nov 9: smaller id first
    assert [r.user_id for r in rec_batch.most_recent_users(activity, 2).collect()] == [1, 3]
    assert rec_batch.most_recent_users(activity, 0).count() == 3


def test_sizing_helpers():
    import random
    z = sample_zset(random.Random(1), 30, 4)
    assert len(z) == 30 and all(round(v, 4) == v for v in z.values())
    opts = {"top_100": {"intent_events_covered": 0.60}, "top_50": {"intent_events_covered": 0.62}}
    assert choose(opts) == "top_100"
    opts["top_50"]["intent_events_covered"] = 0.70
    assert choose(opts) == "top_50"
