"""Who gets rec:batch, and how much Redis the batch keys may use (Pair 1, week 4).

Redis has 2 GB (infra/compose.base.yml) with maxmemory-policy volatile-lru: under pressure it
evicts only keys that have a TTL, i.e. the speed layer's. Batch keys have none, so if they grew
past the limit the speed layer's writes would start failing. All batch keys together therefore
stay within BATCH_BUDGET_BYTES (1 GiB, half of Redis), split per key family in BUDGET_BYTES.

Key sizes measured on Redis 8.10.2, the version in compose (a ZSET of up to 128 members is a
listpack): with scores rounded to SCORE_DECIMALS a key takes about
ZSET_KEY_BYTES + ZSET_ENTRY_BYTES * members of used_memory: 20 members 0.38 KB, 50 members
0.82 KB, 100 members 1.59 KB. Unrounded doubles take about twice as much. batch/redis_sizing.py
measures them again on a live Redis.

rec:batch goes to the users of the ALS matrix who were active most recently before the run date,
as many as fit in the rec:batch budget. Everyone else is served by the speed layer and popularity.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from common.config import settings

MiB = 1 << 20
BATCH_BUDGET_BYTES = 1024 * MiB
BUDGET_BYTES = {"rec:batch": 860 * MiB, "sim:item": 150 * MiB, "pop": 14 * MiB}
SCORE_DECIMALS = 4
ZSET_KEY_BYTES = 72
ZSET_ENTRY_BYTES = 15.2


def estimate_bytes(keys: int, members: int) -> int:
    """Redis memory for `keys` ZSETs holding `members` members in total (rounded scores)."""
    return int(keys * ZSET_KEY_BYTES + members * ZSET_ENTRY_BYTES)


def max_users(top_n: int = settings.batch_top_n, budget: int = BUDGET_BYTES["rec:batch"]) -> int:
    """How many rec:batch keys of top_n products fit in the budget."""
    return int(budget // (ZSET_KEY_BYTES + top_n * ZSET_ENTRY_BYTES))


def last_activity(events: DataFrame, users: DataFrame) -> DataFrame:
    """(user_id, last_event): the time of each user's latest event, for the given users only."""
    return (events.join(users.select("user_id").distinct(), "user_id", "left_semi")
            .groupBy("user_id").agg(F.max("event_time").alias("last_event")))


def with_recency_rank(activity: DataFrame) -> DataFrame:
    """Add recency_rank: 1 = the most recently active user; ties by user_id."""
    order = Window.orderBy(F.col("last_event").desc(), F.col("user_id"))
    return activity.withColumn("recency_rank", F.row_number().over(order))


def most_recent_users(activity: DataFrame, n: int) -> DataFrame:
    """(user_id, last_event) of the n most recently active users (all users when n is 0)."""
    if not n:
        return activity
    return activity.orderBy(F.col("last_event").desc(), F.col("user_id")).limit(n)
