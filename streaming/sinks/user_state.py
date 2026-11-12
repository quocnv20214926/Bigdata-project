"""Sink: per-user state in Redis (week 3, owner C). Contract 1.1.0:

  user:recent:{user_id}  ZSET  "product_id|event_type|epoch" -> epoch
                               keeps the settings.recent_n newest, TTL 24 h
  user:bought:{user_id}  ZSET  product_id -> latest purchase epoch, TTL 30 days

Read user:recent newest first with ZREVRANGE; test a purchase with ZSCORE.

Every write is a ZADD with a stable member, so replaying a micro-batch after a
failure does not create duplicates (the sink is idempotent).
"""

from __future__ import annotations

from typing import Iterable

from common import redis_keys as rk
from common.config import settings


def write_user_state(rows: Iterable, r, recent_n: int = settings.recent_n, flush_every: int = 2000) -> int:
    """Write events (fields user_id, product_id, event_type, epoch) to Redis.

    ZREMRANGEBYRANK and EXPIRE are sent once per touched key per flush.
    Returns the number of events written.
    """
    pipe = r.pipeline(transaction=False)
    recent_keys: set[str] = set()
    bought_keys: set[str] = set()
    n = 0

    def flush() -> None:
        for key in recent_keys:
            pipe.zremrangebyrank(key, 0, -(recent_n + 1))   # keep the recent_n highest scores
            pipe.expire(key, rk.TTL["user:recent"])
        for key in bought_keys:
            pipe.expire(key, rk.TTL["user:bought"])
        pipe.execute()
        recent_keys.clear()
        bought_keys.clear()

    for row in rows:
        epoch = int(row.epoch)
        key = rk.user_recent(row.user_id)
        pipe.zadd(key, {rk.encode_recent(row.product_id, row.event_type, epoch): epoch})
        recent_keys.add(key)
        if row.event_type == "purchase":
            bkey = rk.user_bought(row.user_id)
            pipe.zadd(bkey, {str(row.product_id): epoch}, gt=True)   # keep the latest purchase time
            bought_keys.add(bkey)
        n += 1
        if n % flush_every == 0:
            flush()
    flush()
    return n
