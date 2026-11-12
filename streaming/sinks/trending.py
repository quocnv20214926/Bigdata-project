"""Sink: trending products in the last hour (week 4, owner C).

The trending query in main.py aggregates, in event time,

    groupBy(window(event_ts, "1 hour", "5 minutes"), category_id, product_id)
        .agg(sum(weight))                      # view 1, cart 3, purchase 5

in output mode "update": every micro-batch delivers the rows whose total
changed, with the FULL total of that window (not an increment). Writing them
with ZADD is therefore idempotent.

Redis keys:
  rt:trend:cat:{category_id}:{window_end}  ZSET  per-window totals (internal), TTL 2 h
  rt:trend:global:{window_end}             ZSET  per-window totals (internal), TTL 2 h
  rt:trend:cats:{window_end}               SET   categories seen in that window, TTL 2 h
  rt:trend:published_end                   STRING window_end currently published
  trend:cat:{category_id}, trend:global    ZSET  contract keys: copy of the current
                                                 window, top TREND_TOP_N, TTL 1 h

Which window is "the last hour": an event at time t belongs to 12 sliding
windows. The one that covers the hour up to t is the window whose end is the
first slide boundary after t, i.e.

    current_end = max(window_end in the batch) - window + slide

(windows ending later only contain the last few minutes). When the current
window moves forward, every category seen in it is republished; otherwise only
the categories updated in the micro-batch are.

Each window key only keeps its WINDOW_KEEP best products: totals only grow
inside a window and any product that grows again is rewritten with its full
total, so trimming never changes the top N.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

from common import redis_keys as rk

TREND_TOP_N = 100        # products kept in trend:cat / trend:global (API k is at most 50)
WINDOW_KEEP = 200        # products kept in each internal per-window key
WINDOW_TTL = 2 * rk.HOUR
PUBLISHED_END = "rt:trend:published_end"


def window_cat_key(category_id: int, window_end: int) -> str:
    return f"rt:trend:cat:{category_id}:{window_end}"


def window_global_key(window_end: int) -> str:
    return f"rt:trend:global:{window_end}"


def window_categories_key(window_end: int) -> str:
    return f"rt:trend:cats:{window_end}"


_UNITS = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}


def duration_seconds(text: str) -> int:
    """'1 hour' -> 3600, '5 minutes' -> 300 (the formats Spark's window() accepts here)."""
    m = re.fullmatch(r"\s*(\d+)\s*(second|minute|hour|day)s?\s*", text.lower())
    if not m:
        raise ValueError(f"unsupported duration: {text!r}")
    return int(m.group(1)) * _UNITS[m.group(2)]


def current_window_end(max_window_end: int, window_seconds: int, slide_seconds: int) -> int:
    """End of the window covering the last hour up to the newest event of the batch."""
    return int(max_window_end) - window_seconds + slide_seconds


def write_window_totals(rows: Iterable, r, keep: int = WINDOW_KEEP, flush_every: int = 5000) -> int:
    """Store per-window totals. Rows have window_end (epoch), category_id, product_id, score."""
    pipe = r.pipeline(transaction=False)
    touched: set[str] = set()
    n = 0

    def flush() -> None:
        for key in touched:
            if not key.startswith("rt:trend:cats:"):
                pipe.zremrangebyrank(key, 0, -(keep + 1))
            pipe.expire(key, WINDOW_TTL)
        pipe.execute()
        touched.clear()

    for row in rows:
        end, pid, score = int(row.window_end), str(row.product_id), float(row.score)
        for key in (window_cat_key(row.category_id, end), window_global_key(end)):
            # GT: a late micro-batch can never lower a total that is already stored.
            pipe.zadd(key, {pid: score}, gt=True)
            touched.add(key)
        cats_key = window_categories_key(end)
        pipe.sadd(cats_key, int(row.category_id))
        touched.add(cats_key)
        n += 1
        if n % flush_every == 0:
            flush()
    flush()
    return n


def publish(window_end: int, categories: Iterable[int], r, top_n: int = TREND_TOP_N) -> Optional[int]:
    """Copy the window ending at window_end to the contract keys.

    categories: the categories updated in this micro-batch. When the window
    moves forward, all categories seen in the new window are republished.
    Skipped when a newer window is already published (late data).
    Returns the published window_end, or None if skipped.
    """
    current = r.get(PUBLISHED_END)
    if current is not None and int(current) > window_end:
        return None
    categories = {int(c) for c in categories}
    if current is None or int(current) < window_end:
        categories |= {int(c) for c in r.smembers(window_categories_key(window_end))}
    ttl = rk.TTL["trend"]
    pipe = r.pipeline(transaction=True)
    targets = [(rk.trend_cat(c), window_cat_key(c, window_end)) for c in categories]
    targets.append((rk.TREND_GLOBAL, window_global_key(window_end)))
    for dest, src in targets:
        pipe.zunionstore(dest, [src])           # overwrites dest
        pipe.zremrangebyrank(dest, 0, -(top_n + 1))
        pipe.expire(dest, ttl)
    pipe.set(PUBLISHED_END, window_end)
    pipe.execute()
    return window_end

