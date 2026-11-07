"""Sink: session co-occurrence similarity sim:cooc (week 4, owner D).

Definition (docs/plan.md, section 3.2):
  - For each session, keep the last settings.session_window (10) DISTINCT
    products, newest first.
  - When a product i enters that window (it is not already in it):
        cnt(i)    += 1
        cooc(i,j) += 1   for every product j already in the window
    A product that is viewed again while still in the window only moves to
    the front; nothing is counted. So a pair is counted at most once while both
    products stay in the window. The similarity is clipped to [0, 1] (a pair
    can rarely be counted twice, see refresh_similarity).
  - sim(i, j) = cooc(i, j) / sqrt(cnt(i) * cnt(j))   (common.scoring.cooc_similarity)
  - sim:cooc:{i} = top settings.sim_top_n (50) products j by sim(i, j).

Redis keys (only sim:cooc:* is part of the contract; rt:* are internal):
  rt:sess:{user_session}  LIST  product ids, newest first, TTL 2 h
  rt:cooc:{i}             HASH  j -> cooc(i, j), TTL 7 days
  rt:cooc:cnt             HASH  i -> cnt(i)
  sim:cooc:{i}            ZSET  j -> sim(i, j), TTL 7 days

HINCRBY is not idempotent: main.py skips a micro-batch that was already
committed, so counts are only doubled if the job crashes in the middle of a batch.
Events without a user_session (2 rows in October) are ignored here.
"""

from __future__ import annotations

import heapq
from typing import Iterable

from common import redis_keys as rk
from common.config import settings
from common.scoring import cooc_similarity

SESSION_TTL = 2 * rk.HOUR
COUNT_KEY = "rt:cooc:cnt"


def session_key(user_session: str) -> str:
    return f"rt:sess:{user_session}"


def pair_key(product_id: int) -> str:
    return f"rt:cooc:{product_id}"


def update_cooc(rows: Iterable, r, window: int = settings.session_window) -> set[int]:
    """Count co-occurrences for events (fields user_session, product_id) in time order.

    Returns the product ids whose counts changed (their sim:cooc must be refreshed).
    """
    rows = [row for row in rows if row.user_session]
    if not rows:
        return set()

    # 1. Load the stored window of every session in this partition (one round trip).
    sessions = list(dict.fromkeys(row.user_session for row in rows))
    pipe = r.pipeline(transaction=False)
    for s in sessions:
        pipe.lrange(session_key(s), 0, window - 1)
    history = {s: [int(x) for x in h] for s, h in zip(sessions, pipe.execute())}

    # 2. Walk the events and count.
    pipe = r.pipeline(transaction=False)
    touched: set[int] = set()
    changed: set[str] = set()
    for row in rows:
        s, pid = row.user_session, int(row.product_id)
        prev = history[s]
        if pid in prev:                       # revisit: move to the front, count nothing
            if prev[0] != pid:
                prev.remove(pid)
                prev.insert(0, pid)
                changed.add(s)
            continue
        pipe.hincrby(COUNT_KEY, pid, 1)
        for j in prev:
            pipe.hincrby(pair_key(pid), j, 1)
            pipe.hincrby(pair_key(j), pid, 1)
            touched.add(j)
        touched.add(pid)
        prev.insert(0, pid)
        del prev[window:]
        changed.add(s)

    # 3. Save the session windows back.
    for s in changed:
        key = session_key(s)
        pipe.delete(key)
        pipe.rpush(key, *history[s])
        pipe.expire(key, SESSION_TTL)
    pipe.execute()
    return touched


def refresh_similarity(products: Iterable[int], r, top_n: int = settings.sim_top_n, chunk: int = 500) -> int:
    """Recompute sim:cooc:{i} for the given products from the raw counts.

    Returns the number of sim:cooc keys written.
    """
    products = list(dict.fromkeys(int(p) for p in products))
    written = 0
    for start in range(0, len(products), chunk):
        part = products[start:start + chunk]
        pipe = r.pipeline(transaction=False)
        for p in part:
            pipe.hgetall(pair_key(p))
        pairs = dict(zip(part, pipe.execute()))

        ids = list(dict.fromkeys(part + [int(j) for h in pairs.values() for j in h]))
        counts: dict[int, float] = {}
        for s in range(0, len(ids), 5000):
            sub = ids[s:s + 5000]
            counts.update({i: float(c or 0) for i, c in zip(sub, r.hmget(COUNT_KEY, sub))})

        # MULTI/EXEC: readers never see a sim:cooc key between DEL and ZADD.
        pipe = r.pipeline(transaction=True)
        for p in part:
            # Clipped at 1: a pair can be counted again when a product leaves the
            # window and comes back while the other one was kept by a revisit.
            scores = {
                int(j): min(1.0, cooc_similarity(float(c), counts.get(p, 0.0), counts.get(int(j), 0.0)))
                for j, c in pairs[p].items()
            }
            top = heapq.nlargest(top_n, scores.items(), key=lambda kv: kv[1])
            key = rk.sim_cooc(p)
            pipe.delete(key)
            if top:
                pipe.zadd(key, {str(j): round(s, 6) for j, s in top})
                pipe.expire(key, rk.TTL["sim:cooc"])
                written += 1
            pipe.expire(pair_key(p), rk.TTL["sim:cooc"])
        pipe.execute()
    return written


def write_partition_cooc(rows: Iterable, r) -> int:
    """Count, then refresh the similarity of the touched products. Returns keys written."""
    touched = update_cooc(rows, r)
    return refresh_similarity(touched, r) if touched else 0
