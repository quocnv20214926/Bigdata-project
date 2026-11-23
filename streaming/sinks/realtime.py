"""Sink: real-time recommendation scores rec:rt (week 5, owner D).

Plan, section 3.3: for an event of type e by a user on product i,

    rt[j] += w_e * sim(i, j) * exp(-lambda * dt)    for j in the top-K of sim:cooc:{i}

with lambda = ln 2 / half-life (30 minutes, common.scoring.DEFAULT_HALF_LIFE_SECONDS)
and w_e from common.weights (view 1, cart 3, purchase 5). The arithmetic is
common.scoring.time_decay / propagate, the same code the offline evaluation uses.

Storage. Scores are kept decayed to the time of the user's newest event:

  rec:rt:{user_id}   ZSET   product_id -> score at that time, top RT_TOP_N, TTL 6 h
  rt:rect:{user_id}  STRING epoch of that newest event (internal), TTL 6 h

When a newer event arrives, every stored score is multiplied by
exp(-lambda * (t_new - t_ref)) before the new increments are added. All scores of
a user decay by the same factor, so the ranking does not change with wall-clock
time, and the API min-max normalises scores before merging: no decay is needed
when reading.

An event older than the stored reference (late data) adds its increment already
decayed by its age. The update reads and rewrites the ZSET, so it is not
idempotent: main.py skips micro-batches that were already committed.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from typing import Iterable

from common import redis_keys as rk
from common.scoring import DEFAULT_HALF_LIFE_SECONDS, propagate, time_decay

RT_NEIGHBOURS = 20     # K: neighbours of sim:cooc:{i} used per event
RT_TOP_N = 100         # products kept in rec:rt per user
MIN_SCORE = 1e-6       # smaller scores are dropped (about 20 half-lives)


def ref_key(user_id) -> str:
    return f"rt:rect:{user_id}"


def update_realtime(rows: Iterable, r, k: int = RT_NEIGHBOURS, top_n: int = RT_TOP_N,
                    half_life: float = DEFAULT_HALF_LIFE_SECONDS) -> int:
    """Apply events (fields user_id, product_id, event_type, epoch), sorted by
    user and time, to rec:rt. Returns the number of users written."""
    by_user: dict[int, list] = defaultdict(list)
    for row in rows:
        by_user[int(row.user_id)].append(row)
    if not by_user:
        return 0
    users = list(by_user)
    products = list({int(row.product_id) for evs in by_user.values() for row in evs})

    # 1. One round trip: current scores and reference time of each user,
    #    top-K neighbours of each product.
    pipe = r.pipeline(transaction=False)
    for u in users:
        pipe.zrange(rk.rec_rt(u), 0, -1, withscores=True)
        pipe.get(ref_key(u))
    for p in products:
        pipe.zrevrange(rk.sim_cooc(p), 0, k - 1, withscores=True)
    res = pipe.execute()
    state = {u: (dict(res[2 * n]), res[2 * n + 1]) for n, u in enumerate(users)}
    base = 2 * len(users)
    neighbours = {p: {int(j): float(s) for j, s in res[base + n]} for n, p in enumerate(products)}

    # 2. Replay each user's events in time order.
    pipe = r.pipeline(transaction=True)      # readers never see a half-written key
    ttl = rk.TTL["rec:rt"]
    written = 0
    for u, events in by_user.items():
        stored, ref = state[u]
        scores = {int(p): float(s) for p, s in stored.items()}
        ref = int(ref) if ref is not None else None
        for ev in sorted(events, key=lambda e: int(e.epoch)):
            t = int(ev.epoch)
            age = 0.0
            if ref is None:
                ref = t
            elif t > ref:
                factor = time_decay(t - ref, half_life)
                scores = {p: s * factor for p, s in scores.items()}
                ref = t
            else:
                age = ref - t                     # late event: decay its own increment
            for j, inc in propagate(ev.event_type, neighbours.get(int(ev.product_id), {}),
                                    age, half_life).items():
                scores[j] = scores.get(j, 0.0) + inc
        top = heapq.nlargest(top_n, ((p, s) for p, s in scores.items() if s >= MIN_SCORE),
                             key=lambda kv: kv[1])
        key = rk.rec_rt(u)
        pipe.delete(key)
        if top:
            pipe.zadd(key, {str(p): round(s, 6) for p, s in top})
            pipe.expire(key, ttl)
            pipe.set(ref_key(u), ref, ex=ttl)
            written += 1
    pipe.execute()
    return written
