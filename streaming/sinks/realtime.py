"""Real-time recommendation score sink (contract: rec:rt:{user_id} ZSET).

For each user event on item i, propagate event_weight * sim:cooc(i,j) to j.
The API combines this speed-layer score with rec:batch.
"""
from __future__ import annotations

from typing import Iterable

from common import redis_keys as rk
from common.weights import event_weight


def write_realtime_recommendations(rows: Iterable, r, flush_every: int = 2000) -> int:
    """Apply event-driven score increments; return number of neighbour updates.

    A micro-batch replay can increment scores twice if a process dies after
    Redis writes but before Spark commits the batch guard. This is at-least-once,
    not end-to-end exactly-once delivery.
    """
    events = [(int(row.user_id), int(row.product_id), event_weight(str(row.event_type)))
              for row in rows]
    events = [(uid, pid, weight) for uid, pid, weight in events if weight > 0]
    if not events:
        return 0

    # Pipeline similarity reads per partition instead of one network roundtrip
    # per event. Duplicate product events intentionally each contribute weight.
    read_pipe = r.pipeline(transaction=False)
    for _, pid, _ in events:
        read_pipe.zrange(rk.sim_cooc(pid), 0, -1, withscores=True)
    neighbour_lists = read_pipe.execute()

    write_pipe = r.pipeline(transaction=False)
    touched: set[str] = set()
    updates = 0
    pending = 0
    for (uid, pid, weight), neighbours in zip(events, neighbour_lists):
        key = rk.rec_rt(uid)
        for neighbour, similarity in neighbours:
            candidate, similarity = int(neighbour), float(similarity)
            if candidate == pid or similarity <= 0:
                continue
            write_pipe.zincrby(key, weight * similarity, str(candidate))
            touched.add(key)
            updates += 1
            pending += 1
            if pending >= flush_every:
                for touched_key in touched:
                    write_pipe.expire(touched_key, rk.TTL["rec:rt"])
                write_pipe.execute()
                write_pipe = r.pipeline(transaction=False)
                touched.clear()
                pending = 0
    if touched:
        for touched_key in touched:
            write_pipe.expire(touched_key, rk.TTL["rec:rt"])
        write_pipe.execute()
    return updates
