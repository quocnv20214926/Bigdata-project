"""Speed-only offline evaluation: replay events, next-item Hit Rate (week 7, owner D).

The events are replayed in event-time order through the *production* code of the
speed layer, on a dedicated Redis:

    streaming.sinks.user_state  -> user:recent, user:bought
    streaming.sinks.products    -> rt:prod
    streaming.sinks.cooc        -> sim:cooc
    streaming.sinks.realtime    -> rec:rt          (time decay, half-life, K)
    streaming.sinks.trending    -> trend:cat, trend:global   (1 h window, 5 min slide)

and the lists are read back with the API's own RedisRepository. Events are grouped
in micro-batches of --batch-seconds of event time (default 30 s: a replay at 15x
with the 2 s trigger of the streaming job). For every target event in a micro-batch, the
recommendations are taken from the state left by the *previous* micro-batches,
i.e. what the API would have answered just before the event, then the
micro-batch is applied.

Target events (the "next item"): an event of a sampled user who already has an
event in the last 24 h (user:recent), on a product different from the user's
previous one. Reported for all targets, by event type, and for the "unseen"
subset (product not among the user's 20 recent events).

Methods compared, top K:
  trending          trend:global (most active products of the last hour)
  trending_cat      trend:cat of the category of the user's last product, then trend:global
  cooc_last         sim:cooc of the user's last product only (no history, no decay)
  rec_rt            rec:rt alone (empty for users whose products have no co-views)
  api               /recommend without rec:batch: rec:rt, purchased products removed,
                    filled from trend:cat -> trend:global -> pop:global (what users get)

With --sim-item FILE (product_id,neighbour_id,sim; streaming/evaluation/build_sim_item.py),
sim:item is loaded before the replay and --rt-similarity item|hybrid makes rec:rt use it
(week 8 ablation cooc / item / hybrid). The file must be built from events before
--warmup-until, or the evaluation leaks the future.

Metrics: HR@K (target in the top K), NDCG@K (1 / log2(1 + rank)), coverage (share of
targets for which the method returned at least one product).

Usage (from the repository root, redis-server on --redis-port; its database is FLUSHED):

    python -m streaming.evaluation.replay_eval --file data/raw/2019-Oct.csv \
        --warmup-until 2019-10-02T00:00:00Z --end 2019-10-04T00:00:00Z \
        --redis-port 6390 --json docs/eval-pair2-week07.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional

import redis
import redis.asyncio as aioredis

from common import redis_keys as rk
from common.config import settings
from common.scoring import DEFAULT_HALF_LIFE_SECONDS
from common.weights import event_weight
from ingestion.replayer import read_events
from serving.app.redis_repository import RedisRepository, product_key
from streaming.sinks import realtime as rt_sink
from streaming.sinks.cooc import write_partition_cooc
from streaming.sinks.products import write_products
from streaming.sinks.trending import current_window_end, publish, write_window_totals
from streaming.sinks.user_state import write_user_state

log = logging.getLogger("replay_eval")

METHODS = ["trending", "trending_cat", "cooc_last", "rec_rt", "api"]
WINDOW, SLIDE = 3600, 300
DEDUP_SECONDS = 600            # watermark of the streaming job


def parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def sampled(user_id: int, fraction: float) -> bool:
    if fraction >= 1:
        return True
    return (zlib.crc32(str(user_id).encode()) % 10_000) < fraction * 10_000


# ---- metrics ---------------------------------------------------------------------

@dataclass
class Tally:
    n: int = 0
    covered: int = 0

    def __post_init__(self):
        self.hits: dict[int, int] = defaultdict(int)
        self.ndcg: dict[int, float] = defaultdict(float)

    def add(self, ranked: list[int], target: int, ks: list[int]) -> None:
        self.n += 1
        if ranked:
            self.covered += 1
        try:
            rank = ranked.index(target) + 1
        except ValueError:
            return
        for k in ks:
            if rank <= k:
                self.hits[k] += 1
                self.ndcg[k] += 1.0 / math.log2(1 + rank)

    def row(self, ks: list[int]) -> dict:
        out = {"targets": self.n, "coverage": round(self.covered / self.n, 4) if self.n else 0.0}
        for k in ks:
            out[f"hr@{k}"] = round(self.hits[k] / self.n, 4) if self.n else 0.0
            out[f"ndcg@{k}"] = round(self.ndcg[k] / self.n, 4) if self.n else 0.0
        return out


class Results:
    def __init__(self, ks: list[int]):
        self.ks = ks
        self.groups: dict[str, dict[str, Tally]] = defaultdict(lambda: {m: Tally() for m in METHODS})

    def add(self, groups: list[str], lists: dict[str, list[int]], target: int) -> None:
        for g in groups:
            for m in METHODS:
                self.groups[g][m].add(lists[m], target, self.ks)

    def to_dict(self) -> dict:
        return {g: {m: t.row(self.ks) for m, t in ms.items()} for g, ms in self.groups.items()}


# ---- in-memory trending windows (the Spark aggregation of the trending query) ----

class TrendWindows:
    """Totals of sum(weight) per (window_end, category, product), 1 h windows sliding every 5 min."""

    def __init__(self):
        self.totals: dict[int, dict[tuple[int, int], float]] = defaultdict(lambda: defaultdict(float))

    def add_batch(self, events) -> list[SimpleNamespace]:
        changed: dict[tuple[int, int, int], float] = {}
        for ev in events:
            w = event_weight(ev.event_type)
            if w <= 0:
                continue
            t = int(ev.epoch)
            first_end = (t // SLIDE + 1) * SLIDE              # windows [end - 1 h, end) containing t
            for end in range(first_end, first_end + WINDOW, SLIDE):
                key = (int(ev.category_id), int(ev.product_id))
                self.totals[end][key] += w
                changed[(end, *key)] = self.totals[end][key]
        return [SimpleNamespace(window_end=e, category_id=c, product_id=p, score=s)
                for (e, c, p), s in changed.items()]

    def drop_before(self, epoch: int) -> None:
        for end in [e for e in self.totals if e < epoch - 2 * WINDOW]:
            del self.totals[end]


# ---- replay ------------------------------------------------------------------------

def to_row(ev) -> SimpleNamespace:
    epoch = int(ev.event_time.timestamp())
    return SimpleNamespace(
        user_id=ev.user_id, product_id=ev.product_id, event_type=ev.event_type, epoch=epoch,
        user_session=ev.user_session, event_time=ev.event_time, category_id=ev.category_id,
        category_code=ev.category_code, brand=ev.brand, price=ev.price,
    )


def apply_batch(rows: list, r: redis.Redis, trend: TrendWindows, args) -> None:
    rows.sort(key=lambda e: (e.user_id, e.epoch))           # repartition("user_id") + sortWithinPartitions
    write_user_state(rows, r)
    write_products(rows, r)
    write_partition_cooc(rows, r)
    rt_sink.update_realtime(rows, r, k=args.neighbours, half_life=args.half_life,
                            similarity=getattr(args, "rt_similarity", "cooc"))
    totals = trend.add_batch(rows)
    if totals:
        write_window_totals(totals, r)
        end = current_window_end(max(t.window_end for t in totals), WINDOW, SLIDE)
        publish(end, {t.category_id for t in totals if t.window_end == end}, r)


def load_sim_item(path: str, r: redis.Redis, top_n: int = settings.sim_top_n) -> int:
    """sim:item:{product} from a CSV product_id,neighbour_id,sim (header line). Returns products loaded."""
    import csv
    lists: dict[int, dict[str, float]] = defaultdict(dict)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            lists[int(row["product_id"])][row["neighbour_id"]] = float(row["sim"])
    pipe = r.pipeline(transaction=False)
    for n, (pid, nb) in enumerate(lists.items(), 1):
        top = dict(sorted(nb.items(), key=lambda kv: kv[1], reverse=True)[:top_n])
        pipe.zadd(rk.sim_item(pid), top)
        if n % 5000 == 0:
            pipe.execute()
    pipe.execute()
    return len(lists)


def candidate_lists(targets: list, r: redis.Redis, repo: RedisRepository, loop, kmax: int) -> list[dict]:
    """The top-kmax lists of every method for each target, from the current Redis state.

    Every method sees the same information: the state left by the previous
    micro-batches. "The user's last product" is therefore the newest one in
    user:recent, not the event just before the target if that event is in the
    same micro-batch (it has not been processed yet).
    """
    pipe = r.pipeline(transaction=False)
    pipe.zrevrange(rk.TREND_GLOBAL, 0, kmax - 1)
    for t in targets:
        pipe.zrevrange(rk.rec_rt(t.user_id), 0, kmax - 1)
        pipe.zrevrange(rk.user_recent(t.user_id), 0, 0)
    res = pipe.execute()
    glob = [int(x) for x in res[0]]
    last = [rk.decode_recent(m[0])[0] if m else None for m in res[2::2]]

    pipe = r.pipeline(transaction=False)
    for p in last:
        if p is not None:
            pipe.zrevrange(rk.sim_cooc(p), 0, kmax - 1)
            pipe.hget(product_key(p), "category_id")
    flat = iter(pipe.execute())
    cooc_of, cat_of = [], []
    for p in last:
        if p is None:
            cooc_of.append([])
            cat_of.append(None)
        else:
            cooc_of.append([int(x) for x in next(flat)])
            cat_of.append(next(flat))
    cat_list = sorted({int(c) for c in cat_of if c})
    pipe = r.pipeline(transaction=False)
    for c in cat_list:
        pipe.zrevrange(rk.trend_cat(c), 0, kmax - 1)
    trend_cat = {c: [int(x) for x in lst] for c, lst in zip(cat_list, pipe.execute())}

    async def api_lists():
        users = list(dict.fromkeys(t.user_id for t in targets))
        sem = asyncio.Semaphore(32)

        async def one(u):
            async with sem:
                return await repo.recommend(u, kmax)
        resps = await asyncio.gather(*(one(u) for u in users))
        return {u: [i.product_id for i in resp.items] for u, resp in zip(users, resps)}

    api = loop.run_until_complete(api_lists())

    out = []
    for n, t in enumerate(targets):
        tc = list(trend_cat.get(int(cat_of[n]), [])) if cat_of[n] else []
        tc += [p for p in glob if p not in tc]
        out.append({"trending": glob, "trending_cat": tc[:kmax], "cooc_last": cooc_of[n],
                    "rec_rt": [int(x) for x in res[1 + 2 * n]], "api": api[t.user_id]})
    return out


def run(args, r=None, ar=None) -> dict:
    """r / ar: sync and asyncio clients of the same Redis (tests pass fakeredis)."""
    r = r or redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    if r.dbsize() and not args.flush:
        raise SystemExit(f"Redis {args.redis_host}:{args.redis_port} is not empty: use a dedicated "
                         "instance, or --flush to empty it")
    r.flushdb()
    if getattr(args, "sim_item", None):
        stats_sim = load_sim_item(args.sim_item, r)
        log.info("sim:item loaded: %s products", f"{stats_sim:,}")
    ar = ar or aioredis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    repo = RedisRepository(ar, alpha=0.5)
    loop = asyncio.new_event_loop()

    ks = sorted(set(args.k))
    results = Results(ks)
    trend = TrendWindows()
    warmup_until = parse_time(args.warmup_until)
    warm_epoch = int(warmup_until.timestamp()) if warmup_until else 0

    last: dict[int, tuple[int, int]] = {}          # user -> (last product, epoch)
    recent: dict[int, list[int]] = {}              # user -> last 20 products (newest last)
    seen: dict[tuple, int] = {}                    # dedup key -> epoch (watermark-limited)
    batch: list = []
    batch_end: Optional[int] = None
    stats = {"events": 0, "duplicates": 0, "batches": 0, "targets": 0}
    started = time.time()

    def flush_batch():
        nonlocal batch
        if not batch:
            return
        targets = [e for e in batch if getattr(e, "target", False)]
        if targets:
            lists = candidate_lists(targets, r, repo, loop, max(ks))
            for t, lst in zip(targets, lists):
                groups = ["all", t.event_type] + (["unseen"] if t.unseen else [])
                results.add(groups, lst, int(t.product_id))
            stats["targets"] += len(targets)
        apply_batch(batch, r, trend, args)
        trend.drop_before(batch[-1].epoch)
        stats["batches"] += 1
        batch = []

    for ev in read_events(args.file, end_time=parse_time(args.end)):
        row = to_row(ev)
        key = (row.user_id, row.product_id, row.event_type, row.epoch, row.user_session)
        if key in seen:
            stats["duplicates"] += 1
            continue
        seen[key] = row.epoch
        stats["events"] += 1

        # Warm-up micro-batches can be longer: nothing is measured there, and fewer,
        # larger batches merge the trending and co-occurrence writes (much faster).
        size = (args.batch_seconds if row.epoch >= warm_epoch
                else getattr(args, "warmup_batch_seconds", args.batch_seconds))
        if batch_end is None:
            batch_end = (row.epoch // size + 1) * size
        if row.epoch >= batch_end:
            flush_batch()
            batch_end = (row.epoch // size + 1) * size
            if len(seen) > 2_000_000:
                seen = {k: t for k, t in seen.items() if t >= row.epoch - DEDUP_SECONDS}

        prev = last.get(row.user_id)
        row.target = False
        if (row.epoch >= warm_epoch and prev is not None and row.epoch - prev[1] <= rk.DAY
                and prev[0] != row.product_id and sampled(row.user_id, args.sample)):
            row.target = True
            row.unseen = row.product_id not in recent.get(row.user_id, [])
        last[row.user_id] = (row.product_id, row.epoch)
        hist = recent.setdefault(row.user_id, [])
        hist.append(row.product_id)
        del hist[:-settings.recent_n]
        batch.append(row)

        if stats["events"] % 500_000 == 0:
            log.info("%s events, %s targets, event time %s, %.0f events/s", f"{stats['events']:,}",
                     f"{stats['targets']:,}", ev.event_time.isoformat(), stats["events"] / (time.time() - started))
    flush_batch()
    loop.run_until_complete(ar.aclose())
    loop.close()

    stats["seconds"] = round(time.time() - started, 1)
    return {
        "config": {"file": args.file, "warmup_until": args.warmup_until, "end": args.end,
                   "batch_seconds": args.batch_seconds,
                   "warmup_batch_seconds": getattr(args, "warmup_batch_seconds", args.batch_seconds),
                   "half_life_seconds": args.half_life,
                   "neighbours": args.neighbours, "sample": args.sample, "k": ks,
                   "rt_similarity": getattr(args, "rt_similarity", "cooc"),
                   "sim_item": getattr(args, "sim_item", None)},
        "stats": stats,
        "results": results.to_dict(),
    }


def markdown(result: dict, group: str = "all") -> str:
    ks = result["config"]["k"]
    head = "| Method | Targets | Coverage | " + " | ".join(f"HR@{k} | NDCG@{k}" for k in ks) + " |"
    lines = [head, "|---|---:|---:|" + "---:|---:|" * len(ks)]
    for m, row in result["results"].get(group, {}).items():
        cells = " | ".join(f"{row[f'hr@{k}']:.4f} | {row[f'ndcg@{k}']:.4f}" for k in ks)
        lines.append(f"| `{m}` | {row['targets']:,} | {row['coverage']:.1%} | {cells} |")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", action="append", required=True, help="raw CSV (.csv or .csv.gz), repeatable")
    p.add_argument("--warmup-until", default=None, help="events before this time only build the state")
    p.add_argument("--end", default=None, help="stop at this event time (exclusive)")
    p.add_argument("--batch-seconds", type=int, default=30, help="event time per micro-batch")
    p.add_argument("--warmup-batch-seconds", type=int, default=600,
                   help="event time per micro-batch before --warmup-until (state only)")
    p.add_argument("--half-life", type=float, default=DEFAULT_HALF_LIFE_SECONDS, help="seconds")
    p.add_argument("--neighbours", type=int, default=rt_sink.RT_NEIGHBOURS, help="K of sim:cooc per event")
    p.add_argument("--rt-similarity", choices=rt_sink.SIMILARITIES, default="cooc",
                   help="neighbours used by rec:rt (item / hybrid need --sim-item)")
    p.add_argument("--sim-item", default=None, help="CSV product_id,neighbour_id,sim loaded as sim:item")
    p.add_argument("--sample", type=float, default=1.0, help="fraction of users evaluated (all events are applied)")
    p.add_argument("--k", type=int, action="append", default=None, help="cut-offs (default 10 and 20)")
    p.add_argument("--redis-host", default="localhost")
    p.add_argument("--redis-port", type=int, default=6390)
    p.add_argument("--flush", action="store_true", help="empty a non-empty Redis database first")
    p.add_argument("--json", default=None, help="write the results to this file")
    args = p.parse_args()
    args.k = args.k or [10, 20]
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

    result = run(args)
    for group in ("all", "view", "cart", "purchase", "unseen"):
        if group in result["results"]:
            print(f"\n{group}\n{markdown(result, group)}")
    print(f"\n{result['stats']}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(result, f, indent=2)
            f.write("\n")


if __name__ == "__main__":
    main()
