"""Redis bulk loader for the keys Pair 1 owns (week 5). Key names come from common/redis_keys.py.

  pop:global, pop:cat:{category_id}   ZSET product_id -> long-term popularity
  sim:item:{product_id}               ZSET neighbour_id -> cosine similarity
  rec:batch:{user_id}                 ZSET product_id -> ALS score
  meta:batch:last_run                 ISO 8601 UTC time of the load, written last

How a load runs:
  1. Budget: the estimated size of each family (batch/rec_batch.py) must fit BUDGET_BYTES,
     otherwise nothing is written. Batch keys have no TTL, so Redis would never evict them.
  2. Each Spark partition opens its own connection and sends its keys in batches of
     `batch_keys`; a batch is one MULTI/EXEC transaction of DEL + ZADD per key, so the API never
     reads a list that is half written, and Redis is blocked for only a few ms per batch.
  3. Scores are rounded to SCORE_DECIMALS: half the memory, same order.
  4. Keys of a family that are not part of this load (users no longer recent, products dropped)
     are deleted with SCAN + UNLINK.
  5. meta:batch:last_run is written last, so it only moves once the whole load has finished.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import DataFrame, functions as F

from batch.rec_batch import BUDGET_BYTES, SCORE_DECIMALS, estimate_bytes
from common import redis_keys as rk
from common.config import settings

BATCH_KEYS = 200
WRITERS = 4
SCAN_COUNT = 1000


class BudgetError(RuntimeError):
    """A key family would not fit its Redis budget; nothing was written."""


@dataclass(frozen=True)
class RedisTarget:
    host: str = settings.redis_host
    port: int = settings.redis_port
    db: int = settings.redis_db

    def client(self):
        import redis

        return redis.Redis(host=self.host, port=self.port, db=self.db, decode_responses=True,
                           socket_timeout=30, socket_connect_timeout=10)


@dataclass(frozen=True)
class Family:
    """One kind of batch key: how its rows map to ZSETs."""
    budget: str                     # entry of BUDGET_BYTES
    key: Callable[[int], str]       # key helper from common/redis_keys.py
    key_col: str
    member_col: str
    score_col: str

    @property
    def prefix(self) -> str:
        return self.key("")


REC_BATCH = Family("rec:batch", rk.rec_batch, "user_id", "product_id", "score")
SIM_ITEM = Family("sim:item", rk.sim_item, "product_id", "neighbour_id", "sim")
POP_CAT = Family("pop", rk.pop_cat, "category_id", "product_id", "score")


def family_size(df: DataFrame, family: Family) -> tuple[int, int]:
    """(keys, members) that `df` would write."""
    row = df.agg(F.countDistinct(family.key_col).alias("keys"), F.count("*").alias("members")).first()
    return int(row["keys"]), int(row["members"])


def check_budget(sizes: dict[str, tuple[int, int]]) -> dict[str, int]:
    """Estimated bytes per budget entry; raises BudgetError when one is over."""
    estimated: dict[str, int] = {}
    for budget, (keys, members) in sizes.items():
        estimated[budget] = estimated.get(budget, 0) + estimate_bytes(keys, members)
    over = {b: v for b, v in estimated.items() if v > BUDGET_BYTES[b]}
    if over:
        raise BudgetError(f"over the Redis budget (bytes): {over}, budget {BUDGET_BYTES}")
    return estimated


def write_family(df: DataFrame, family: Family, target: RedisTarget, writers: int = WRITERS,
                 batch_keys: int = BATCH_KEYS, decimals: int = SCORE_DECIMALS) -> dict:
    """Replace one ZSET per key_col value with {member: rounded score}; returns key and member counts."""
    lists = (df.groupBy(family.key_col)
             .agg(F.collect_list(F.struct(F.col(family.member_col).alias("m"),
                                          # double first: a rounded float32 (sim) reads back as 0.9122999906539917
                                          F.round(F.col(family.score_col).cast("double"), decimals).alias("s")))
                  .alias("items"))
             .repartition(writers))
    key = family.key

    def write(rows):
        client = target.client()
        pipe = client.pipeline(transaction=True)
        keys = members = pending = 0
        for key_id, items in rows:
            name = key(key_id)
            pipe.delete(name)
            pipe.zadd(name, {int(i["m"]): float(i["s"]) for i in items})
            keys, members, pending = keys + 1, members + len(items), pending + 1
            if pending >= batch_keys:
                pipe.execute()
                pending = 0
        if pending:
            pipe.execute()
        client.close()
        yield keys, members

    counts = lists.rdd.mapPartitions(write).collect()
    return {"keys": sum(k for k, _ in counts), "members": sum(m for _, m in counts)}


def write_zset(client, name: str, scores: dict[int, float], decimals: int = SCORE_DECIMALS) -> None:
    """Replace one ZSET atomically (for small keys such as pop:global)."""
    pipe = client.pipeline(transaction=True)
    pipe.delete(name)
    if scores:
        pipe.zadd(name, {int(m): round(float(s), decimals) for m, s in scores.items()})
    pipe.execute()


def delete_stale(client, family: Family, keep: set[int]) -> int:
    """UNLINK the family's keys whose id is not in `keep`; returns how many were deleted."""
    prefix, stale, deleted = family.prefix, [], 0
    for name in client.scan_iter(match=prefix + "*", count=SCAN_COUNT):
        suffix = name[len(prefix):]
        if suffix.lstrip("-").isdigit() and int(suffix) not in keep:
            stale.append(name)
        if len(stale) >= SCAN_COUNT:
            deleted += client.unlink(*stale)
            stale = []
    if stale:
        deleted += client.unlink(*stale)
    return deleted


def set_last_run(client, when: datetime | None = None) -> str:
    stamp = (when or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    client.set(rk.META_BATCH_LAST_RUN, stamp)
    return stamp


def load(target: RedisTarget, rec_batch: DataFrame | None = None, sim_item: DataFrame | None = None,
         pop_global: DataFrame | None = None, pop_cat: DataFrame | None = None,
         writers: int = WRITERS, batch_keys: int = BATCH_KEYS, when: datetime | None = None) -> dict:
    """Write the given tables, delete stale keys, then meta:batch:last_run.

    rec_batch: (user_id, product_id, score); sim_item: (product_id, neighbour_id, sim);
    pop_global: (product_id, score); pop_cat: (category_id, product_id, score).
    """
    tables = [(f, df) for f, df in ((POP_CAT, pop_cat), (SIM_ITEM, sim_item), (REC_BATCH, rec_batch))
              if df is not None]
    sizes = {f.prefix: family_size(df, f) for f, df in tables}
    budget_sizes: dict[str, tuple[int, int]] = {}
    for f, df in tables:
        k, m = sizes[f.prefix]
        old = budget_sizes.get(f.budget, (0, 0))
        budget_sizes[f.budget] = (old[0] + k, old[1] + m)
    global_rows = pop_global.select("product_id", "score").collect() if pop_global is not None else None
    if global_rows is not None:
        old = budget_sizes.get("pop", (0, 0))
        budget_sizes["pop"] = (old[0] + 1, old[1] + len(global_rows))
    estimated = check_budget(budget_sizes)

    client = target.client()
    stats: dict = {"estimated_bytes": estimated, "families": {}}
    if global_rows is not None:
        write_zset(client, rk.POP_GLOBAL, {r["product_id"]: r["score"] for r in global_rows})
        stats["families"][rk.POP_GLOBAL] = {"keys": 1, "members": len(global_rows)}
    for f, df in tables:
        written = write_family(df, f, target, writers, batch_keys)
        keep = {int(r[0]) for r in df.select(f.key_col).distinct().toLocalIterator()}
        written["stale_deleted"] = delete_stale(client, f, keep)
        stats["families"][f.prefix] = written
    stats["last_run"] = set_last_run(client, when)
    client.close()
    return stats


def inspect(target: RedisTarget, sample: int = 100) -> dict:
    """Key counts and sampled memory per family, Redis memory and the last run (validate step)."""
    client = target.client()
    result = {}
    for prefix in (rk.POP_GLOBAL, POP_CAT.prefix, SIM_ITEM.prefix, REC_BATCH.prefix):
        pattern = prefix if prefix == rk.POP_GLOBAL else prefix + "*"
        count, keys = 0, []
        for key in client.scan_iter(match=pattern, count=SCAN_COUNT):
            count += 1
            if len(keys) < sample:
                keys.append(key)
        usage = [client.memory_usage(k) or 0 for k in keys]
        members = [client.zcard(k) for k in keys]
        result[prefix] = {
            "keys": count,
            "sampled_mean_members": round(sum(members) / len(members), 1) if members else 0,
            "estimated_mb": round(sum(usage) / len(usage) * count / (1 << 20), 1) if usage else 0.0,
            "sample_key": keys[0] if keys else None,
        }
    mem = client.info("memory")
    out = {"families": result, "last_run": client.get(rk.META_BATCH_LAST_RUN),
           "used_memory_mb": round(mem["used_memory"] / (1 << 20), 1),
           "maxmemory_mb": round(mem.get("maxmemory", 0) / (1 << 20), 1),
           "dbsize": client.dbsize()}
    key = result[REC_BATCH.prefix]["sample_key"]
    if key:
        out["sample_rec_batch"] = {"key": key, "top5": client.zrevrange(key, 0, 4, withscores=True)}
    client.close()
    return out
