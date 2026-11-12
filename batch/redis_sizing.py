"""How big rec:batch can be: Redis memory per key and the users it then covers (Pair 1, week 4).

1. Measure (needs a Redis, skipped with --no-measure): write sample ZSETs shaped like rec:batch
   (REES46-like product ids, ALS-like scores) into an EMPTY database (--db, default 15), read
   used_memory before and after, delete them again. Compared with batch/rec_batch.py's formula.
2. Coverage, on the validation week (batch/split.py), so the test split stays untouched:
   for each top-N, rec:batch holds max_users(N) users, the most recently active users of the
   fit-period ALS matrix. Reported: how many of the ALS users who come back in the validation
   week have a list, and the share of validation cart and purchase events made by those users
   (the week 8 batch + speed experiment scores recommendations at exactly those events).

Usage (from the repository root, MinIO and Redis running):
  python -m batch.redis_sizing --report docs/redis-sizing-week04.json
  python -m batch.redis_sizing --no-measure --input data/clean/events --no-s3
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from batch import rec_batch
from batch.interactions import filter_rare, interaction_scores
from batch.spark_session import get_spark
from batch.split import FIT_END, VALID_END, VALID_START, load_validation_split
from common import redis_keys as rk
from common.config import settings

SIZES = (20, 50, 100)
TOP_NS = (50, 100)
SAMPLE_KEYS = 5000
DAY_WINDOWS = (1, 3, 7, 14, 30)


def sample_zset(rng: random.Random, n: int, decimals: int | None) -> dict[int, float]:
    """n REES46-like product ids (7 or 8 digits) with ALS-like scores in [0, 1.2)."""
    members = {}
    while len(members) < n:
        pid = rng.randrange(1_000_000, 8_000_000) if rng.random() < 0.5 else rng.randrange(10_000_000, 100_000_000)
        score = rng.random() * 1.2
        members[pid] = round(score, decimals) if decimals is not None else score
    return members


def measure(client, sizes=SIZES, keys: int = SAMPLE_KEYS, seed: int = 7) -> dict:
    """used_memory per key for ZSETs of each size, rounded and unrounded scores.

    `client` must point at an empty database: the sample keys are deleted afterwards and
    nothing else is touched.
    """
    if client.dbsize():
        raise RuntimeError("the sizing database is not empty; pick another one with --db")
    rng, result = random.Random(seed), {}
    for n in sizes:
        for decimals in (rec_batch.SCORE_DECIMALS, None):
            names = [rk.rec_batch(500_000_000 + i) for i in range(keys)]
            before = client.info("memory")["used_memory"]
            pipe = client.pipeline(transaction=False)
            for name in names:
                pipe.zadd(name, sample_zset(rng, n, decimals))
            pipe.execute()
            after = client.info("memory")["used_memory"]
            usage = [client.memory_usage(name) for name in names[:200]]
            encoding = client.object("encoding", names[0])
            client.unlink(*names)
            label = f"{n}_rounded" if decimals is not None else f"{n}_raw"
            result[label] = {"members": n, "bytes_per_key": round((after - before) / keys),
                             "memory_usage_per_key": round(sum(usage) / len(usage)),
                             "formula": rec_batch.estimate_bytes(1, n) if decimals is not None else None,
                             "encoding": encoding}
    info = client.info("server")
    return {"redis_version": info.get("redis_version"), "keys_per_size": keys, "sizes": result}


def coverage(spark, path: str | None, top_ns=TOP_NS) -> dict:
    """Validation-week coverage of rec:batch for each top-N within the rec:batch budget."""
    fit, valid = load_validation_split(spark, path)
    matrix = filter_rare(interaction_scores(fit)).select("user_id").distinct().cache()
    activity = rec_batch.with_recency_rank(rec_batch.last_activity(fit, matrix)).cache()
    als_users = activity.count()

    end = datetime(FIT_END.year, FIT_END.month, FIT_END.day, tzinfo=timezone.utc).timestamp() + 86400
    by_days = activity.agg(*[F.sum((F.col("last_event") >= F.lit(end - d * 86400).cast("timestamp")).cast("int"))
                             .alias(str(d)) for d in DAY_WINDOWS]).first().asDict()

    returning = valid.select("user_id").distinct().join(activity, "user_id")
    intent = (valid.where(F.col("event_type").isin("cart", "purchase")).select("user_id")
              .join(activity, "user_id", "left"))
    ret_total, intent_total = returning.count(), intent.count()
    options = {}
    for n in top_ns:
        m = min(rec_batch.max_users(n), als_users)
        covered = returning.where(F.col("recency_rank") <= m).count()
        hits = intent.where(F.col("recency_rank") <= m).count()
        cutoff = activity.where(F.col("recency_rank") == m).select("last_event").first()
        options[f"top_{n}"] = {
            "top_n": n, "users": m,
            "estimated_mb": round(rec_batch.estimate_bytes(m, m * n) / rec_batch.MiB, 1),
            "oldest_last_event": str(cutoff[0]) if cutoff else None,
            "returning_users_covered": round(covered / ret_total, 4) if ret_total else 0.0,
            "intent_events_covered": round(hits / intent_total, 4) if intent_total else 0.0,
        }
    every = intent.where(F.col("recency_rank").isNotNull()).count()
    return {
        "fit_end": str(FIT_END), "validation": [str(VALID_START), str(VALID_END)],
        "als_users": als_users,
        "als_users_active_in_last_days": {k: int(v or 0) for k, v in by_days.items()},
        "all_als_users_top_100_mb": round(rec_batch.estimate_bytes(als_users, als_users * 100) / rec_batch.MiB, 1),
        "validation_returning_als_users": ret_total,
        "validation_intent_events": intent_total,
        "intent_events_by_als_users": round(every / intent_total, 4) if intent_total else 0.0,
        "options": options,
    }


def choose(options: dict, margin: float = 0.03) -> str:
    """Keep top-100 (the contract value) unless top-50 covers > margin more intent events."""
    a, b = options["top_100"]["intent_events_covered"], options["top_50"]["intent_events_covered"]
    return "top_50" if b - a > margin else "top_100"


def main() -> None:
    p = argparse.ArgumentParser(description="Redis memory per rec:batch key and the users it covers")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--report", help="write the results (JSON) to this path")
    p.add_argument("--no-measure", action="store_true", help="skip the Redis measurement")
    p.add_argument("--no-coverage", action="store_true", help="skip the Spark part")
    p.add_argument("--redis-host", default=settings.redis_host)
    p.add_argument("--redis-port", type=int, default=settings.redis_port)
    p.add_argument("--db", type=int, default=15, help="an empty Redis database for the sample keys")
    p.add_argument("--keys", type=int, default=SAMPLE_KEYS, help="sample keys per size")
    args = p.parse_args()

    started, report = time.time(), {"budget_mb": {k: v / rec_batch.MiB for k, v in rec_batch.BUDGET_BYTES.items()},
                                    "formula": {"key_bytes": rec_batch.ZSET_KEY_BYTES,
                                                "entry_bytes": rec_batch.ZSET_ENTRY_BYTES,
                                                "score_decimals": rec_batch.SCORE_DECIMALS}}
    if not args.no_measure:
        import redis

        client = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.db, decode_responses=True)
        report["measured"] = measure(client, keys=args.keys)
    if not args.no_coverage:
        spark = get_spark("batch-redis-sizing", s3=not args.no_s3)
        spark.sparkContext.setLogLevel("WARN")
        report["coverage"] = coverage(spark, args.input)
        report["choice"] = choose(report["coverage"]["options"])
        spark.stop()
    report["seconds"] = round(time.time() - started, 1)
    print(json.dumps(report, indent=2))
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
