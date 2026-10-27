"""Nightly batch pipeline (Pair 1, week 5).

One step per command, so Airflow (batch/dags/nightly_batch.py) runs, retries and times each
step on its own, and the very same commands can be run by hand:

  python -m batch.nightly <step> --run-id 20261028 [--as-of 2019-11-16] [--train-days N]

  step        reads                              writes (under --root/<run-id>/)
  check       clean Parquet                      -   (fails if the window has no events)
  popularity  clean Parquet                      pop_global/, pop_cat/
  train       clean Parquet                      model/ (ALS), activity/ (each user's last event)
  recommend   model/, activity/                  rec_batch/ (most recent users within the budget)
  sim         clean Parquet (or model/)          sim_item/ (Item-kNN; --sim-source als: ALS factors)
  load        rec_batch/, sim_item/, pop_*/      Redis (batch/redis_loader.py)
  validate    Redis                              -   (fails on empty families or a full Redis)
  cleanup     --root                             keeps only the newest --keep-runs run folders

The run uses the events of [as_of - train_days, as_of). as_of defaults to the first test day,
2019-11-16, so the batch layer only knows what happened before the period Pair 2 replays;
train_days = 0 means from the first day of the data. Each step prints a JSON summary and also
saves it to <stats-dir>/<run-id>/<step>.json.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, SparkSession, functions as F

from batch import als, item_knn, rec_batch, redis_loader, sim_item
from batch.interactions import filter_rare, interaction_scores, matrix_stats
from batch.popularity import product_popularity, top_by_category, top_global
from batch.spark_session import get_spark
from batch.split import TEST_START
from common.config import settings
from common.schemas import CLEAN_PARTITION_COLUMN

STEPS = ("check", "popularity", "train", "recommend", "sim", "load", "validate", "cleanup")
# sim:item source. Item-kNN won every quality measure of the week 4 comparison
# (docs/week-04.md); the ALS factor table is kept for the similarity ablation.
SIM_SOURCES = ("item_knn", "als")
POP_TOP_N = 100     # products in pop:global and each pop:cat, whatever batch_top_n (rec:batch) is
DEFAULT_ROOT = os.getenv("BATCH_ROOT", "s3a://lake/batch/")
DEFAULT_STATS_DIR = os.getenv("BATCH_STATS_DIR", "data/batch-runs")


def window(cfg) -> tuple[date | None, date]:
    as_of = date.fromisoformat(cfg.as_of)
    return (as_of - timedelta(days=cfg.train_days) if cfg.train_days else None), as_of


def events(spark: SparkSession, cfg) -> DataFrame:
    """Clean events of the training window (a partition filter: only those dates are read)."""
    start, as_of = window(cfg)
    day = F.col(CLEAN_PARTITION_COLUMN)
    cond = day < F.lit(as_of)
    if start:
        cond &= day >= F.lit(start)
    return spark.read.parquet(cfg.input).where(cond)


def path(cfg, name: str) -> str:
    return f"{cfg.root.rstrip('/')}/{cfg.run_id}/{name}"


def target(cfg) -> redis_loader.RedisTarget:
    return redis_loader.RedisTarget(cfg.redis_host, cfg.redis_port, cfg.redis_db)


def als_params(cfg) -> als.ALSParams:
    return als.ALSParams(rank=cfg.rank, reg_param=cfg.reg, alpha=cfg.alpha, max_iter=cfg.iter)


# ---- steps ----------------------------------------------------------------

def check(spark, cfg) -> dict:
    s = events(spark, cfg).agg(F.count("*").alias("events"), F.countDistinct("user_id").alias("users"),
                               F.min(CLEAN_PARTITION_COLUMN).alias("first_day"),
                               F.max(CLEAN_PARTITION_COLUMN).alias("last_day")).first().asDict()
    start, as_of = window(cfg)
    if not s["events"]:
        raise RuntimeError(f"no clean events before {as_of} in {cfg.input}")
    if s["last_day"] < as_of - timedelta(days=1):
        raise RuntimeError(f"clean data stops at {s['last_day']}, the day before as_of ({as_of}) is missing")
    return {**s, "first_day": str(s["first_day"]), "last_day": str(s["last_day"]),
            "window": [str(start) if start else None, str(as_of)]}


def popularity(spark, cfg) -> dict:
    pop = product_popularity(events(spark, cfg)).cache()
    top_global(pop, cfg.pop_n).write.mode("overwrite").parquet(path(cfg, "pop_global"))
    top_by_category(pop, cfg.pop_n).write.mode("overwrite").parquet(path(cfg, "pop_cat"))
    cats = spark.read.parquet(path(cfg, "pop_cat")).agg(F.countDistinct("category_id"), F.count("*")).first()
    return {"products": pop.count(), "categories": cats[0], "pop_cat_rows": cats[1], "top_n": cfg.pop_n}


def train(spark, cfg) -> dict:
    ev = events(spark, cfg)
    matrix = filter_rare(interaction_scores(ev)).cache()
    stats = {"als_matrix": matrix_stats(matrix), "als_params": als_params(cfg).as_dict()}
    checkpoint_dir = tempfile.mkdtemp(prefix="als-nightly-")
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    t = time.time()
    try:
        model = als.train(matrix, als_params(cfg))
        model.write().overwrite().save(path(cfg, "model"))
    finally:
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
    stats["train_seconds"] = round(time.time() - t, 1)
    rec_batch.last_activity(ev, matrix).write.mode("overwrite").parquet(path(cfg, "activity"))
    return stats


def recommend(spark, cfg) -> dict:
    model = ALSModel.load(path(cfg, "model"))
    activity = spark.read.parquet(path(cfg, "activity"))
    limit = cfg.max_users or rec_batch.max_users(cfg.top_n)
    users = rec_batch.most_recent_users(activity, limit).cache()
    als.recommend(model, users, cfg.top_n).write.mode("overwrite").parquet(path(cfg, "rec_batch"))
    recs = spark.read.parquet(path(cfg, "rec_batch"))
    n_users, rows = recs.agg(F.countDistinct("user_id"), F.count("*")).first()
    oldest = users.agg(F.min("last_event")).first()[0]
    return {"als_users": activity.count(), "user_limit": limit, "users": n_users, "rows": rows,
            "top_n": cfg.top_n, "oldest_last_event": str(oldest),
            "estimated_mb": round(rec_batch.estimate_bytes(n_users, rows) / rec_batch.MiB, 1)}


def sim(spark, cfg) -> dict:
    if cfg.sim_source == "als":
        model = ALSModel.load(path(cfg, "model"))
        table, candidates = sim_item.from_model(model, block_size=cfg.sim_block_size), model.itemFactors.count()
    else:
        matrix = filter_rare(interaction_scores(events(spark, cfg))).cache()   # the matrix ALS is trained on
        table, candidates = item_knn.similarities(matrix), matrix.select("product_id").distinct().count()
    table.select("product_id", "neighbour_id", "sim").write.mode("overwrite").parquet(path(cfg, "sim_item"))
    s = spark.read.parquet(path(cfg, "sim_item")).agg(F.countDistinct("product_id"), F.count("*"), F.avg("sim")).first()
    return {"source": cfg.sim_source, "products_in_matrix": candidates, "products": s[0], "rows": s[1],
            "mean_sim": round(s[2] or 0.0, 4)}


def load(spark, cfg) -> dict:
    read = lambda name: spark.read.parquet(path(cfg, name))
    return redis_loader.load(target(cfg), rec_batch=read("rec_batch"), sim_item=read("sim_item"),
                             pop_global=read("pop_global"), pop_cat=read("pop_cat"),
                             writers=cfg.writers)


def validate(spark, cfg) -> dict:
    report = redis_loader.inspect(target(cfg))
    fams = report["families"]
    empty = [name for name, f in fams.items() if not f["keys"]]
    if empty or not report["last_run"]:
        raise RuntimeError(f"batch keys missing in Redis: {empty or 'meta:batch:last_run'}")
    if report["maxmemory_mb"] and report["used_memory_mb"] > 0.95 * report["maxmemory_mb"]:
        raise RuntimeError(f"Redis is nearly full: {report['used_memory_mb']} of {report['maxmemory_mb']} MB")
    return report


def cleanup(spark, cfg) -> dict:
    jvm = spark.sparkContext._jvm
    root = jvm.org.apache.hadoop.fs.Path(cfg.root)
    fs = root.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(root):
        return {"kept": [], "deleted": []}
    runs = sorted(s.getPath().getName() for s in fs.listStatus(root) if s.isDirectory())
    old = runs[:-cfg.keep_runs] if cfg.keep_runs else runs
    for name in old:
        fs.delete(jvm.org.apache.hadoop.fs.Path(root, name), True)
    return {"kept": runs[len(old):], "deleted": old}


RUNNERS = {"check": check, "popularity": popularity, "train": train, "recommend": recommend,
           "sim": sim, "load": load, "validate": validate, "cleanup": cleanup}


def run_step(spark: SparkSession, step: str, cfg) -> dict:
    t = time.time()
    stats = RUNNERS[step](spark, cfg)
    stats = {"step": step, "run_id": cfg.run_id, "as_of": cfg.as_of, **stats, "seconds": round(time.time() - t, 1)}
    if cfg.stats_dir:
        out = Path(cfg.stats_dir) / cfg.run_id / f"{step}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(stats, indent=2, default=str) + "\n", encoding="utf-8")
    return stats


def parser() -> argparse.ArgumentParser:
    d = als.ALSParams()
    p = argparse.ArgumentParser(description="Nightly batch: train ALS, sim:item, popularity, load Redis")
    p.add_argument("step", choices=STEPS)
    p.add_argument("--run-id", required=True, help="folder name of this run, e.g. the Airflow ds_nodash")
    p.add_argument("--as-of", default=str(TEST_START), help="use events before this date (YYYY-MM-DD)")
    p.add_argument("--train-days", type=int, default=0, help="days before as_of to train on, 0 = all")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--root", default=DEFAULT_ROOT, help="where the runs keep their tables")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--stats-dir", default=DEFAULT_STATS_DIR, help="JSON summary per step ('' = none)")
    p.add_argument("--rank", type=int, default=d.rank)
    p.add_argument("--reg", type=float, default=d.reg_param)
    p.add_argument("--alpha", type=float, default=d.alpha)
    p.add_argument("--iter", type=int, default=d.max_iter)
    p.add_argument("--top-n", type=int, default=settings.batch_top_n, help="products per user in rec:batch")
    p.add_argument("--max-users", type=int, default=0, help="rec:batch users, 0 = as many as the budget allows")
    p.add_argument("--pop-n", type=int, default=POP_TOP_N, help="products in pop:global and each pop:cat")
    p.add_argument("--sim-source", choices=SIM_SOURCES, default=SIM_SOURCES[0], help="where sim:item comes from")
    p.add_argument("--sim-block-size", type=int, default=sim_item.DEFAULT_BLOCK_SIZE, help="--sim-source als only")
    p.add_argument("--redis-host", default=settings.redis_host)
    p.add_argument("--redis-port", type=int, default=settings.redis_port)
    p.add_argument("--redis-db", type=int, default=settings.redis_db)
    p.add_argument("--writers", type=int, default=redis_loader.WRITERS, help="parallel Redis connections")
    p.add_argument("--keep-runs", type=int, default=2, help="cleanup: run folders to keep")
    return p


def main(argv=None) -> None:
    cfg = parser().parse_args(argv)
    needs_spark = cfg.step != "validate"
    spark = get_spark(f"nightly-{cfg.step}", s3=not cfg.no_s3) if needs_spark else None
    if spark:
        spark.sparkContext.setLogLevel("WARN")
    stats = run_step(spark, cfg.step, cfg)
    print(json.dumps(stats, indent=2, default=str))
    if spark:
        spark.stop()


if __name__ == "__main__":
    main()
