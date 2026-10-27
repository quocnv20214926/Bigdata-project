"""Offline evaluation: Popularity baseline vs implicit ALS on the time-based split (Pair 1, week 3).

Protocol (details in evaluation/README.md):
  - train = 2019-10-01 .. 11-15, test = 11-16 .. 11-30 (batch/split.py)
  - ground truth = distinct products each user interacted with in the test period
    (--truth-events, default every event type)
  - evaluated users = users with ground truth who are in the filtered ALS matrix,
    a deterministic sample of --eval-users of them; both models score the same users

Usage (from the repository root):
  SPARK_DRIVER_MEMORY=14g python -m evaluation.offline --report docs/eval-week03.json
  python -m evaluation.offline --input data/clean/events --no-s3 --eval-users 20000
  python -m evaluation.offline --truth-events cart purchase      # purchase intent only
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from batch import als
from batch.interactions import (
    MIN_PRODUCTS_PER_USER, MIN_USERS_PER_PRODUCT, filter_rare, interaction_scores, matrix_stats,
)
from batch.popularity import product_popularity, recommend_popular, top_global
from batch.spark_session import get_spark
from batch.split import TEST_END, TEST_START, TRAIN_END, TRAIN_START, load_split
from common.config import settings
from evaluation.metrics import catalog_coverage, ranking_metrics

DEFAULT_KS = (10, 20)
DEFAULT_EVAL_USERS = 100_000
TRUTH_EVENTS = ("view", "cart", "purchase")


def ground_truth(test: DataFrame, event_types=TRUTH_EVENTS) -> DataFrame:
    return (test.where(F.col("event_type").isin(*event_types))
            .select("user_id", "product_id").distinct())


def sample_users(users: DataFrame, n: int, seed: int) -> DataFrame:
    """The same n users on every run with the same seed (0 = all users)."""
    if not n:
        return users
    return users.orderBy(F.xxhash64(F.col("user_id"), F.lit(seed))).limit(n)


def run(train: DataFrame, test: DataFrame, params: als.ALSParams = als.ALSParams(),
        eval_users: int = DEFAULT_EVAL_USERS, truth_events=TRUTH_EVENTS, ks=DEFAULT_KS,
        model_out: str | None = None) -> dict:
    timings: dict[str, float] = {}
    started = time.time()

    interactions = interaction_scores(train).cache()
    matrix = filter_rare(interactions).cache()
    data = {"interactions": matrix_stats(interactions), "als_matrix": matrix_stats(matrix)}
    timings["interactions"] = round(time.time() - started, 1)

    truth_all = ground_truth(test, truth_events)
    candidates = truth_all.select("user_id").distinct().join(matrix.select("user_id"), "user_id", "left_semi")
    users = sample_users(candidates, eval_users, params.seed).cache()
    truth = truth_all.join(users, "user_id", "left_semi").cache()
    data["ground_truth"] = {"users": users.count(), "pairs": truth.count()}

    k_max = max(ks)
    t = time.time()
    top = top_global(product_popularity(train), k_max).cache()
    recs = {"popularity": recommend_popular(users, top, k_max)}
    timings["popularity"] = round(time.time() - t, 1)

    t = time.time()
    model = als.train(matrix, params)
    if model_out:
        model.write().overwrite().save(model_out)
    timings["als_train"] = round(time.time() - t, 1)
    t = time.time()
    recs["als"] = als.recommend(model, users, k_max).cache()
    recs["als"].count()
    timings["als_recommend"] = round(time.time() - t, 1)

    k_min = min(ks)
    results = {}
    for name, r in recs.items():
        t = time.time()
        results[name] = ranking_metrics(r, truth, ks)
        results[name][f"coverage@{k_min}"] = catalog_coverage(r, k_min, data["als_matrix"]["products"])
        timings[f"evaluate_{name}"] = round(time.time() - t, 1)
    timings["total"] = round(time.time() - started, 1)

    return {
        "protocol": {
            "train": [str(TRAIN_START), str(TRAIN_END)],
            "test": [str(TEST_START), str(TEST_END)],
            "truth_events": list(truth_events),
            "eval_users_requested": eval_users,
            "filter": {"min_users_per_product": MIN_USERS_PER_PRODUCT,
                       "min_products_per_user": MIN_PRODUCTS_PER_USER},
            "ks": list(ks),
        },
        "als_params": params.as_dict(),
        "data": data,
        "results": results,
        "seconds": timings,
    }


def format_table(results: dict) -> str:
    names = list(next(iter(results.values())).keys())
    names.remove("users")
    lines = ["| model | " + " | ".join(names) + " |", "|---" * (len(names) + 1) + "|"]
    for model, m in results.items():
        lines.append(f"| {model} | " + " | ".join("-" if m[n] is None else f"{m[n]:.4f}" for n in names) + " |")
    return "\n".join(lines)


def main() -> None:
    defaults = als.ALSParams()
    p = argparse.ArgumentParser(description="Popularity vs implicit ALS on the time-based split")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--report", help="write the results (JSON) to this path")
    p.add_argument("--eval-users", type=int, default=DEFAULT_EVAL_USERS, help="0 = every eligible user")
    p.add_argument("--truth-events", nargs="+", default=list(TRUTH_EVENTS), choices=TRUTH_EVENTS)
    p.add_argument("--rank", type=int, default=defaults.rank)
    p.add_argument("--reg", type=float, default=defaults.reg_param)
    p.add_argument("--alpha", type=float, default=defaults.alpha)
    p.add_argument("--iter", type=int, default=defaults.max_iter)
    p.add_argument("--seed", type=int, default=defaults.seed)
    p.add_argument("--model-out", help="also save the ALS model here (used by sim:item)")
    args = p.parse_args()

    spark = get_spark("batch-eval", s3=not args.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    # ALS checkpoints its factors every 10 iterations when a checkpoint dir is set.
    spark.sparkContext.setCheckpointDir(tempfile.mkdtemp(prefix="als-checkpoint-"))
    train, test = load_split(spark, args.input)
    params = als.ALSParams(rank=args.rank, reg_param=args.reg, alpha=args.alpha,
                           max_iter=args.iter, seed=args.seed)
    result = run(train, test, params, args.eval_users, tuple(args.truth_events), model_out=args.model_out)
    result["input"] = args.input

    print(json.dumps(result, indent=2))
    print(format_table(result["results"]))
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
