"""Final offline evaluation of the batch layer (Pair 1, week 7).

Same split and ground truth as the week 3 protocol (evaluation/README.md), three additions:

  1. 95% bootstrap intervals over users for every metric, and paired intervals for the
     differences between models (evaluation/bootstrap.py).
  2. User segments by their training history (distinct products with a positive event weight):
     new (none), light (1 to 5) and heavy (more than 5). Every test user can be scored, not only
     the users ALS knows: a model that has no list, or a short one, for a user is completed with
     the Popularity list, as the API does when rec:batch is missing.
  3. An "unseen" variant: products the user already interacted with in train are removed from
     the lists (which are then refilled from deeper in the model's ranking and from Popularity)
     and from the ground truth; users left without ground truth drop out of that variant.

Users scored:
  - warm: the week 3 sample (--eval-users test users who are in the filtered ALS matrix, same
    seed), so the "all" numbers are comparable with weeks 3 to 6;
  - segments: --segment-users test users drawn from each segment with the same seed.

Usage (from the repository root, MinIO running):
  SPARK_DRIVER_MEMORY=14g python -m evaluation.final_eval --report docs/eval-week07.json
  python -m evaluation.final_eval --input data/clean/events --no-s3 --eval-users 20000 --segment-users 5000
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, Window, functions as F

from batch import als, item_knn
from batch.interactions import filter_rare, interaction_scores, matrix_stats
from batch.popularity import product_popularity, recommend_popular, top_global
from batch.spark_session import get_spark
from batch.split import TEST_END, TEST_START, TRAIN_END, TRAIN_START, load_split
from common.config import settings
from evaluation import bootstrap
from evaluation.metrics import catalog_coverage, per_user_metrics
from evaluation.offline import DEFAULT_EVAL_USERS, DEFAULT_KS, TRUTH_EVENTS, ground_truth, sample_users

MODELS = ("popularity", "item_knn", "als")
VARIANTS = ("all", "unseen")
SEGMENTS = ("new", "light", "heavy")
LIGHT_MAX = 5                   # distinct training products of a light user
DEFAULT_SEGMENT_USERS = 30_000
EXTRA_DEPTH = 100               # candidates kept beyond k, to refill lists after removing seen products
COMPARISONS = (("als", "popularity"), ("als", "item_knn"), ("item_knn", "popularity"))


# ---- users -----------------------------------------------------------------------------------

def user_segments(interactions: DataFrame, users: DataFrame) -> DataFrame:
    """(user_id, train_products, segment) for the given users."""
    counts = interactions.groupBy("user_id").agg(F.count("*").alias("train_products"))
    n = F.col("train_products")
    return (users.select("user_id").distinct().join(counts, "user_id", "left")
            .fillna(0, ["train_products"])
            .withColumn("segment", F.when(n == 0, "new").when(n <= LIGHT_MAX, "light").otherwise("heavy")))


def sample_per_segment(segments: DataFrame, n: int, seed: int) -> DataFrame:
    """Up to n users of each segment, the same ones for the same seed (0 = every user)."""
    if not n:
        return segments
    order = Window.partitionBy("segment").orderBy(F.xxhash64(F.col("user_id"), F.lit(seed)), "user_id")
    return segments.withColumn("pos", F.row_number().over(order)).where(F.col("pos") <= n).drop("pos")


# ---- lists -----------------------------------------------------------------------------------

def rerank(recs: DataFrame, depth: int, order: str = "rank") -> DataFrame:
    w = Window.partitionBy("user_id").orderBy(F.col(order), F.col("product_id"))
    return (recs.withColumn("rank", F.row_number().over(w)).where(F.col("rank") <= depth)
            .select("user_id", "product_id", "rank"))


def complete(recs: DataFrame, fallback: DataFrame, depth: int) -> DataFrame:
    """The model's list, then fallback products it does not already hold, cut at `depth`."""
    offset = F.lit(10 ** 6).cast("long")     # any fallback product ranks after every model product
    both = (recs.select("user_id", "product_id", F.col("rank").cast("long").alias("pos"))
            .unionByName(fallback.select("user_id", "product_id",
                                         (F.col("rank").cast("long") + offset).alias("pos")))
            .groupBy("user_id", "product_id").agg(F.min("pos").alias("pos")))
    return rerank(both, depth, order="pos")


def final_lists(model_recs: DataFrame, popular: DataFrame, seen: DataFrame | None, k: int) -> DataFrame:
    """Top-k list per user: the model's ranking completed by Popularity; with `seen`, products the
    user interacted with in train are removed first."""
    if seen is not None:
        model_recs = model_recs.join(seen, ["user_id", "product_id"], "left_anti")
        popular = popular.join(seen, ["user_id", "product_id"], "left_anti")
    return complete(model_recs, popular, k)


# ---- report ----------------------------------------------------------------------------------

def summarise(frame: pd.DataFrame, metric_names, resamples: int, seed: int) -> dict:
    """Bootstrap means and intervals per model, paired differences, for one group of users."""
    out = {"users": int(len(frame)), "models": {}, "differences": {}}
    if not len(frame):
        return out
    for model in MODELS:
        cols = [f"{model}:{m}" for m in metric_names]
        cis = bootstrap.mean_ci(frame[cols].to_numpy(), resamples, seed)
        out["models"][model] = dict(zip(metric_names, cis))
    for a, b in COMPARISONS:
        diff = bootstrap.paired_diff_ci(frame[[f"{a}:{m}" for m in metric_names]].to_numpy(),
                                        frame[[f"{b}:{m}" for m in metric_names]].to_numpy(), resamples, seed)
        out["differences"][f"{a} - {b}"] = dict(zip(metric_names, diff))
    return out


def run(train: DataFrame, test: DataFrame, params: als.ALSParams = als.ALSParams(),
        eval_users: int = DEFAULT_EVAL_USERS, segment_users: int = DEFAULT_SEGMENT_USERS,
        truth_events=TRUTH_EVENTS, ks=DEFAULT_KS, resamples: int = bootstrap.DEFAULT_RESAMPLES,
        knn_params: item_knn.KNNParams = item_knn.KNNParams()) -> dict:
    timings: dict[str, float] = {}
    started = t = time.time()
    ks = sorted(set(ks))
    k_max, depth = ks[-1], ks[-1] + EXTRA_DEPTH
    metric_names = [f"{m}@{k}" for k in ks for m in ("precision", "recall", "hit_rate", "ndcg")]

    interactions = interaction_scores(train).cache()
    matrix = filter_rare(interactions).cache()
    data = {"interactions": matrix_stats(interactions), "als_matrix": matrix_stats(matrix)}
    seen = interactions.select("user_id", "product_id")

    truth_all = ground_truth(test, truth_events).cache()
    truth_users = truth_all.select("user_id").distinct()
    segments = user_segments(interactions, truth_users).cache()
    data["segments"] = {r["segment"]: r["count"] for r in segments.groupBy("segment").count().collect()}
    warm = sample_users(truth_users.join(matrix.select("user_id"), "user_id", "left_semi"),
                        eval_users, params.seed).withColumn("warm", F.lit(True)).cache()
    sampled = sample_per_segment(segments, segment_users, params.seed).withColumn("sampled", F.lit(True))
    users = (segments.join(warm, "user_id", "left").join(sampled.select("user_id", "sampled"), "user_id", "left")
             .where(F.col("warm") | F.col("sampled"))
             .fillna(False, ["warm", "sampled"]).cache())
    data["scored_users"] = users.count()
    truth = {"all": truth_all.join(users, "user_id", "left_semi").cache()}
    truth["unseen"] = truth["all"].join(seen, ["user_id", "product_id"], "left_anti").cache()
    data["truth"] = {v: t.agg(F.countDistinct("user_id").alias("users"), F.count("*").alias("pairs"))
                     .first().asDict() for v, t in truth.items()}
    timings["prepare"] = round(time.time() - t, 1)

    t = time.time()
    popular_top = top_global(product_popularity(train), depth).cache()
    recs = {"popularity": recommend_popular(users, popular_top, depth).cache()}
    sims = item_knn.similarities(matrix, knn_params).cache()
    recs["item_knn"] = item_knn.recommend(interactions, sims, users, depth).cache()
    data["item_knn_users"] = recs["item_knn"].select("user_id").distinct().count()
    timings["item_knn"] = round(time.time() - t, 1)
    t = time.time()
    model = als.train(matrix, params)
    timings["als_train"] = round(time.time() - t, 1)
    t = time.time()
    recs["als"] = als.recommend(model, users, depth).cache()
    data["als_users"] = recs["als"].select("user_id").distinct().count()
    timings["als_recommend"] = round(time.time() - t, 1)

    t = time.time()
    frame = users.select("user_id", "segment", "train_products", "warm", "sampled").toPandas()
    has_list = {m: set(r["user_id"] for r in recs[m].select("user_id").distinct().collect())
                for m in ("item_knn", "als")}
    for m in has_list:
        frame[f"{m}:own_list"] = frame["user_id"].isin(has_list[m])
    per_variant, coverage = {}, {}
    for variant in VARIANTS:
        merged = None
        coverage[variant] = {}
        for model in MODELS:
            lists = final_lists(recs[model], recs["popularity"], seen if variant == "unseen" else None, k_max).cache()
            warm_lists = lists.join(warm.select("user_id"), "user_id", "left_semi")
            coverage[variant][model] = catalog_coverage(warm_lists, ks[0], data["als_matrix"]["products"])
            pdf = per_user_metrics(lists, truth[variant], ks).toPandas().drop(columns=["n_rel"])
            pdf = pdf.rename(columns={m: f"{model}:{m}" for m in metric_names})
            merged = pdf if merged is None else merged.merge(pdf, on="user_id")
            lists.unpersist()
        per_variant[variant] = frame.merge(merged, on="user_id")     # users with ground truth only
    timings["score"] = round(time.time() - t, 1)

    t = time.time()
    groups = {"warm": lambda f: f[f["warm"]], **{s: (lambda f, s=s: f[f["sampled"] & (f["segment"] == s)])
                                                  for s in SEGMENTS}}
    results = {}
    for name, pick in groups.items():
        g = pick(frame)
        results[name] = {
            "own_list_share": {m: round(float(g[f"{m}:own_list"].mean()), 4) if len(g) else None
                               for m in has_list},
            **{v: summarise(pick(per_variant[v]), metric_names, resamples, params.seed) for v in VARIANTS},
        }
    results["warm"]["coverage@%d" % ks[0]] = coverage
    timings["bootstrap"] = round(time.time() - t, 1)
    timings["total"] = round(time.time() - started, 1)

    return {
        "protocol": {
            "train": [str(TRAIN_START), str(TRAIN_END)],
            "test": [str(TEST_START), str(TEST_END)],
            "truth_events": list(truth_events),
            "ks": ks,
            "eval_users_requested": eval_users,
            "segment_users_requested": segment_users,
            "segments": {"new": "no training interactions", "light": f"1 to {LIGHT_MAX} training products",
                         "heavy": f"more than {LIGHT_MAX} training products"},
            "fallback": "lists completed with the global Popularity ranking",
            "unseen": "training products removed from the lists and from the ground truth",
            "bootstrap": {"resamples": resamples, "level": 0.95, "seed": params.seed},
        },
        "als_params": params.as_dict(),
        "knn_params": knn_params.as_dict(),
        "data": data,
        "results": results,
        "seconds": timings,
    }


def format_report(report: dict, metric: str = "ndcg@10") -> str:
    """One line per group, variant and model: mean [low, high] of `metric`."""
    lines = [f"| group | variant | users | " + " | ".join(MODELS) + " | als - popularity | als - item_knn |",
             "|---" * 8 + "|"]
    cell = lambda c: "-" if c["mean"] is None else f"{c['mean']:.4f} [{c['low']:.4f}, {c['high']:.4f}]"
    for group, res in report["results"].items():
        for v in VARIANTS:
            r = res[v]
            if not r["users"]:
                continue
            models = [cell(r["models"][m][metric]) for m in MODELS]
            diffs = [cell(r["differences"][d][metric]) for d in ("als - popularity", "als - item_knn")]
            lines.append(f"| {group} | {v} | {r['users']} | " + " | ".join(models + diffs) + " |")
    return "\n".join(lines)


def main() -> None:
    defaults, knn_defaults = als.ALSParams(), item_knn.KNNParams()
    p = argparse.ArgumentParser(description="Final evaluation: bootstrap intervals, user segments, unseen variant")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--report", help="write the results (JSON) to this path")
    p.add_argument("--eval-users", type=int, default=DEFAULT_EVAL_USERS, help="warm users, 0 = all")
    p.add_argument("--segment-users", type=int, default=DEFAULT_SEGMENT_USERS, help="users per segment, 0 = all")
    p.add_argument("--resamples", type=int, default=bootstrap.DEFAULT_RESAMPLES)
    p.add_argument("--truth-events", nargs="+", default=list(TRUTH_EVENTS), choices=TRUTH_EVENTS)
    p.add_argument("--rank", type=int, default=defaults.rank)
    p.add_argument("--reg", type=float, default=defaults.reg_param)
    p.add_argument("--alpha", type=float, default=defaults.alpha)
    p.add_argument("--iter", type=int, default=defaults.max_iter)
    p.add_argument("--seed", type=int, default=defaults.seed)
    p.add_argument("--checkpoint-interval", type=int, default=defaults.checkpoint_interval)
    args = p.parse_args()

    spark = get_spark("batch-final-eval", s3=not args.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    checkpoint_dir = tempfile.mkdtemp(prefix="als-checkpoint-")
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    train, test = load_split(spark, args.input)
    params = als.ALSParams(rank=args.rank, reg_param=args.reg, alpha=args.alpha, max_iter=args.iter,
                           seed=args.seed, checkpoint_interval=args.checkpoint_interval)
    report = run(train, test, params, args.eval_users, args.segment_users, tuple(args.truth_events),
                 resamples=args.resamples, knn_params=knn_defaults)
    report["input"] = args.input

    print(format_report(report))
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    spark.stop()
    shutil.rmtree(checkpoint_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
