"""sim:item (cosine of ALS item factors) vs the Item-kNN similarity table (Pair 1, week 4).

Both tables are built from the same filtered training matrix and keep 50 neighbours per
product. They are compared on:
  - size: rows, products with neighbours, mean neighbours per product, mean similarity
  - overlap@k: for a product in both tables, |top-k(a) & top-k(b)| / min(|top-k(a)|, |top-k(b)|)
  - same_category@k: share of (product, neighbour) pairs in the top k with the same category_id
  - next_item: test sessions cut into transitions (a product, then the next different product the
    same session touched); hit@k = share of transitions whose next product is among the top-k
    neighbours of the previous one. This is how the speed layer uses a similarity table.
  - item_based: each table used as an item-based recommender (batch/item_knn.recommend) on the
    users and ground truth of evaluation/offline.py, with the same metrics as week 3.

Usage (from the repository root, MinIO running):
  SPARK_DRIVER_MEMORY=14g python -m evaluation.similarity --model s3a://lake/models/als/week4/ \\
      --sim-out s3a://lake/models/sim_item/week4/ --report docs/eval-week04-sim.json
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, Window, functions as F

from batch import item_knn, sim_item
from batch.popularity import product_popularity
from batch.spark_session import get_spark
from batch.split import load_split
from common.config import settings
from evaluation import offline

DEFAULT_KS = (10, 50)
DEFAULT_TRANSITIONS = 1_000_000


def with_rank(sims: DataFrame) -> DataFrame:
    """Add rank (1 = most similar, ties by neighbour id) when the table has none."""
    if "rank" in sims.columns:
        return sims
    order = Window.partitionBy("product_id").orderBy(F.col("sim").desc(), F.col("neighbour_id"))
    return sims.withColumn("rank", F.row_number().over(order))


def table_stats(sims: DataFrame) -> dict:
    s = sims.agg(F.count("*").alias("rows"), F.countDistinct("product_id").alias("products"),
                 F.avg("sim").alias("mean_sim")).first().asDict()
    s["neighbours_per_product"] = round(s["rows"] / s["products"], 2) if s["products"] else 0.0
    s["mean_sim"] = round(s["mean_sim"] or 0.0, 6)
    return s


def overlap(a: DataFrame, b: DataFrame, ks=DEFAULT_KS) -> dict:
    """Mean overlap of the top-k neighbour sets of the products present in both tables."""
    a, b = with_rank(a), with_rank(b)
    result = {"products_in_both": a.select("product_id").distinct()
              .join(b.select("product_id").distinct(), "product_id").count()}
    for k in ks:
        ta = a.where(F.col("rank") <= k).groupBy("product_id").agg(F.count("*").alias("na"))
        tb = b.where(F.col("rank") <= k).groupBy("product_id").agg(F.count("*").alias("nb"))
        common = (a.where(F.col("rank") <= k).select("product_id", "neighbour_id")
                  .join(b.where(F.col("rank") <= k).select("product_id", "neighbour_id"), ["product_id", "neighbour_id"])
                  .groupBy("product_id").agg(F.count("*").alias("common")))
        per = (ta.join(tb, "product_id").join(common, "product_id", "left").fillna(0, ["common"])
               .select((F.col("common") / F.least("na", "nb")).alias("o")))
        result[f"overlap@{k}"] = round(per.agg(F.avg("o")).first()[0] or 0.0, 6)
    return result


def same_category(sims: DataFrame, products: DataFrame, ks=DEFAULT_KS) -> dict:
    """Share of top-k (product, neighbour) pairs whose two products have the same category_id."""
    cat = products.select("product_id", "category_id").where(F.col("category_id").isNotNull())
    pairs = (with_rank(sims)
             .join(cat, "product_id")
             .join(cat.select(F.col("product_id").alias("neighbour_id"), F.col("category_id").alias("n_cat")),
                   "neighbour_id"))
    row = pairs.agg(*[F.avg(F.when(F.col("rank") <= k, (F.col("category_id") == F.col("n_cat")).cast("double")))
                      .alias(f"same_category@{k}") for k in ks]).first().asDict()
    return {name: round(v or 0.0, 6) for name, v in row.items()}


def transitions(events: DataFrame, catalog: DataFrame, n: int = DEFAULT_TRANSITIONS, seed: int = 42) -> DataFrame:
    """(prev_id, next_id): consecutive different products within a session, the previous one in `catalog`.

    A deterministic sample of n transitions (0 = all), so every table is scored on the same ones.
    """
    order = Window.partitionBy("user_session").orderBy("event_time", "event_type", "product_id")
    t = (events.where(F.col("user_session").isNotNull())
         .select("user_session", "event_time", "event_type", "product_id")
         .withColumn("prev_id", F.lag("product_id").over(order))
         .where(F.col("prev_id").isNotNull() & (F.col("prev_id") != F.col("product_id")))
         .join(catalog.select(F.col("product_id").alias("prev_id")).distinct(), "prev_id", "left_semi"))
    if n:
        t = t.orderBy(F.xxhash64("user_session", "event_time", "product_id", F.lit(seed))).limit(n)
    return t.select("prev_id", F.col("product_id").alias("next_id"))


def next_item(sims: DataFrame, trans: DataFrame, ks=DEFAULT_KS) -> dict:
    """hit@k of the next product among the previous product's top-k neighbours; covered = has neighbours."""
    ranked = with_rank(sims).select(F.col("product_id").alias("prev_id"), F.col("neighbour_id").alias("next_id"), "rank")
    has = sims.select(F.col("product_id").alias("prev_id")).distinct().withColumn("has", F.lit(1))
    scored = trans.join(ranked, ["prev_id", "next_id"], "left").join(has, "prev_id", "left")
    row = scored.agg(F.count("*").alias("transitions"),
                     F.avg(F.coalesce("has", F.lit(0)).cast("double")).alias("covered"),
                     *[F.avg((F.coalesce("rank", F.lit(10 ** 9)) <= k).cast("double")).alias(f"hit@{k}")
                       for k in ks]).first().asDict()
    return {name: (round(v, 6) if isinstance(v, float) else v) for name, v in row.items()}


def compare(tables: dict[str, DataFrame], products: DataFrame, trans: DataFrame | None = None,
            prepared: offline.Prepared | None = None, ks=DEFAULT_KS, rec_ks=offline.DEFAULT_KS) -> dict:
    """All comparisons for {name: similarity table}; the first two tables are also overlapped."""
    timings, report = {}, {"tables": {}, "results": {}}
    for name, sims in tables.items():
        t = time.time()
        report["tables"][name] = table_stats(sims)
        res = same_category(sims, products, ks)
        if trans is not None:
            res.update({f"next_item_{k}": v for k, v in next_item(sims, trans, ks).items()})
        if prepared is not None:
            recs = item_knn.recommend(prepared.matrix, sims.select("product_id", "neighbour_id", "sim"),
                                      prepared.users, max(rec_ks)).cache()
            res["item_based"] = offline.score(recs, prepared.truth, rec_ks, prepared.data["als_matrix"]["products"])
            recs.unpersist()
        report["results"][name] = res
        timings[name] = round(time.time() - t, 1)
    names = list(tables)
    if len(names) >= 2:
        report["overlap"] = {"tables": names[:2], **overlap(tables[names[0]], tables[names[1]], ks)}
    report["seconds"] = timings
    return report


def format_table(report: dict) -> str:
    names = list(report["results"])
    rows = [("products with neighbours", lambda n: report["tables"][n]["products"], "{:,}"),
            ("mean similarity", lambda n: report["tables"][n]["mean_sim"], "{:.4f}")]
    first = report["results"][names[0]]
    for key in first:
        if key.startswith(("same_category", "next_item_hit", "next_item_covered")):
            rows.append((key, lambda n, k=key: report["results"][n][k], "{:.4f}"))
    if "item_based" in first:
        for key in ("ndcg@10", "recall@20", "coverage@10"):
            if key in first["item_based"]:
                rows.append((f"item-based {key}", lambda n, k=key: report["results"][n]["item_based"][k], "{:.4f}"))
    lines = ["| | " + " | ".join(names) + " |", "|---" * (len(names) + 1) + "|"]
    lines += [f"| {label} | " + " | ".join(fmt.format(get(n)) for n in names) + " |" for label, get, fmt in rows]
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description="sim:item (ALS item factors) vs Item-kNN similarities")
    p.add_argument("--input", default=settings.clean_path, help="clean Parquet from batch/etl.py")
    p.add_argument("--no-s3", action="store_true", help="local paths only, do not load the s3a jars")
    p.add_argument("--model", required=True, help="ALS model trained on the train split (evaluation.tune_als --model-out)")
    p.add_argument("--sim-out", help="also save the sim:item table (Parquet) here")
    p.add_argument("--report", help="write the comparison (JSON) to this path")
    p.add_argument("--block-size", type=int, default=sim_item.DEFAULT_BLOCK_SIZE)
    p.add_argument("--transitions", type=int, default=DEFAULT_TRANSITIONS, help="sampled test transitions, 0 = all")
    p.add_argument("--eval-users", type=int, default=offline.DEFAULT_EVAL_USERS,
                   help="users for the item-based comparison, 0 = skip it")
    args = p.parse_args()

    spark = get_spark("batch-sim-item", s3=not args.no_s3)
    spark.sparkContext.setLogLevel("WARN")
    started, timings = time.time(), {}
    train, test = load_split(spark, args.input)
    prepared = offline.prepare(train, test, args.eval_users or 1)
    timings["prepare"] = round(time.time() - started, 1)

    t = time.time()
    model = ALSModel.load(args.model)
    als_sims = sim_item.from_model(model, block_size=args.block_size).cache()
    als_sims.count()
    timings["sim_item"] = round(time.time() - t, 1)
    if args.sim_out:
        als_sims.write.mode("overwrite").parquet(args.sim_out)

    t = time.time()
    knn_sims = item_knn.similarities(prepared.matrix).cache()
    knn_sims.count()
    timings["item_knn"] = round(time.time() - t, 1)

    products = product_popularity(train).select("product_id", "category_id").cache()
    catalog = prepared.matrix.select("product_id").distinct()
    trans = transitions(test, catalog, args.transitions).cache()
    report = compare({"sim_item": als_sims, "item_knn": knn_sims}, products, trans,
                     prepared if args.eval_users else None)
    report["seconds"].update(timings)
    report["seconds"]["total"] = round(time.time() - started, 1)
    report["data"] = {**prepared.data, "transitions": trans.count()}
    report["params"] = {"model": args.model, "rank": model.rank, "neighbours": settings.sim_top_n,
                        "block_size": args.block_size, "knn": item_knn.KNNParams().as_dict()}
    report["input"] = args.input

    print(json.dumps(report, indent=2))
    print(format_table(report))
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
