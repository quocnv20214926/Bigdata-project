"""Ranking metrics for the offline evaluation (Pair 1, week 3).

Binary relevance: a recommended product is relevant if it is in the user's ground truth.
For one user with recommendations r_1..r_k (rank 1 = best) and ground truth T:

  hits@k      = |{r_1..r_k} & T|
  precision@k = hits@k / k
  recall@k    = hits@k / |T|
  hit_rate@k  = 1 if hits@k > 0 else 0
  ndcg@k      = sum over relevant r_i of 1 / log2(i + 1), divided by the same sum for an
                ideal list (min(|T|, k) relevant items at the top)

Each metric is averaged over the users in the ground truth; a user without
recommendations scores 0. The plain-Python functions define the metrics for one user and
can be reused by the speed-layer evaluation; ranking_metrics() computes the same values
in Spark for many users.
"""

from __future__ import annotations

import math
from typing import Collection, Iterable, Sequence

from pyspark.sql import DataFrame, functions as F


def _hits(recommended: Sequence[int], relevant: Collection[int], k: int) -> int:
    return sum(1 for p in recommended[:k] if p in relevant)


def precision_at_k(recommended: Sequence[int], relevant: Collection[int], k: int) -> float:
    return _hits(recommended, relevant, k) / k


def recall_at_k(recommended: Sequence[int], relevant: Collection[int], k: int) -> float:
    return _hits(recommended, relevant, k) / len(relevant) if relevant else 0.0


def hit_rate_at_k(recommended: Sequence[int], relevant: Collection[int], k: int) -> float:
    return 1.0 if _hits(recommended, relevant, k) else 0.0


def ideal_dcg(n_relevant: int, k: int) -> float:
    return sum(1.0 / math.log2(i + 1) for i in range(1, min(n_relevant, k) + 1))


def ndcg_at_k(recommended: Sequence[int], relevant: Collection[int], k: int) -> float:
    dcg = sum(1.0 / math.log2(i + 1) for i, p in enumerate(recommended[:k], start=1) if p in relevant)
    idcg = ideal_dcg(len(relevant), k)
    return dcg / idcg if idcg else 0.0


def ranking_metrics(recs: DataFrame, truth: DataFrame, ks: Iterable[int] = (10, 20)) -> dict:
    """Mean precision, recall, hit rate and NDCG at each k, over the users in `truth`.

    recs:  (user_id, product_id, rank), rank 1 = best, one row per (user, product).
    truth: (user_id, product_id), the relevant products of each evaluated user.
    """
    ks = sorted(set(ks))
    k_max = ks[-1]
    truth = truth.select("user_id", "product_id").distinct()
    n_relevant = truth.groupBy("user_id").agg(F.count("*").alias("n_rel"))
    hits = (recs.where(F.col("rank") <= k_max)
            .join(truth, ["user_id", "product_id"])
            .withColumn("gain", 1.0 / F.log2(F.col("rank") + 1)))

    sums = []
    for k in ks:
        in_k = F.col("rank") <= k
        sums += [F.sum(F.when(in_k, 1).otherwise(0)).alias(f"hits{k}"),
                 F.sum(F.when(in_k, F.col("gain")).otherwise(0.0)).alias(f"dcg{k}")]
    per_user = n_relevant.join(hits.groupBy("user_id").agg(*sums), "user_id", "left").fillna(0)

    means = [F.count("*").alias("users")]
    for k in ks:
        idcg = F.element_at(F.array(*[F.lit(ideal_dcg(n, k)) for n in range(1, k + 1)]),
                            F.least(F.col("n_rel"), F.lit(k)).cast("int"))
        means += [F.avg(F.col(f"hits{k}") / k).alias(f"precision@{k}"),
                  F.avg(F.col(f"hits{k}") / F.col("n_rel")).alias(f"recall@{k}"),
                  F.avg((F.col(f"hits{k}") > 0).cast("double")).alias(f"hit_rate@{k}"),
                  F.avg(F.col(f"dcg{k}") / idcg).alias(f"ndcg@{k}")]
    row = per_user.agg(*means).first().asDict()
    return {name: (round(v, 6) if isinstance(v, float) else v) for name, v in row.items()}


def catalog_coverage(recs: DataFrame, k: int, catalog_size: int) -> float:
    """Share of the catalog that appears in at least one top-k list."""
    n = recs.where(F.col("rank") <= k).select("product_id").distinct().count()
    return round(n / catalog_size, 6) if catalog_size else 0.0
