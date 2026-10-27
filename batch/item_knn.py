"""Item-kNN baseline: item-item cosine similarity on the interaction matrix (Pair 1, week 3).

Each product is a vector over users with r(u, i) = log(1 + score), the same transform as the
ALS ratings (batch/als.py). For two products i and j, using the training interactions only:

  sim(i, j) = sum over users of r(u, i) * r(u, j) / (||r_i|| * ||r_j||)

Each product keeps its `neighbours` most similar products that share at least
`min_common_users` users with it. A user's score for product j is the sum of
r(u, i) * sim(i, j) over the products i the user interacted with; the top k form the list.

Cost: the number of product pairs grows with the square of the products per user, so only
each user's `max_items_per_user` highest-scored products enter the similarity (products per
user in October: median 3, p99 69; notebooks/01_eda.ipynb, section 6). Scoring uses the full
history of the evaluated users.

Not to be confused with sim:item (week 4: cosine between ALS item factors) or with Pair 2's
sim:cooc (co-occurrence within a session).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, Window, functions as F

from common.config import settings


@dataclass(frozen=True)
class KNNParams:
    neighbours: int = settings.sim_top_n
    max_items_per_user: int = 50
    min_common_users: int = 2

    def as_dict(self) -> dict:
        return asdict(self)


def to_weights(interactions: DataFrame) -> DataFrame:
    """(user_id, product_id, r = log(1 + score)), as in als.to_ratings."""
    return interactions.select("user_id", "product_id", F.log1p("score").alias("r"))


def cap_per_user(weights: DataFrame, max_items: int) -> DataFrame:
    """Each user's max_items highest-weighted products; ties broken by product_id."""
    order = Window.partitionBy("user_id").orderBy(F.col("r").desc(), F.col("product_id"))
    return (weights.withColumn("pos", F.row_number().over(order))
            .where(F.col("pos") <= max_items).drop("pos"))


def similarities(interactions: DataFrame, params: KNNParams = KNNParams()) -> DataFrame:
    """(product_id, neighbour_id, sim): the top `neighbours` products for each product."""
    w = cap_per_user(to_weights(interactions), params.max_items_per_user)
    norms = w.groupBy("product_id").agg(F.sqrt(F.sum(F.col("r") * F.col("r"))).alias("norm"))

    left = w.select("user_id", F.col("product_id").alias("i"), F.col("r").alias("ri"))
    right = w.select("user_id", F.col("product_id").alias("j"), F.col("r").alias("rj"))
    pairs = (left.join(right, "user_id").where(F.col("i") < F.col("j"))
             .groupBy("i", "j")
             .agg(F.sum(F.col("ri") * F.col("rj")).alias("dot"), F.count("*").alias("common"))
             .where(F.col("common") >= params.min_common_users))

    norm_i = F.broadcast(norms.select(F.col("product_id").alias("i"), F.col("norm").alias("norm_i")))
    norm_j = F.broadcast(norms.select(F.col("product_id").alias("j"), F.col("norm").alias("norm_j")))
    half = (pairs.join(norm_i, "i").join(norm_j, "j")
            .select("i", "j", (F.col("dot") / (F.col("norm_i") * F.col("norm_j"))).alias("sim")))
    both = half.unionByName(half.select(F.col("j").alias("i"), F.col("i").alias("j"), "sim"))

    order = Window.partitionBy("i").orderBy(F.col("sim").desc(), F.col("j"))
    return (both.withColumn("pos", F.row_number().over(order))
            .where(F.col("pos") <= params.neighbours)
            .select(F.col("i").alias("product_id"), F.col("j").alias("neighbour_id"), "sim"))


def recommend(interactions: DataFrame, sims: DataFrame, users: DataFrame, k: int) -> DataFrame:
    """(user_id, product_id, rank, score) for the given users; rank 1 = best.

    Like ALS, products the user already interacted with are not removed (evaluation/README.md).
    Users whose products have no neighbours get no recommendations.
    """
    history = to_weights(interactions).join(users.select("user_id").distinct(), "user_id", "left_semi")
    scores = (history.join(sims, "product_id")
              .select("user_id", F.col("neighbour_id").alias("product_id"),
                      (F.col("r") * F.col("sim")).alias("part"))
              .groupBy("user_id", "product_id").agg(F.sum("part").alias("score")))
    order = Window.partitionBy("user_id").orderBy(F.col("score").desc(), F.col("product_id"))
    return (scores.withColumn("rank", F.row_number().over(order))
            .where(F.col("rank") <= k)
            .select("user_id", "product_id", "rank", "score"))
