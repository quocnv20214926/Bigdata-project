"""Implicit ALS on the interaction matrix (Pair 1, week 3).

Spark's implicit ALS uses the confidence c = 1 + alpha * r. Training on
r = log(1 + score) therefore gives exactly common.weights.als_confidence(score, alpha),
the formula in docs/plan.md section 3.3.

REES46 user_id and product_id fit in a 32-bit int, which Spark ALS requires;
it raises an error if an id does not.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, functions as F

from common.weights import DEFAULT_ALS_ALPHA


@dataclass(frozen=True)
class ALSParams:
    rank: int = 64
    reg_param: float = 0.1
    alpha: float = DEFAULT_ALS_ALPHA
    max_iter: int = 10
    seed: int = 42
    # With a checkpoint dir set, factors are saved every N iterations; this cuts the lineage
    # so Spark can delete the shuffle files of earlier iterations (they filled a laptop disk).
    checkpoint_interval: int = 2

    def as_dict(self) -> dict:
        return asdict(self)


def to_ratings(interactions: DataFrame) -> DataFrame:
    """(user_id, product_id, rating = log(1 + score)) for ALS."""
    return interactions.select("user_id", "product_id", F.log1p("score").alias("rating"))


def train(interactions: DataFrame, params: ALSParams = ALSParams()) -> ALSModel:
    als = ALS(
        rank=params.rank,
        maxIter=params.max_iter,
        regParam=params.reg_param,
        alpha=params.alpha,
        implicitPrefs=True,
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        seed=params.seed,
        checkpointInterval=params.checkpoint_interval,
    )
    return als.fit(to_ratings(interactions))


def recommend(model: ALSModel, users: DataFrame, k: int) -> DataFrame:
    """(user_id, product_id, rank, score) for the users the model knows; rank 1 = best.

    Products the user already interacted with are not removed (see evaluation/README.md).
    """
    recs = model.recommendForUserSubset(users.select("user_id").distinct(), k)
    return (recs.select("user_id", F.posexplode("recommendations").alias("pos", "rec"))
            .select("user_id",
                    F.col("rec.product_id").alias("product_id"),
                    (F.col("pos") + 1).alias("rank"),
                    F.col("rec.rating").alias("score")))
