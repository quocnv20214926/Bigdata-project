"""Implicit ALS on the interaction matrix (Pair 1, week 3).

Spark's implicit ALS uses the confidence c = 1 + alpha * r. Training on
r = log(1 + score) therefore gives exactly common.weights.als_confidence(score, alpha),
the formula in docs/plan.md section 3.3.

The default parameters are the best of the week 4 search on the validation week
(evaluation/tune_als.py, docs/eval-week04-tuning.json): rank 128, regParam 0.1, alpha 5.

REES46 user_id and product_id fit in a 32-bit int, which Spark ALS requires;
it raises an error if an id does not.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, SparkSession, functions as F



@dataclass(frozen=True)
class ALSParams:
    rank: int = 128
    reg_param: float = 0.1
    alpha: float = 5.0      # week 3 used common.weights.DEFAULT_ALS_ALPHA (10)
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


def release(spark: SparkSession) -> None:
    """Unpersist the factor RDDs that fit() leaves cached ("userFactors", "itemFactors").

    Spark frees them only once the model is garbage-collected; when many models are trained in
    one session (evaluation/tune_als.py) they would otherwise pile up in memory and on disk.
    """
    for rdd in spark.sparkContext._jsc.getPersistentRDDs().values():
        if rdd.name() in ("userFactors", "itemFactors"):
            rdd.unpersist(False)
