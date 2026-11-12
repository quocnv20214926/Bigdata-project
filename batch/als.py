"""Implicit ALS on the interaction matrix (Pair 1, week 3).

Spark's implicit ALS uses the confidence c = 1 + alpha * r. Training on
r = log(1 + score) therefore gives exactly common.weights.als_confidence(score, alpha),
the formula in docs/plan.md section 3.3.

The default parameters are the best of the week 4 search on the validation week
(evaluation/tune_als.py, docs/eval-week04-tuning.json): rank 128, regParam 0.1, alpha 5.

REES46 user_id and product_id fit in a 32-bit int, which Spark ALS requires;
it raises an error if an id does not.

recommend() scores users against all products with numpy instead of Spark's
recommendForUserSubset (week 6): the item factors (products x rank floats, about 80 MB at rank
128) are broadcast and each task multiplies a block of users by them, keeping the top k of each
row, the same way batch/sim_item.py scores products. Same lists, several times faster on the
nightly's 0.5 to 1 million users (docs/week-06.md).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, SparkSession, functions as F

REC_SCHEMA = "user_id int, product_id int, rank int, score float"
# Users scored per matrix product: the scores and their argpartition take about
# block x products x 12 bytes (240 MB for 128 users and 155,000 products) in each Python worker.
DEFAULT_USER_BLOCK = 128


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
    # Spark ALS splits users and products into this many blocks each (Spark's default is 10).
    blocks: int = 10

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
        numUserBlocks=params.blocks,
        numItemBlocks=params.blocks,
    )
    return als.fit(to_ratings(interactions))


def factor_matrix(factors: DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """(ids, vectors) sorted by id, from ALS factors (id, features)."""
    pdf = factors.select("id", "features").toPandas().sort_values("id")
    ids = pdf["id"].to_numpy(dtype=np.int64)
    return ids, np.array(pdf["features"].tolist(), dtype=np.float32).reshape(len(ids), -1)


def block_top_k(user_ids: np.ndarray, user_vectors: np.ndarray, item_ids: np.ndarray, items: np.ndarray,
                k: int) -> pd.DataFrame:
    """Top-k products by dot product for each user, rank 1 = best; ties go to the smaller product id."""
    k = min(k, len(item_ids))
    if k <= 0 or not len(user_ids):
        return pd.DataFrame({"user_id": pd.Series(dtype="int32"), "product_id": pd.Series(dtype="int32"),
                             "rank": pd.Series(dtype="int32"), "score": pd.Series(dtype="float32")})
    scores = user_vectors @ items.T
    part = np.argpartition(scores, -k, axis=1)[:, -k:]      # the k largest, in no order
    top = np.take_along_axis(scores, part, axis=1)
    order = np.lexsort((item_ids[part], -top), axis=1)      # score descending, then product id
    part = np.take_along_axis(part, order, axis=1)
    return pd.DataFrame({
        "user_id": np.repeat(user_ids, k).astype(np.int32),
        "product_id": item_ids[part].ravel().astype(np.int32),
        "rank": np.tile(np.arange(1, k + 1, dtype=np.int32), len(user_ids)),
        "score": np.take_along_axis(top, order, axis=1).ravel().astype(np.float32),
    })


def recommend(model: ALSModel, users: DataFrame, k: int, block_size: int = DEFAULT_USER_BLOCK) -> DataFrame:
    """(user_id, product_id, rank, score) for the users the model knows; rank 1 = best.

    score is the ALS prediction (user factor . item factor), as in recommendForUserSubset.
    Products the user already interacted with are not removed (see evaluation/README.md).
    """
    sc = users.sparkSession.sparkContext
    item_ids, items = factor_matrix(model.itemFactors)
    bc_ids, bc_items = sc.broadcast(item_ids), sc.broadcast(items)
    wanted = users.select(F.col("user_id").alias("id")).distinct()

    def top_k(batches):
        all_ids, all_items = bc_ids.value, bc_items.value
        for pdf in batches:
            for lo in range(0, len(pdf), block_size):
                part = pdf.iloc[lo:lo + block_size]
                vectors = np.array(part["features"].tolist(), dtype=np.float32).reshape(len(part), -1)
                yield block_top_k(part["id"].to_numpy(dtype=np.int64), vectors, all_ids, all_items, k)

    return model.userFactors.join(wanted, "id", "left_semi").mapInPandas(top_k, REC_SCHEMA)


def recommend_builtin(model: ALSModel, users: DataFrame, k: int) -> DataFrame:
    """recommend() with Spark's recommendForUserSubset (weeks 3 to 5), kept to check and time the new one."""
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
