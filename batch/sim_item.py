"""The most similar products by cosine between ALS item factors (Pair 1, week 4).

This was the week 4 candidate for sim:item. Item-kNN (batch/item_knn.py) won the comparison
(docs/week-04.md), so the nightly job loads that table and uses this one only with
`--sim-source als` (similarity ablation).

  sim(i, j) = f_i . f_j / (||f_i|| * ||f_j||)    for the item factors f of batch/als.py

Each product keeps its `n` most similar products (common.config sim_top_n = 50) with sim > 0:
the speed layer adds w * sim(i, j) * decay to rt[j], so a negative cosine would lower a score.

The result is exact, not approximate. With about 155,000 products there are 12 billion pairs,
far too many to join as DataFrame rows, so the normalised factors (products x rank floats, about
80 MB at rank 128) are broadcast to every task and each task scores one block of `block_size`
products against all of them with a single matrix product (numpy), keeping the top n of each row.
Memory per task is about block_size x products x 12 bytes (the scores and their argpartition).

Not to be confused with Item-kNN (batch/item_knn.py: cosine between product vectors over users)
or Pair 2's sim:cooc (co-occurrence in a session).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, functions as F

from common.config import settings

SCHEMA = "product_id long, neighbour_id long, sim float, rank int"
DEFAULT_BLOCK_SIZE = 128


def normalised_factors(factors: DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """(ids, unit vectors) sorted by id, from ALS item factors (id, features).

    A zero vector stays zero: its cosine with everything is 0, so it gets no neighbours.
    """
    pdf = factors.select("id", "features").toPandas().sort_values("id")
    ids = pdf["id"].to_numpy(dtype=np.int64)
    m = np.array(pdf["features"].tolist(), dtype=np.float32).reshape(len(ids), -1)
    norms = np.linalg.norm(m, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return ids, m / norms


def block_top_n(ids: np.ndarray, unit: np.ndarray, lo: int, hi: int, n: int,
                min_sim: float = 0.0) -> pd.DataFrame:
    """Top-n neighbours of rows lo..hi-1 among all rows, rank 1 = most similar.

    Ties are broken by the smaller neighbour id; pairs with sim <= min_sim are dropped.
    """
    k = min(n, len(ids) - 1)
    if k <= 0 or lo >= hi:
        return pd.DataFrame({"product_id": pd.Series(dtype="int64"), "neighbour_id": pd.Series(dtype="int64"),
                             "sim": pd.Series(dtype="float32"), "rank": pd.Series(dtype="int32")})
    sims = unit[lo:hi] @ unit.T
    rows = np.arange(hi - lo)
    sims[rows, lo + rows] = -np.inf                       # a product is not its own neighbour
    part = np.argpartition(sims, -k, axis=1)[:, -k:]      # the k largest, in no order
    top = np.take_along_axis(sims, part, axis=1)
    order = np.lexsort((ids[part], -top), axis=1)        # sim descending, then neighbour id
    part = np.take_along_axis(part, order, axis=1)
    top = np.take_along_axis(top, order, axis=1)
    keep = top > min_sim
    rank = np.broadcast_to(np.arange(1, k + 1, dtype=np.int32), top.shape)
    return pd.DataFrame({
        "product_id": np.repeat(ids[lo:hi], k)[keep.ravel()],
        "neighbour_id": ids[part][keep],
        "sim": top[keep].astype(np.float32),
        "rank": rank[keep],
    })


def similar_items(factors: DataFrame, n: int = settings.sim_top_n, block_size: int = DEFAULT_BLOCK_SIZE,
                  min_sim: float = 0.0) -> DataFrame:
    """(product_id, neighbour_id, sim, rank) for every product of the ALS item factors (id, features)."""
    spark = factors.sparkSession
    ids, unit = normalised_factors(factors)
    sc = spark.sparkContext
    bc_ids, bc_unit = sc.broadcast(ids), sc.broadcast(unit)
    n_blocks = max(1, math.ceil(len(ids) / block_size))

    def top_n(batches):
        all_ids, all_unit = bc_ids.value, bc_unit.value
        for pdf in batches:
            for b in pdf["block"]:
                lo = int(b) * block_size
                yield block_top_n(all_ids, all_unit, lo, min(lo + block_size, len(all_ids)), n, min_sim)

    # One row per block, spread over the cores so the blocks are scored in parallel.
    parts = min(n_blocks, sc.defaultParallelism * 4)
    blocks = spark.range(n_blocks).select(F.col("id").alias("block")).repartition(parts)
    return blocks.mapInPandas(top_n, SCHEMA)


def from_model(model, n: int = settings.sim_top_n, block_size: int = DEFAULT_BLOCK_SIZE) -> DataFrame:
    """sim:item rows from a fitted or loaded ALSModel."""
    return similar_items(model.itemFactors, n, block_size)
