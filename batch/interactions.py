"""User-product interaction matrix for ALS and the baselines (Pair 1, week 3).

score(user, product) = sum of the event weights in common/weights.py (view 1, cart 3,
purchase 5) over the training events. batch/als.py turns it into the ALS confidence
1 + alpha * log(1 + score) (common.weights.als_confidence).

Rare users and products are removed before ALS (thresholds from notebooks/01_eda.ipynb,
section 7): a product needs at least 5 distinct users, then a user needs at least 2 of
the remaining products. Users filtered out here are served by the popularity fallback.
"""

from __future__ import annotations

from itertools import chain

from pyspark.sql import Column, DataFrame, functions as F

from common.weights import EVENT_WEIGHTS

MIN_USERS_PER_PRODUCT = 5
MIN_PRODUCTS_PER_USER = 2


def event_weight(event_type: Column) -> Column:
    """Spark version of common.weights.event_weight: unknown event types weigh 0."""
    weights = F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in EVENT_WEIGHTS.items()))
    return F.coalesce(weights[event_type], F.lit(0.0))


def interaction_scores(events: DataFrame) -> DataFrame:
    """(user_id, product_id, score): one row per pair with a positive summed weight."""
    return (
        events.withColumn("weight", event_weight(F.col("event_type")))
        .where(F.col("weight") > 0)
        .groupBy("user_id", "product_id")
        .agg(F.sum("weight").alias("score"))
    )


def filter_rare(interactions: DataFrame, min_users_per_product: int = MIN_USERS_PER_PRODUCT,
                min_products_per_user: int = MIN_PRODUCTS_PER_USER) -> DataFrame:
    """Single pass: drop rare products first, then users left with too few products."""
    products = (interactions.groupBy("product_id").count()
                .where(F.col("count") >= min_users_per_product).select("product_id"))
    kept = interactions.join(products, "product_id", "left_semi")
    users = (kept.groupBy("user_id").count()
             .where(F.col("count") >= min_products_per_user).select("user_id"))
    return kept.join(users, "user_id", "left_semi")


def matrix_stats(interactions: DataFrame) -> dict:
    s = interactions.agg(
        F.count("*").alias("pairs"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("product_id").alias("products"),
    ).first().asDict()
    cells = s["users"] * s["products"]
    s["density"] = round(s["pairs"] / cells, 8) if cells else 0.0
    return s
