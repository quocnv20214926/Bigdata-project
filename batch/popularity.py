"""Long-term popularity from the training events (Pair 1, week 3).

The same scores serve two purposes:
  - the Popularity baseline in the offline evaluation (every user gets the global top-k);
  - the pop:global and pop:cat:{category_id} lists written by the Redis loader (week 5).

popularity(product) = sum of event weights (common/weights.py) over all users.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from batch.interactions import event_weight


def product_popularity(events: DataFrame) -> DataFrame:
    """(product_id, category_id, score). A product keeps one category_id (the largest seen)."""
    return (
        events.withColumn("weight", event_weight(F.col("event_type")))
        .where(F.col("weight") > 0)
        .groupBy("product_id")
        .agg(F.max("category_id").alias("category_id"), F.sum("weight").alias("score"))
    )


def top_global(popularity: DataFrame, n: int) -> DataFrame:
    """(product_id, score, rank) for the n most popular products; ties broken by product_id."""
    order = Window.orderBy(F.col("score").desc(), F.col("product_id"))
    return (popularity.orderBy(F.col("score").desc(), F.col("product_id")).limit(n)
            .withColumn("rank", F.row_number().over(order))
            .select("product_id", "score", "rank"))


def top_by_category(popularity: DataFrame, n: int) -> DataFrame:
    """(category_id, product_id, score, rank) for the n most popular products of each category."""
    order = Window.partitionBy("category_id").orderBy(F.col("score").desc(), F.col("product_id"))
    return (popularity.where(F.col("category_id").isNotNull())
            .withColumn("rank", F.row_number().over(order))
            .where(F.col("rank") <= n)
            .select("category_id", "product_id", "score", "rank"))


def recommend_popular(users: DataFrame, top: DataFrame, k: int) -> DataFrame:
    """Popularity baseline: (user_id, product_id, rank, score), the same top-k for every user."""
    return (users.select("user_id").distinct()
            .crossJoin(F.broadcast(top.where(F.col("rank") <= k)))
            .select("user_id", "product_id", "rank", "score"))
