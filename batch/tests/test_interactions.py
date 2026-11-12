"""Interaction matrix, popularity and ALS on tiny data (local Spark).

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

import math

import pytest

from batch import als
from batch.interactions import filter_rare, interaction_scores, matrix_stats
from batch.popularity import product_popularity, recommend_popular, top_by_category, top_global

COLUMNS = ["user_id", "product_id", "category_id", "event_type"]


def events(spark, rows):
    return spark.createDataFrame(rows, COLUMNS)


def as_dict(df, key_cols, value_col):
    return {tuple(r[c] for c in key_cols): r[value_col] for r in df.collect()}


def test_scores_sum_event_weights(spark):
    df = events(spark, [
        (1, 10, 7, "view"), (1, 10, 7, "view"), (1, 10, 7, "cart"), (1, 10, 7, "purchase"),
        (1, 11, 7, "remove_from_cart"),   # weight 0: no interaction
        (1, 12, 7, "click"),              # unknown type: weight 0
        (2, 10, 7, "purchase"),           # no cart before the purchase
    ])
    scores = as_dict(interaction_scores(df), ["user_id", "product_id"], "score")
    assert scores == {(1, 10): 10.0, (2, 10): 5.0}


def test_filter_rare_products_then_users(spark):
    rows = [(u, 100, 1, "view") for u in range(1, 6)]           # product 100: 5 users, kept
    rows += [(u, 101, 1, "view") for u in range(1, 3)]          # product 101: 2 users, dropped
    rows += [(6, 100, 1, "view"), (6, 101, 1, "view")]          # user 6: 2 products, then only 100
    rows += [(1, 102, 1, "view")] + [(u, 102, 1, "view") for u in range(2, 6)]  # product 102: 5 users
    inter = interaction_scores(events(spark, rows))
    kept = filter_rare(inter, min_users_per_product=5, min_products_per_user=2)
    pairs = {(r.user_id, r.product_id) for r in kept.collect()}
    # product 100 now has 6 users; user 6 keeps a single product and is dropped
    assert {p for _, p in pairs} == {100, 102}
    assert {u for u, _ in pairs} == {1, 2, 3, 4, 5}
    stats = matrix_stats(kept)
    assert stats["pairs"] == 10 and stats["users"] == 5 and stats["products"] == 2
    assert stats["density"] == pytest.approx(1.0)


def test_popularity_lists(spark):
    df = events(spark, [
        (1, 10, 7, "purchase"), (2, 10, 7, "view"),     # 10: 6
        (1, 11, 7, "cart"), (2, 11, 7, "cart"),         # 11: 6, tie with 10 -> lower id first
        (3, 20, 8, "view"),                             # 20: 1
        (3, 21, None, "purchase"),                      # 21: 5, no category
    ])
    pop = product_popularity(df)
    top = top_global(pop, 3)
    assert [(r.product_id, r.rank) for r in top.orderBy("rank").collect()] == [(10, 1), (11, 2), (21, 3)]

    by_cat = as_dict(top_by_category(pop, 1), ["category_id", "rank"], "product_id")
    assert by_cat == {(7, 1): 10, (8, 1): 20}

    users = spark.createDataFrame([(1,), (2,), (2,)], ["user_id"])
    recs = recommend_popular(users, top, k=2)
    assert sorted((r.user_id, r.product_id, r.rank) for r in recs.collect()) == [
        (1, 10, 1), (1, 11, 2), (2, 10, 1), (2, 11, 2)]


def two_communities(spark):
    """Users 1-20 view products 100-104, users 21-40 view products 200-204 (each skips one)."""
    rows = []
    for u in range(1, 41):
        base = 100 if u <= 20 else 200
        rows += [(u, base + i, 1, "view") for i in range(5) if i != u % 5]
    return interaction_scores(events(spark, rows))


def test_ratings_match_shared_confidence(spark):
    from common.weights import als_confidence

    inter = interaction_scores(events(spark, [(1, 10, 7, "cart"), (1, 10, 7, "purchase")]))
    rating = als.to_ratings(inter).first()["rating"]
    alpha = als.ALSParams().alpha
    # Spark implicit ALS: confidence = 1 + alpha * rating
    assert 1 + alpha * rating == pytest.approx(als_confidence(8.0, alpha))
    assert rating == pytest.approx(math.log1p(8.0))


def test_als_recommends_within_community(spark):
    inter = two_communities(spark)
    model = als.train(inter, als.ALSParams(rank=4, reg_param=0.01, max_iter=10))
    users = spark.createDataFrame([(1,), (25,), (999,)], ["user_id"])   # 999 is unknown
    recs = als.recommend(model, users, k=4).collect()

    assert {r.user_id for r in recs} == {1, 25}
    for user in (1, 25):
        mine = sorted((r.rank, r.product_id) for r in recs if r.user_id == user)
        assert [rank for rank, _ in mine] == [1, 2, 3, 4]
        base = 100 if user <= 20 else 200
        in_community = sum(base <= p < base + 5 for _, p in mine)
        assert in_community >= 3
