"""Item-kNN baseline on tiny data (local Spark).

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

import math
import random

import pytest

from batch import item_knn
from batch.item_knn import KNNParams

COLUMNS = ["user_id", "product_id", "score"]


def interactions(spark, rows):
    return spark.createDataFrame([(u, p, float(s)) for u, p, s in rows], COLUMNS)


def sims_dict(df):
    return {(r.product_id, r.neighbour_id): r.sim for r in df.collect()}


def python_cosine(rows, min_common):
    """Reference: cosine between product vectors over users, r = log(1 + score)."""
    vectors: dict[int, dict[int, float]] = {}
    for u, p, s in rows:
        vectors.setdefault(p, {})[u] = math.log1p(s)
    out = {}
    for i, vi in vectors.items():
        for j, vj in vectors.items():
            common = vi.keys() & vj.keys()
            if i == j or len(common) < min_common:
                continue
            dot = sum(vi[u] * vj[u] for u in common)
            norm = math.sqrt(sum(x * x for x in vi.values())) * math.sqrt(sum(x * x for x in vj.values()))
            out[(i, j)] = dot / norm
    return out


def test_similarity_matches_python(spark):
    rng = random.Random(3)
    rows = [(u, p, rng.randint(1, 10)) for u in range(1, 9) for p in range(10, 16) if rng.random() < 0.6]
    params = KNNParams(neighbours=10, max_items_per_user=10, min_common_users=1)
    got = sims_dict(item_knn.similarities(interactions(spark, rows), params))
    expected = python_cosine(rows, min_common=1)
    assert got.keys() == expected.keys()
    for pair, value in expected.items():
        assert got[pair] == pytest.approx(value), pair


def test_identical_products_have_similarity_one(spark):
    rows = [(1, 10, 3), (1, 11, 3), (2, 10, 1), (2, 11, 1)]
    got = sims_dict(item_knn.similarities(interactions(spark, rows), KNNParams(min_common_users=2)))
    assert got == {(10, 11): pytest.approx(1.0), (11, 10): pytest.approx(1.0)}


def test_min_common_users_and_top_neighbours(spark):
    rows = [(1, 10, 1), (1, 11, 1), (2, 10, 1), (2, 11, 1),   # 10-11: 2 common users
            (3, 10, 1), (3, 12, 1), (4, 10, 1), (4, 12, 1),   # 10-12: 2 common users
            (5, 10, 1), (5, 13, 1)]                           # 10-13: 1 common user, dropped
    inter = interactions(spark, rows)
    got = sims_dict(item_knn.similarities(inter, KNNParams(neighbours=5, min_common_users=2)))
    assert set(got) == {(10, 11), (11, 10), (10, 12), (12, 10)}
    # 11 and 12 tie as neighbours of 10: the lower id is kept
    top1 = sims_dict(item_knn.similarities(inter, KNNParams(neighbours=1, min_common_users=2)))
    assert {pair for pair in top1 if pair[0] == 10} == {(10, 11)}


def test_cap_keeps_each_users_strongest_products(spark):
    # with max_items_per_user=2 both users drop product 12 (their weakest), so 12 has no neighbours
    rows = [(1, 10, 5), (1, 11, 5), (1, 12, 1), (2, 10, 5), (2, 11, 5), (2, 12, 1)]
    got = sims_dict(item_knn.similarities(interactions(spark, rows), KNNParams(max_items_per_user=2)))
    assert set(got) == {(10, 11), (11, 10)}


def test_recommend_sums_weighted_similarities(spark):
    inter = interactions(spark, [(1, 10, 1), (1, 11, 3), (2, 12, 1)])
    sims = spark.createDataFrame(
        [(10, 20, 0.5), (10, 21, 0.4), (11, 21, 0.3), (11, 22, 0.1), (12, 20, 0.9)],
        ["product_id", "neighbour_id", "sim"])
    users = spark.createDataFrame([(1,), (1,), (3,)], ["user_id"])   # user 3 has no history
    recs = item_knn.recommend(inter, sims, users, k=2).collect()

    r10, r11 = math.log1p(1), math.log1p(3)
    expected = {21: r10 * 0.4 + r11 * 0.3, 20: r10 * 0.5, 22: r11 * 0.1}
    assert [(r.user_id, r.product_id, r.rank) for r in sorted(recs, key=lambda r: r.rank)] == [
        (1, 21, 1), (1, 20, 2)]
    for r in recs:
        assert r.score == pytest.approx(expected[r.product_id])
