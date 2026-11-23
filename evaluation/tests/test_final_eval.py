"""Final evaluation (week 7): bootstrap intervals, segments, completed lists, one tiny end-to-end run.

Run from the repository root:  python -m pytest evaluation/tests -q
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from batch.als import ALSParams
from evaluation import bootstrap
from evaluation.final_eval import (MODELS, complete, final_lists, format_report, run, sample_per_segment,
                                   user_segments)


# ---- bootstrap (plain numpy) ------------------------------------------------------------------

def test_mean_ci_of_a_constant_is_the_constant():
    (ci,) = bootstrap.mean_ci(np.full(50, 0.25), resamples=200)
    assert ci == {"mean": 0.25, "low": 0.25, "high": 0.25}


def test_mean_ci_brackets_the_mean_and_is_reproducible():
    rng = np.random.default_rng(0)
    x = np.column_stack([rng.random(500), (rng.random(500) < 0.3).astype(float)])
    a = bootstrap.mean_ci(x, resamples=500, seed=7)
    assert a == bootstrap.mean_ci(x, resamples=500, seed=7)
    assert a != bootstrap.mean_ci(x, resamples=500, seed=8)
    for col, ci in enumerate(a):
        assert ci["mean"] == round(x[:, col].mean(), 6)
        assert ci["low"] < ci["mean"] < ci["high"]
    # a 0/1 column with p = 0.3 over 500 users: the interval is about +-1.96 * sqrt(p(1-p)/n) = 0.04
    width = a[1]["high"] - a[1]["low"]
    assert 0.06 < width < 0.10


def test_mean_ci_without_users():
    assert bootstrap.mean_ci(np.empty((0, 2))) == [{"mean": None, "low": None, "high": None}] * 2


def test_paired_differences():
    rng = np.random.default_rng(1)
    base = rng.random((300, 1))
    (same,) = bootstrap.paired_diff_ci(base, base, resamples=200)
    assert same == {"mean": 0.0, "low": 0.0, "high": 0.0}
    # better by 0.05 on average, with noise much smaller than the spread between users
    (better,) = bootstrap.paired_diff_ci(base + 0.05 + rng.normal(0, 0.01, (300, 1)), base, resamples=500)
    assert 0 < better["low"] < 0.05 < better["high"]
    with pytest.raises(ValueError):
        bootstrap.paired_diff_ci(np.zeros((3, 2)), np.zeros((4, 2)))


# ---- users and lists (Spark) ------------------------------------------------------------------

def test_user_segments(spark):
    interactions = spark.createDataFrame(
        [(1, p, 1.0) for p in range(3)] + [(2, p, 1.0) for p in range(5)] + [(3, p, 1.0) for p in range(6)],
        ["user_id", "product_id", "score"])
    users = spark.createDataFrame([(1,), (2,), (3,), (4,), (4,)], ["user_id"])
    got = {r["user_id"]: (r["train_products"], r["segment"]) for r in user_segments(interactions, users).collect()}
    assert got == {1: (3, "light"), 2: (5, "light"), 3: (6, "heavy"), 4: (0, "new")}


def test_sample_per_segment(spark):
    segments = spark.createDataFrame([(u, "new" if u < 10 else "heavy") for u in range(1, 31)],
                                     ["user_id", "segment"])
    picked = sample_per_segment(segments, 4, seed=42)
    rows = picked.collect()
    assert sorted(r["segment"] for r in rows) == ["heavy"] * 4 + ["new"] * 4
    assert {r["user_id"] for r in rows} == {r["user_id"] for r in sample_per_segment(segments, 4, 42).collect()}
    assert {r["user_id"] for r in rows} != {r["user_id"] for r in sample_per_segment(segments, 4, 7).collect()}
    assert sample_per_segment(segments, 0, 42).count() == 30


def lists_of(df):
    out = {}
    for r in df.orderBy("user_id", "rank").collect():
        out.setdefault(r["user_id"], []).append(r["product_id"])
    return out


def test_complete_fills_short_lists_with_the_fallback(spark):
    cols = ["user_id", "product_id", "rank"]
    recs = spark.createDataFrame([(1, 30, 1), (1, 10, 2), (2, 40, 1)], cols)
    fallback = spark.createDataFrame([(u, p, r) for u in (1, 2, 3) for r, p in enumerate([10, 20, 30, 50], 1)], cols)
    got = complete(recs, fallback, 3)
    assert lists_of(got) == {1: [30, 10, 20], 2: [40, 10, 20], 3: [10, 20, 30]}
    assert sorted(r["rank"] for r in got.where("user_id = 1").collect()) == [1, 2, 3]


def test_final_lists_remove_seen_products_from_both(spark):
    cols = ["user_id", "product_id", "rank"]
    recs = spark.createDataFrame([(1, 30, 1), (1, 10, 2), (1, 60, 3)], cols)
    popular = spark.createDataFrame([(u, p, r) for u in (1, 2) for r, p in enumerate([10, 20, 30], 1)], cols)
    seen = spark.createDataFrame([(1, 30), (1, 20), (2, 10)], ["user_id", "product_id"])
    assert lists_of(final_lists(recs, popular, None, 3)) == {1: [30, 10, 60], 2: [10, 20, 30]}
    assert lists_of(final_lists(recs, popular, seen, 3)) == {1: [10, 60], 2: [20, 30]}


# ---- end to end -------------------------------------------------------------------------------

def test_final_eval_end_to_end(spark):
    """Heavy users in two communities of 8 products: in train each views 7, in test the 8th.
    Light users bought only the popular product 300 (too few products for the ALS matrix); in test
    they view 300 again and product 104. New users appear only in test."""
    cols = ["user_id", "product_id", "category_id", "event_type"]
    train_rows, test_rows = [], []
    for u in range(1, 41):
        base = 100 if u <= 20 else 200
        skipped = base + u % 8
        train_rows += [(u, base + i, base, "view") for i in range(8) if base + i != skipped]
        test_rows.append((u, skipped, base, "view"))
    for u in range(41, 61):
        train_rows.append((u, 300, 300, "purchase"))
    for u in range(41, 51):
        test_rows += [(u, 300, 300, "view"), (u, 104, 100, "view")]
    for u in range(70, 80):
        test_rows += [(u, 300, 300, "view"), (u, 100, 100, "view")]
    train = spark.createDataFrame(train_rows, cols)
    test = spark.createDataFrame(test_rows, cols)

    report = run(train, test, ALSParams(rank=4, reg_param=0.01, max_iter=10), eval_users=0, segment_users=0,
                 ks=(1, 5), resamples=200)
    json.dumps(report)                                         # the report is plain JSON

    data, res = report["data"], report["results"]
    assert data["segments"] == {"heavy": 40, "light": 10, "new": 10}
    assert data["scored_users"] == 60
    assert data["truth"]["all"] == {"users": 60, "pairs": 40 + 20 + 20}
    assert data["truth"]["unseen"] == {"users": 60, "pairs": 40 + 10 + 20}      # 300 was seen by light users
    assert data["als_users"] == data["item_knn_users"] == 40
    assert [res[g]["all"]["users"] for g in ("warm", "heavy", "light", "new")] == [40, 40, 10, 10]

    # warm = heavy here: every one has an own ALS and Item-kNN list
    assert res["warm"]["own_list_share"] == {"item_knn": 1.0, "als": 1.0}
    heavy_all = res["heavy"]["all"]["models"]
    assert heavy_all["als"]["ndcg@5"]["mean"] > heavy_all["popularity"]["ndcg@5"]["mean"]
    # without seen products, the only product left in the user's community is the one viewed in test
    heavy_unseen = res["heavy"]["unseen"]["models"]
    assert heavy_unseen["item_knn"]["hit_rate@1"]["mean"] == 1.0
    assert heavy_unseen["als"]["ndcg@5"]["mean"] > heavy_unseen["popularity"]["ndcg@5"]["mean"]
    diff = res["heavy"]["unseen"]["differences"]["item_knn - popularity"]["hit_rate@1"]
    assert diff["low"] > 0

    # light and new users have no list of their own: every model falls back to Popularity
    for group in ("light", "new"):
        assert res[group]["own_list_share"] == {"item_knn": 0.0, "als": 0.0}
        for v in ("all", "unseen"):
            m = res[group][v]["models"]
            assert m["als"] == m["item_knn"] == m["popularity"]
            assert res[group][v]["differences"]["als - popularity"]["ndcg@5"] == {"mean": 0.0, "low": 0.0,
                                                                                  "high": 0.0}
    # product 300 is the most popular: a hit at rank 1 for light users, unless it is removed as seen
    assert res["light"]["all"]["models"]["popularity"]["hit_rate@1"]["mean"] == 1.0
    assert res["light"]["unseen"]["models"]["popularity"]["hit_rate@1"]["mean"] == 0.0
    assert res["new"]["all"]["models"]["popularity"]["hit_rate@1"]["mean"] == 1.0

    assert set(res["warm"]["coverage@1"]) == {"all", "unseen"}
    assert set(res["warm"]["coverage@1"]["all"]) == set(MODELS)
    table = format_report(report, "ndcg@5")
    assert table.count("\n") == 1 + 4 * 2                      # header, rule, 4 groups x 2 variants
    assert "| new | unseen | 10 |" in table
