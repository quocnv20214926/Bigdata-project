"""Ranking metrics and the offline evaluation runner (local Spark).

Run from the repository root:  python -m pytest evaluation/tests -q
"""

from __future__ import annotations

import math
import random

import pytest

from batch.als import ALSParams
from evaluation.metrics import (
    catalog_coverage, hit_rate_at_k, ndcg_at_k, precision_at_k, ranking_metrics, recall_at_k,
)
from evaluation.offline import format_table, run


def test_single_user_metrics():
    recs = [5, 1, 7, 3]
    truth = {1, 3, 9}
    assert precision_at_k(recs, truth, 2) == 0.5
    assert recall_at_k(recs, truth, 4) == pytest.approx(2 / 3)
    assert hit_rate_at_k(recs, truth, 1) == 0.0 and hit_rate_at_k(recs, truth, 2) == 1.0
    dcg = 1 / math.log2(3) + 1 / math.log2(5)
    idcg = 1 + 1 / math.log2(3) + 1 / math.log2(4)
    assert ndcg_at_k(recs, truth, 4) == pytest.approx(dcg / idcg)
    assert ndcg_at_k([1, 3, 9], truth, 3) == pytest.approx(1.0)


def test_spark_metrics_match_python(spark):
    rng = random.Random(7)
    recs_by_user = {u: rng.sample(range(50), 20) for u in range(1, 31)}
    truth_by_user = {u: set(rng.sample(range(50), rng.randint(1, 15))) for u in range(1, 36)}
    # users 31-35 have ground truth but no recommendations: they score 0
    recs = spark.createDataFrame(
        [(u, p, i + 1) for u, ps in recs_by_user.items() for i, p in enumerate(ps)],
        ["user_id", "product_id", "rank"])
    truth = spark.createDataFrame(
        [(u, p) for u, ps in truth_by_user.items() for p in ps], ["user_id", "product_id"])

    got = ranking_metrics(recs, truth, ks=(5, 10))
    assert got["users"] == 35
    for k in (5, 10):
        for name, fn in [("precision", precision_at_k), ("recall", recall_at_k),
                         ("hit_rate", hit_rate_at_k), ("ndcg", ndcg_at_k)]:
            expected = sum(fn(recs_by_user.get(u, []), t, k) for u, t in truth_by_user.items()) / 35
            assert got[f"{name}@{k}"] == pytest.approx(expected, abs=1e-6), f"{name}@{k}"


def test_catalog_coverage(spark):
    recs = spark.createDataFrame([(1, 10, 1), (1, 11, 2), (2, 10, 1), (2, 12, 2)],
                                 ["user_id", "product_id", "rank"])
    assert catalog_coverage(recs, 1, 10) == 0.1
    assert catalog_coverage(recs, 2, 10) == 0.3


def test_offline_run_end_to_end(spark):
    """Two communities; in the test period each user views the item it skipped in train."""
    cols = ["user_id", "product_id", "category_id", "event_type"]
    train_rows, test_rows = [], []
    for u in range(1, 41):
        base = 100 if u <= 20 else 200
        skipped = base + u % 5
        train_rows += [(u, base + i, base, "view") for i in range(5) if base + i != skipped]
        test_rows.append((u, skipped, base, "view"))
    train_rows += [(u, 300, 300, "purchase") for u in range(41, 61)]   # popular, but not relevant
    test_rows.append((99, 100, 100, "view"))                           # user not in train: not evaluated
    train = spark.createDataFrame(train_rows, cols)
    test = spark.createDataFrame(test_rows, cols)

    report = run(train, test, ALSParams(rank=4, reg_param=0.01, max_iter=10), eval_users=0, ks=(1, 5))

    assert report["data"]["ground_truth"] == {"users": 40, "pairs": 40}
    assert report["data"]["als_matrix"]["users"] == 40          # users 41-60 have 1 product each
    pop, als_ = report["results"]["popularity"], report["results"]["als"]
    assert pop["users"] == als_["users"] == 40
    assert pop["hit_rate@1"] == 0.0                              # everyone gets product 300 first
    assert als_["hit_rate@5"] > pop["hit_rate@5"]
    assert "| als |" in format_table(report["results"])
