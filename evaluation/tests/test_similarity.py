"""Comparison of similarity tables on hand-made data (local Spark).

Run from the repository root:  python -m pytest evaluation/tests -q
"""

from __future__ import annotations

from datetime import datetime

import pytest

from evaluation.similarity import compare, format_table, next_item, overlap, same_category, transitions

SIMS = ["product_id", "neighbour_id", "sim"]


def table(spark, rows):
    return spark.createDataFrame(rows, SIMS)


def test_overlap_counts_shared_neighbours(spark):
    a = table(spark, [(1, 2, .9), (1, 3, .8), (1, 4, .7), (5, 6, .9)])
    b = table(spark, [(1, 3, .9), (1, 9, .5), (7, 8, .9)])
    got = overlap(a, b, ks=(1, 3))
    assert got["products_in_both"] == 1
    assert got["overlap@1"] == 0.0                 # top-1: {2} vs {3}
    assert got["overlap@3"] == pytest.approx(0.5)  # {2,3,4} vs {3,9}: 1 shared / min(3, 2)


def test_same_category_share(spark):
    sims = table(spark, [(1, 2, .9), (1, 3, .8), (2, 1, .9), (2, 4, .1)])
    products = spark.createDataFrame([(1, 7), (2, 7), (3, 8), (4, None)], "product_id long, category_id long")
    got = same_category(sims, products, ks=(1, 2))
    assert got["same_category@1"] == 1.0           # 1->2 and 2->1 share category 7
    assert got["same_category@2"] == pytest.approx(2 / 3)   # 1->3 differs; 2->4 has no category


def test_transitions_and_next_item(spark):
    t0 = datetime(2019, 11, 16, 10, 0, 0)
    ev = [("s1", t0.replace(minute=m), "view", p) for m, p in [(0, 1), (1, 1), (2, 2), (3, 3), (4, 9)]]
    ev += [("s2", t0, "view", 2), ("s2", t0.replace(minute=5), "cart", 1)]
    ev += [(None, t0, "view", 5), (None, t0.replace(minute=1), "view", 6)]   # no session: ignored
    events = spark.createDataFrame(ev, "user_session string, event_time timestamp, event_type string, product_id long")
    catalog = spark.createDataFrame([(1,), (2,), (3,)], ["product_id"])     # 9 is not in the catalog

    trans = transitions(events, catalog, n=0)
    pairs = sorted((r.prev_id, r.next_id) for r in trans.collect())
    assert pairs == [(1, 2), (2, 1), (2, 3), (3, 9)]   # repeat view 1->1 dropped; prev 9 never occurs

    sims = table(spark, [(1, 2, .9), (2, 5, .9), (2, 3, .4)])
    got = next_item(sims, trans, ks=(1, 2))
    assert got["transitions"] == 4
    assert got["covered"] == 0.75                      # product 3 has no neighbours
    assert got["hit@1"] == 0.25                        # only 1->2
    assert got["hit@2"] == 0.5                         # and 2->3 at rank 2


def test_compare_report(spark):
    a = table(spark, [(1, 2, .9), (2, 1, .9)])
    b = table(spark, [(1, 2, .5)])
    products = spark.createDataFrame([(1, 7), (2, 7)], "product_id long, category_id long")
    report = compare({"sim_item": a, "item_knn": b}, products, ks=(1,))
    assert report["tables"]["sim_item"]["products"] == 2 and report["tables"]["item_knn"]["rows"] == 1
    assert report["overlap"]["tables"] == ["sim_item", "item_knn"] and report["overlap"]["overlap@1"] == 1.0
    assert "| same_category@1 |" in format_table(report)
