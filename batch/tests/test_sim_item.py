"""sim:item from ALS item factors: blocked top-n against brute force (numpy, local Spark).

Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

import numpy as np
import pytest

from batch.sim_item import block_top_n, normalised_factors, similar_items


def brute_force(ids, vectors, n):
    """{product: [(neighbour, sim), ...]} with plain Python loops."""
    unit = [v / (np.linalg.norm(v) or 1.0) for v in vectors]
    out = {}
    for a, pa in enumerate(ids):
        pairs = [(float(np.dot(unit[a], unit[b])), int(pb)) for b, pb in enumerate(ids) if b != a]
        pairs = [(pb, s) for s, pb in sorted(pairs, key=lambda x: (-x[0], x[1])) if s > 0][:n]
        out[int(pa)] = pairs
    return out


def as_lists(rows):
    out = {}
    for r in sorted(rows, key=lambda r: (r["product_id"], r["rank"])):
        out.setdefault(int(r["product_id"]), []).append((int(r["neighbour_id"]), float(r["sim"])))
    return out


def check_same(got, expected):
    assert set(got) <= set(expected)
    for p, pairs in expected.items():
        mine = got.get(p, [])
        assert [q for q, _ in mine] == [q for q, _ in pairs], p
        assert [s for _, s in mine] == pytest.approx([s for _, s in pairs], abs=1e-5)


def test_block_top_n_matches_brute_force():
    rng = np.random.default_rng(3)
    ids = np.array(sorted(rng.choice(10_000, 40, replace=False)), dtype=np.int64)
    vectors = rng.normal(size=(40, 6)).astype(np.float32)
    vectors[7] = 0.0                                              # zero vector: no neighbours
    unit = vectors / np.maximum(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-30)
    expected = brute_force(ids, vectors, 5)
    got = {}
    for lo in range(0, 40, 16):                                   # blocks of 16, last one smaller
        pdf = block_top_n(ids, unit, lo, min(lo + 16, 40), 5)
        got.update(as_lists(pdf.to_dict("records")))
        assert set(pdf["rank"]) <= {1, 2, 3, 4, 5}
    check_same(got, expected)
    assert int(ids[7]) not in got
    assert all(q != int(ids[7]) for pairs in got.values() for q, _ in pairs)


def test_block_top_n_ties_and_small_catalogs():
    ids = np.array([10, 20, 30], dtype=np.int64)
    unit = np.array([[1, 0], [1, 0], [1, 0]], dtype=np.float32)   # all identical: sim 1
    pdf = block_top_n(ids, unit, 0, 3, 50)
    assert len(pdf) == 6                                          # n capped at products - 1
    first = pdf[pdf["product_id"] == 10].sort_values("rank")
    assert first["neighbour_id"].tolist() == [20, 30]             # tie: smaller id first
    assert block_top_n(ids[:1], unit[:1], 0, 1, 5).empty


def test_similar_items_on_spark(spark):
    rng = np.random.default_rng(11)
    vectors = rng.normal(size=(300, 8)).astype(np.float32)
    ids = np.arange(1000, 1300, dtype=np.int64)
    factors = spark.createDataFrame([(int(i), v.tolist()) for i, v in zip(ids, vectors)], ["id", "features"])

    got_ids, unit = normalised_factors(factors)
    assert got_ids.tolist() == ids.tolist()
    assert np.allclose(np.linalg.norm(unit, axis=1), 1.0, atol=1e-5)

    sims = similar_items(factors, n=10, block_size=64)
    rows = [r.asDict() for r in sims.collect()]
    check_same(as_lists(rows), brute_force(ids, vectors, 10))
    assert {r["rank"] for r in rows} <= set(range(1, 11))
    assert all(r["sim"] > 0 for r in rows)
