"""Offline replay (week 8): the speed layer emulation against the streaming code, the API merge,
and one tiny end-to-end run.

Run from the repository root:  python -m pytest evaluation/tests -q
"""

from __future__ import annotations

import json
import random
from collections import Counter, namedtuple
from datetime import date, datetime, timedelta

import numpy as np
import pytest

from batch.als import ALSParams
from common.scoring import merge_scores, propagate
from evaluation import replay

Ev = namedtuple("Ev", "user_session product_id")
COLS = ["event_time", "event_type", "product_id", "category_id", "user_id", "user_session", "event_date"]


def random_sessions(seed=3, sessions=40):
    rng = random.Random(seed)
    out = []
    for s in range(sessions):
        out += [Ev(f"s{s:03d}", rng.randint(1, 15)) for _ in range(rng.randint(1, 30))]
    return out


def counts_from_streaming(rows):
    fakeredis = pytest.importorskip("fakeredis")
    from streaming.sinks import cooc

    r = fakeredis.FakeRedis(decode_responses=True)
    cooc.update_cooc(rows, r)
    cnt = Counter({int(i): int(c) for i, c in r.hgetall(cooc.COUNT_KEY).items()})
    pairs = Counter()
    for i in cnt:
        for j, c in r.hgetall(cooc.pair_key(i)).items():
            if i < int(j):
                pairs[(i, int(j))] = int(c)
    return r, cooc, cnt, pairs


def test_walk_sessions_counts_like_the_streaming_sink():
    rows = random_sessions()
    _, _, cnt_ref, pairs_ref = counts_from_streaming(rows)
    cnt, pairs, state = Counter(), Counter(), {}
    cut = len(rows) // 2 + 3                       # split inside a session: the window carries over
    replay.walk_sessions([e.user_session for e in rows[:cut]], [e.product_id for e in rows[:cut]], cnt, pairs, state)
    replay.walk_sessions([e.user_session for e in rows[cut:]], [e.product_id for e in rows[cut:]], cnt, pairs, state)
    assert cnt == cnt_ref and pairs == pairs_ref
    assert len(state["window"]) <= 10


def events_frame(spark, rows):
    """Clean-event frame from (event_time, event_type, product_id, user_id, user_session)."""
    return spark.createDataFrame([(t, e, p, p // 10, u, s, t.date()) for t, e, p, u, s in rows], COLS)


def test_cooc_similarities_match_the_streaming_sink(spark):
    rows = random_sessions(seed=5)
    r, cooc, _, _ = counts_from_streaming(rows)
    cooc.refresh_similarity(range(1, 16), r)
    t0 = datetime(2019, 10, 5, 8, 0)
    events = events_frame(spark, [(t0 + timedelta(seconds=n), "view", e.product_id, 1, e.user_session)
                                  for n, e in enumerate(rows)])
    got = {}
    for row in replay.cooc_similarities(replay.cooc_counts(events)).collect():
        got.setdefault(row["product_id"], {})[row["neighbour_id"]] = row["sim"]
    for i in range(1, 16):
        expected = {int(j): s for j, s in r.zrange(f"sim:cooc:{i}", 0, -1, withscores=True)}
        assert set(got.get(i, {})) == set(expected)
        for j, s in expected.items():
            assert got[i][j] == pytest.approx(s, abs=1e-6)


def test_hybrid_similarities(spark):
    cols = ["product_id", "neighbour_id", "sim"]
    item = spark.createDataFrame([(1, 2, 0.8), (1, 3, 0.4)], cols)
    cooc = spark.createDataFrame([(1, 3, 1.0), (1, 4, 0.2)], cols)
    got = {(r["neighbour_id"]): r["sim"] for r in replay.hybrid_similarities(item, cooc, n=2).collect()}
    assert got == pytest.approx({3: 0.7, 2: 0.4})


def test_pick_targets_one_per_user(spark):
    day = date(2019, 11, 16)
    t = datetime(2019, 11, 16, 9, 0)
    rows = [(t, "cart", 10, 1, "a"), (t + timedelta(minutes=5), "purchase", 10, 1, "a"),
            (t, "view", 11, 2, "b"), (t, "cart", 12, 2, "b"), (t - timedelta(days=1), "cart", 13, 3, "c")]
    targets = replay.pick_targets(events_frame(spark, rows), day, 0, seed=42).collect()
    assert sorted(r["user_id"] for r in targets) == [1, 2]              # user 3 has no cart on the day
    assert {r["event_type"] for r in targets if r["user_id"] == 2} == {"cart"}
    assert sorted(r["target_id"] for r in targets) == [0, 1]
    assert len(replay.pick_targets(events_frame(spark, rows), day, 1, seed=42).collect()) == 1


def test_speed_state(spark):
    t = datetime(2019, 11, 16, 12, 0)
    rows = [(t - timedelta(days=40), "purchase", 30, 1, "old"),        # bought 40 days ago, kept: last purchase is recent
            (t - timedelta(days=2), "purchase", 31, 1, "x"),
            (t - timedelta(hours=30), "view", 32, 1, "x"),
            (t - timedelta(minutes=20), "view", 33, 1, "y"),
            (t - timedelta(minutes=10), "view", 34, 1, "y"),
            (t - timedelta(minutes=10), "view", 34, 1, "y"),             # duplicate event: one member
            (t, "cart", 35, 1, "y"),                                     # the target itself is not context
            (t + timedelta(minutes=1), "view", 36, 1, "y"),              # later events are not context
            (t - timedelta(hours=30), "view", 40, 2, "z"),               # user 2: newest event 30 h old
            (t, "cart", 41, 2, "z")]
    events = events_frame(spark, rows)
    targets = spark.createDataFrame([(0, 1, 35, "cart", t), (1, 2, 41, "cart", t)],
                                    ["target_id", "user_id", "product_id", "event_type", "event_time"])
    recent, bought = replay.speed_state(events, targets, recent_n=3)
    got = sorted((r["target_id"], r["product_id"], r["event_type"], r["age"]) for r in recent.collect())
    assert got == [(0, 32, "view", 30 * 3600), (0, 33, "view", 1200), (0, 34, "view", 600)]
    assert sorted((r["target_id"], r["product_id"]) for r in bought.collect()) == [(0, 30), (0, 31)]


def test_realtime_scores_follow_propagate(spark):
    recent = spark.createDataFrame([(0, 1, "view", 600), (0, 2, "cart", 3600), (1, 1, "purchase", 60)],
                                   ["target_id", "product_id", "event_type", "age"])
    neigh = {1: {5: 0.5, 6: 0.25}, 2: {5: 1.0, 7: 0.1}}
    sims = spark.createDataFrame([(i, j, s) for i, js in neigh.items() for j, s in js.items()],
                                 ["product_id", "neighbour_id", "sim"])
    got = {}
    for r in replay.realtime_scores(recent, sims, half_lives=(1800, 7200), top_n=2).collect():
        got.setdefault((r["target_id"], r["half_life"]), {})[r["product_id"]] = r["score"]
    events = {0: [(1, "view", 600), (2, "cart", 3600)], 1: [(1, "purchase", 60)]}
    for tid, evs in events.items():
        for hl in (1800, 7200):
            ref = Counter()
            for i, e, age in evs:
                ref.update(propagate(e, neigh[i], age, hl))
            top = dict(sorted(ref.items(), key=lambda kv: (-kv[1], kv[0]))[:2])
            assert got[(tid, hl)] == pytest.approx(top)


def test_rank_variants_uses_the_api_merge():
    import pandas as pd

    targets = pd.DataFrame({"target_id": [0, 1, 2], "user_id": [7, 8, 9], "product_id": [2, 4, 50]})
    batch = {7: {1: 0.9, 2: 0.5, 3: 0.1}, 8: {1: 0.9}}
    rt = {("cooc", 1800): {0: {4: 1.0, 2: 0.6, 5: 0.1}, 2: {50: 0.2, 51: 0.9}}}
    bought = {0: {3}}
    popular = [3, 60, 61, 50, 62]
    names, ranks = replay.rank_variants(targets, batch, rt, bought, popular, half_lives=(1800,), alphas=(0.3, 0.8),
                                        sims=("cooc",), k=4)
    assert names == ["popularity", "batch", "speed:cooc:1800", "hybrid:cooc:1800:0.3", "hybrid:cooc:1800:0.8"]
    by = dict(zip(names, ranks.T.tolist()))

    def expected(b, r, alpha, product, ex):
        return replay.rank_in(product, replay.final_list(merge_scores(b, r, alpha=alpha, exclude=ex, k=4),
                                                         popular, ex, 4))

    assert by["popularity"] == [0, 0, 4]                        # target 0 bought 3, so its list moves up
    assert by["batch"] == [2, 0, 4]                             # target 2 has no batch list: popularity
    assert by["speed:cooc:1800"] == [expected({}, rt[("cooc", 1800)][0], 0, 2, {3}), 0, 2]
    for a in (0.3, 0.8):
        assert by[f"hybrid:cooc:1800:{a}"][0] == expected(batch[7], rt[("cooc", 1800)][0], a, 2, {3})
        assert by[f"hybrid:cooc:1800:{a}"][1] == by["batch"][1]      # no rec:rt: the batch list
        assert by[f"hybrid:cooc:1800:{a}"][2] == by["speed:cooc:1800"][2]   # no rec:batch: the rt list

    m = replay.metrics_from_ranks(np.array([[1], [3], [0]]), ks=(1, 10))
    assert m["hit_rate@1"].ravel().tolist() == [1, 0, 0] and m["hit_rate@10"].ravel().tolist() == [1, 1, 0]
    assert m["ndcg@10"].ravel().tolist() == pytest.approx([1.0, 0.5, 0.0])


def test_choose():
    grid = {n: {"ndcg@10": 0.0} for n in replay.variant_names((10, 20), (0.5,), ("cooc", "item"))}
    grid["speed:item:20"]["ndcg@10"] = 0.3
    grid["hybrid:cooc:10:0.5"]["ndcg@10"] = 0.4
    grid["hybrid:item:20:0.5"]["ndcg@10"] = 0.5
    chosen = replay.choose(grid, "ndcg@10", (10, 20), (0.5,), ("cooc", "item"))
    assert chosen["speed:item"] == "speed:item:20" and chosen["hybrid:cooc"] == "hybrid:cooc:10:0.5"
    assert chosen["hybrid"] == "hybrid:item:20:0.5"


def tiny_world(spark):
    """Products come in pairs viewed together in October sessions; on each replayed day a user
    views one product of a pair and carts the other five minutes later. Product 99 is bought by
    everyone in October (the most popular product, then removed as bought)."""
    pairs = [(10, 11), (12, 13), (14, 15), (20, 21), (22, 23), (24, 25)]
    base = datetime(2019, 10, 2, 10, 0)
    rows = []
    for u in range(1, 41):
        for s in range(3):
            x, y = pairs[(u + s) % 6]
            t0 = base + timedelta(days=u % 20 + s, hours=s)
            rows += [(t0, "view", x, u, f"h{u}-{s}"), (t0 + timedelta(minutes=1), "view", y, u, f"h{u}-{s}")]
        rows.append((base + timedelta(days=25), "purchase", 99, u, f"p{u}"))
        for day in (date(2019, 11, 9), date(2019, 11, 16)):
            x, y = pairs[u % 6]
            t0 = datetime(day.year, day.month, day.day, 12, 0) + timedelta(minutes=u)
            rows += [(t0, "view", x, u, f"t{u}-{day.day}"), (t0 + timedelta(minutes=5), "cart", y, u, f"t{u}-{day.day}")]
    return events_frame(spark, rows)


def test_replay_end_to_end(spark):
    report = replay.run(tiny_world(spark), ALSParams(rank=4, reg_param=0.01, max_iter=3), n_targets=0,
                        half_lives=(1800, 43200), alphas=(0.3, 0.7), resamples=100)
    json.dumps(report)

    val, test = report["validation"], report["test"]
    assert val["data"]["targets"] == test["data"]["targets"] == 40
    assert test["data"]["target_events"] == {"cart": 40}
    assert test["data"]["groups"]["both"] == 40                  # every user has rec:batch and user:recent
    assert test["data"]["with_bought"] == 40                     # product 99
    assert len(test["grid"]) == 2 + 3 * 2 * (1 + 2)
    chosen = report["chosen_on_validation"]
    assert set(chosen) == {"speed:cooc", "speed:item", "speed:hybrid", "hybrid:cooc", "hybrid:item",
                           "hybrid:hybrid", "hybrid"}
    v = test["variants"]
    # the viewed product's pair partner is its strongest co-occurrence: rank 1 for speed with sim:cooc
    assert v["speed:cooc"]["hit_rate@10"]["mean"] == 1.0
    assert v["speed:cooc"]["ndcg@10"]["mean"] > v["popularity"]["ndcg@10"]["mean"]
    assert test["differences"]["hybrid - popularity"]["ndcg@10"]["low"] > 0
    assert test["groups"]["neither"]["targets"] == 0 and test["groups"]["neither"]["batch"]["ndcg@10"] is None
    assert format_lines(report) == 1 + 1 + len(v) + len(test["differences"])


def format_lines(report):
    return replay.format_report(report).count("\n") + 1


def test_days_saved_and_reused(tmp_path):
    names = replay.variant_names((1800,), (0.5,))
    rng = np.random.default_rng(7)

    def fake_day(n):
        return {"data": {"targets": n}, "seconds": {"als_train": 1.0}, "names": names,
                "ranks": rng.integers(0, 21, size=(n, len(names))).astype(np.int16),
                "group": np.array(["both", "neither"] * (n // 2))}

    days = {"validation": fake_day(40), "test": fake_day(30)}
    for name, day in days.items():
        replay.save_day(day, tmp_path / name)
    back = replay.load_day(tmp_path / "test")
    assert back["names"] == names and back["data"] == {"targets": 30}
    assert (back["ranks"] == days["test"]["ranks"]).all() and list(back["group"]) == list(days["test"]["group"])
    assert replay.load_day(tmp_path / "missing") is None

    # both days on disk: no Spark, no events needed
    part = replay.run(None, ALSParams(), cache_dir=str(tmp_path), only="test", half_lives=(1800,), alphas=(0.5,))
    assert part == {"only": "test", "data": {"targets": 30}, "seconds": {"als_train": 1.0}}
    report = replay.run(None, ALSParams(), cache_dir=str(tmp_path), half_lives=(1800,), alphas=(0.5,), resamples=50)
    grid = replay.mean_grid(names, replay.metrics_from_ranks(days["validation"]["ranks"]))
    assert report["chosen_on_validation"] == replay.choose(grid, "ndcg@10", (1800,), (0.5,))
    assert report["test"]["data"] == {"targets": 30} and report["test"]["groups"]["both"]["targets"] == 15
    json.dumps(report)
