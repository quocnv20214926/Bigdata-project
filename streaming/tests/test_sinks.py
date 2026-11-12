"""Unit tests for the Redis sinks (no Spark). Run: python -m pytest streaming/tests -q"""

from __future__ import annotations

import math
from collections import namedtuple

import fakeredis
import pytest

from common import redis_keys as rk
from streaming.sinks import cooc, trending
from streaming.sinks.user_state import write_user_state

Ev = namedtuple("Ev", "user_id product_id event_type epoch user_session")
Tr = namedtuple("Tr", "window_end category_id product_id score")


@pytest.fixture()
def r():
    return fakeredis.FakeRedis(decode_responses=True)


# ---- user state (week 3, contract 1.1.0: ZSETs) ---------------------------

def test_recent_keeps_newest_and_is_idempotent(r):
    rows = [Ev(1, 100 + i, "view", 1_570_000_000 + i, "s") for i in range(25)]
    write_user_state(rows, r, recent_n=20, flush_every=7)
    write_user_state(rows, r, recent_n=20)                       # replayed batch
    newest = [rk.decode_recent(m) for m in r.zrevrange(rk.user_recent(1), 0, -1)]
    assert len(newest) == 20
    assert newest[0] == (124, "view", 1_570_000_024) and newest[-1][0] == 105
    assert 0 < r.ttl(rk.user_recent(1)) <= rk.TTL["user:recent"]


def test_bought_zset_keeps_latest_purchase(r):
    write_user_state([Ev(7, 5, "purchase", 30, "s"), Ev(7, 5, "purchase", 10, "s"),
                      Ev(7, 9, "cart", 40, "s"), Ev(8, 5, "view", 50, "s")], r)
    assert r.zrange(rk.user_bought(7), 0, -1, withscores=True) == [("5", 30.0)]
    assert r.zscore(rk.user_bought(7), "9") is None               # cart is not a purchase
    assert not r.exists(rk.user_bought(8))
    assert 0 < r.ttl(rk.user_bought(7)) <= rk.TTL["user:bought"]


# ---- co-occurrence (week 4) ----------------------------------------------

def sim(r, i):
    return {int(j): s for j, s in r.zrange(rk.sim_cooc(i), 0, -1, withscores=True)}


def test_cooc_counts_and_normalisation(r):
    # session a: 1 2 3 ; session b: 1 2 ; session c: 1 4
    rows = [Ev(1, 1, "view", 1, "a"), Ev(1, 2, "view", 2, "a"), Ev(1, 3, "view", 3, "a"),
            Ev(2, 1, "view", 1, "b"), Ev(2, 2, "view", 2, "b"),
            Ev(3, 1, "view", 1, "c"), Ev(3, 4, "cart", 2, "c")]
    cooc.write_partition_cooc(rows, r)
    assert r.hgetall(cooc.COUNT_KEY) == {"1": "3", "2": "2", "3": "1", "4": "1"}
    assert r.hgetall(cooc.pair_key(1)) == {"2": "2", "3": "1", "4": "1"}
    s1 = sim(r, 1)
    assert s1[2] == pytest.approx(2 / math.sqrt(3 * 2))          # cooc / sqrt(cnt_i * cnt_j)
    assert s1[3] == pytest.approx(1 / math.sqrt(3))
    assert max(s1, key=s1.get) == 2
    assert 0 < r.ttl(rk.sim_cooc(1)) <= rk.TTL["sim:cooc"]


def test_cooc_revisit_counts_once_and_session_continues_across_batches(r):
    cooc.write_partition_cooc([Ev(1, 1, "view", 1, "a"), Ev(1, 2, "view", 2, "a"),
                               Ev(1, 1, "view", 3, "a"), Ev(1, 2, "view", 4, "a")], r)
    assert r.hget(cooc.pair_key(1), "2") == "1" and r.hget(cooc.COUNT_KEY, "1") == "1"
    assert sim(r, 1) == {2: pytest.approx(1.0)}
    # next micro-batch, same session: 3 pairs with 2 and 1 (window loaded from Redis)
    cooc.write_partition_cooc([Ev(1, 3, "view", 5, "a")], r)
    assert set(sim(r, 3)) == {1, 2}
    assert r.lrange(cooc.session_key("a"), 0, -1) == ["3", "2", "1"]


def test_cooc_window_size_and_missing_session(r):
    rows = [Ev(1, p, "view", p, "a") for p in range(1, 13)] + [Ev(1, 99, "view", 20, None)]
    touched = cooc.update_cooc(rows, r, window=10)
    assert 99 not in touched                                     # no session -> ignored
    assert r.hget(cooc.pair_key(12), "1") is None                # 1 left the window of 10
    assert r.hget(cooc.pair_key(12), "2") == "1"
    assert r.llen(cooc.session_key("a")) == 10


def test_cooc_top_n(r):
    rows = [Ev(1, 0, "view", 0, "s")] + [Ev(1, p, "view", p, "s") for p in range(1, 9)]
    cooc.update_cooc(rows, r, window=10)
    cooc.refresh_similarity([0], r, top_n=3)
    assert r.zcard(rk.sim_cooc(0)) == 3


# ---- trending (week 4) -----------------------------------------------------

def test_current_window_is_the_hour_before_the_newest_event():
    assert trending.duration_seconds("1 hour") == 3600 and trending.duration_seconds("5 minutes") == 300
    # newest event at 10:06 -> windows end at 10:10 ... 11:05; the last hour is [09:10, 10:10)
    assert trending.current_window_end(11 * 3600 + 300, 3600, 300) == 10 * 3600 + 600


def test_trending_publishes_window_and_all_its_categories_on_rollover(r):
    rows = [Tr(3600, 10, 1, 5.0), Tr(3600, 10, 2, 1.0), Tr(3600, 20, 3, 3.0),
            Tr(3900, 10, 1, 6.0), Tr(3900, 10, 2, 4.0), Tr(3900, 20, 3, 2.0)]
    trending.write_window_totals(rows, r)
    assert trending.publish(3600, {10}, r) == 3600
    assert r.zrevrange(rk.trend_cat(10), 0, -1, withscores=True) == [("1", 5.0), ("2", 1.0)]
    assert r.zrevrange(rk.TREND_GLOBAL, 0, 0) == ["1"]
    # window moves on, only category 10 had events in this batch: 20 is republished too
    assert trending.publish(3900, {10}, r) == 3900
    assert r.zrevrange(rk.trend_cat(10), 0, -1, withscores=True) == [("1", 6.0), ("2", 4.0)]
    assert r.zscore(rk.trend_cat(20), "3") == 2.0
    assert 0 < r.ttl(rk.trend_cat(10)) <= rk.TTL["trend"]


def test_trending_totals_are_idempotent_and_late_windows_not_published(r):
    trending.write_window_totals([Tr(3900, 10, 1, 6.0)], r)
    trending.write_window_totals([Tr(3900, 10, 1, 6.0)], r)     # replay: same total, not 12
    trending.write_window_totals([Tr(3900, 10, 1, 2.0)], r)     # stale value cannot lower it
    assert r.zscore(trending.window_cat_key(10, 3900), "1") == 6.0
    trending.publish(3900, {10}, r)
    assert trending.publish(3600, {10}, r) is None               # older window: skipped
    assert r.get(trending.PUBLISHED_END) == "3900"


def test_trending_trim_keeps_top(r):
    trending.write_window_totals([Tr(60, 1, p, float(p)) for p in range(10)], r, keep=3)
    assert r.zrange(trending.window_cat_key(1, 60), 0, -1) == ["7", "8", "9"]
    trending.publish(60, {1}, r, top_n=2)
    assert r.zrevrange(rk.trend_cat(1), 0, -1) == ["9", "8"]
