"""batch/demo_recommend.py: list scoring by hand, then the Spark and Redis plumbing end to end.

Redis is replaced by a small in-memory stand-in (only the calls the demo makes), so these tests
need no server. Run from the repository root:  python -m pytest batch/tests -q
"""

from __future__ import annotations

import json
import math
from datetime import date

import pytest

from batch import demo_recommend as demo
from common import redis_keys as rk


class FakeRedis:
    """zrevrange / exists / get and a pipeline over plain dicts."""

    def __init__(self, zsets=None, strings=None):
        self.zsets = {k: dict(v) for k, v in (zsets or {}).items()}
        self.strings = dict(strings or {})

    def zrevrange(self, key, start, end, withscores=False):
        rows = sorted(self.zsets.get(key, {}).items(), key=lambda kv: (-kv[1], str(kv[0])))
        rows = rows[start:None if end == -1 else end + 1]
        return [(str(m), float(s)) for m, s in rows] if withscores else [str(m) for m, _ in rows]

    def exists(self, key):
        return int(key in self.zsets or key in self.strings)

    def get(self, key):
        return self.strings.get(key)

    def pipeline(self, transaction=False):
        return _Pipeline(self)


class _Pipeline:
    def __init__(self, client):
        self.client, self.calls = client, []

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            return self
        return call

    def execute(self):
        return [getattr(self.client, n)(*a, **kw) for n, a, kw in self.calls]


def ev(event_type, product_id, category_id=1, time="2019-11-01 10:00"):
    return {"event_type": event_type, "product_id": product_id, "category_id": category_id, "time": time}


def test_interaction_scores_and_main_category():
    events = [ev("view", 1, 7), ev("view", 1, 7), ev("cart", 2, 8), ev("remove_from_cart", 3, 9),
              ev("purchase", 4, 9)]
    assert demo.interaction_scores(events) == {1: 2.0, 2: 3.0, 4: 5.0}
    assert demo.main_category(events) == 9                       # 5 beats 3 and 2
    assert demo.main_category([ev("view", 1, 7), ev("view", 2, 5)]) == 5   # tie: smaller id
    assert demo.main_category([ev("remove_from_cart", 1, 7)]) is None
    assert demo.main_category([]) is None


def test_knn_scores_by_hand():
    history = {1: 1.0, 2: 3.0}                     # r = log 2 and log 4
    neighbours = {1: [(10, 0.5), (20, 0.4)], 2: [(20, 0.25), (1, 0.9)], 3: [(30, 1.0)]}
    got = dict(demo.knn_scores(history, neighbours, 10))
    assert got[10] == pytest.approx(math.log(2) * 0.5)
    assert got[20] == pytest.approx(math.log(2) * 0.4 + math.log(4) * 0.25)
    assert got[1] == pytest.approx(math.log(4) * 0.9)              # seen products are kept
    assert 30 not in got
    assert [p for p, _ in demo.knn_scores(history, neighbours, 10)] == [1, 20, 10]
    assert demo.knn_scores(history, neighbours, 1) == [(1, pytest.approx(math.log(4) * 0.9))]
    assert demo.ranked({5: 1.0, 3: 1.0, 9: 2.0}, 3) == [(9, 2.0), (3, 1.0), (5, 1.0)]


def test_marks_and_list_metrics():
    marks = demo.period_marks([ev("view", 1), ev("purchase", 1), ev("cart", 2), ev("view", 3), ev("cart", 3),
                             ev("view", 4), ev("remove_from_cart", 5)])
    assert marks == {1: "P", 2: "C", 3: "C", 4: "V"}
    m = demo.score_list([4, 9, 1], marks, 3)
    assert m["hits"] == 2 and m["hit@3"] == 1.0 and m["intent_hits"] == 1
    idcg_all = 1 + 1 / math.log2(3) + 1 / math.log2(4)            # 4 relevant, k = 3
    assert m["ndcg@3"] == pytest.approx(round((1 + 1 / math.log2(4)) / idcg_all, 4))
    idcg_intent = 1 + 1 / math.log2(3) + 1 / math.log2(4)         # 3 relevant (1, 2, 3)
    assert m["intent_ndcg@3"] == pytest.approx(round((1 / math.log2(4)) / idcg_intent, 4))
    assert demo.score_list([], marks, 3)["hit@3"] == 0.0


def redis_with_batch():
    return FakeRedis(zsets={
        rk.rec_batch(7): {100: 0.9, 101: 0.8, 102: 0.7},
        rk.sim_item(1): {100: 0.6, 200: 0.5},
        rk.sim_item(2): {200: 0.4},
        rk.pop_cat(9): {300: 50.0, 301: 40.0},
        rk.POP_GLOBAL: {400: 99.0, 401: 98.0},
    }, strings={rk.META_BATCH_LAST_RUN: "2026-11-12T01:00:00Z"})


def test_build_warm_and_cold_users():
    client = redis_with_batch()
    history = [ev("view", 1, 9), ev("cart", 2, 9)]
    test = [ev("purchase", 100), ev("view", 200), ev("view", 999)]
    warm = demo.build(client, 7, history, test, k=2)
    assert warm.has_rec_batch
    assert warm.lists["ALS"] == [(100, 0.9), (101, 0.8)]
    assert [p for p, _ in warm.lists["Item-kNN"]] == [200, 100]     # log2*0.5 + log4*0.4 > log2*0.6
    assert warm.pop_key == rk.pop_cat(9) and [p for p, _ in warm.lists["Popularity"]] == [300, 301]
    m = warm.metrics(2)
    assert m["ALS"]["hits"] == 1 and m["ALS"]["intent_hits"] == 1
    assert m["Item-kNN"]["hits"] == 2 and m["Popularity"]["hits"] == 0

    cold = demo.build(client, 8, [], [ev("purchase", 400)], k=2)     # new user: no history, no rec:batch
    assert not cold.has_rec_batch and cold.lists["ALS"] == [] and cold.lists["Item-kNN"] == []
    assert cold.pop_key == rk.POP_GLOBAL and cold.metrics(2)["Popularity"]["intent_ndcg@2"] == 1.0

    other = demo.build(client, 8, [ev("view", 1, 5)], [], k=2)       # category without pop:cat
    assert other.pop_key == rk.POP_GLOBAL


def test_render_and_json_without_product_table():
    d = demo.build(redis_with_batch(), 7, [ev("view", 1, 9)], [ev("purchase", 100)], k=3)
    text = demo.render(d, None, 3, date(2019, 11, 16), date(2019, 11, 30))
    assert "User 7" in text and "rec:batch: yes" in text and "P*" not in text
    assert "popularity list: pop:cat:9 (main category of the history)" in text
    assert "1 event, 1 product (view 1)" in text
    assert "    100 ?" in text and "hits 1, NDCG@3 1.000" in text
    out = demo.to_json(d, None, 3)
    assert out["lists"]["ALS"][0] == {"product_id": 100, "score": 0.9, "label": "?", "test_event": "P"}
    assert out["test"] == {"events": 1, "products": 1, "intent_products": 1}
    json.dumps(out)


@pytest.fixture
def clean_events(spark, tmp_path):
    t = str     # times are parsed by Spark in its session time zone (UTC), whatever the machine's is
    rows = [  # (event_time, event_type, product_id, category_id, category_code, brand, price, user_id)
        (t("2019-11-10 08:00:00"), "view", 1, 9, "electronics.smartphone", "samsung", 100.0, 7),
        (t("2019-11-15 23:59:00"), "cart", 2, 9, None, "apple", 200.0, 7),
        (t("2019-11-16 00:01:00"), "purchase", 100, 9, "electronics.audio.headphone", None, 30.0, 7),
        (t("2019-11-20 12:00:00"), "view", 2, 9, None, "apple", 210.0, 7),
        (t("2019-12-01 00:00:00"), "view", 101, 9, None, None, 1.0, 7),       # after --until
        (t("2019-11-17 09:00:00"), "cart", 300, 9, None, None, 5.0, 8),
        (t("2019-11-18 09:00:00"), "view", 400, 9, None, None, 5.0, 9),        # no cart/purchase
    ]
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows, "event_time string, event_type string, product_id int, category_id long, "
                                     "category_code string, brand string, price double, user_id int")
    path = str(tmp_path / "clean")
    df = df.withColumn("event_time", F.to_timestamp("event_time"))
    df.withColumn("event_date", F.to_date("event_time")).write.partitionBy("event_date").parquet(path)
    return path


def test_spark_side(spark, clean_events, tmp_path):
    as_of, until = date(2019, 11, 16), date(2019, 11, 30)
    events = demo.user_events(spark, clean_events, [7, 8], as_of, until)
    history, test = events[7]
    assert [(e["event_type"], e["product_id"], e["time"]) for e in history] == [
        ("view", 1, "2019-11-10 08:00"), ("cart", 2, "2019-11-15 23:59")]
    assert [e["product_id"] for e in test] == [100, 2]
    assert events[8] == ([], [ev("cart", 300, 9, "2019-11-17 09:00")])

    cache = tmp_path / "demo" / "products.parquet"
    products = demo.product_table(spark, clean_events, str(cache))
    assert cache.exists()
    assert demo.label(products, 1) == "samsung smartphone" and demo.label(products, 100) == "headphone"
    assert demo.label(products, 300) == "-" and demo.label(products, 12345) == "?"
    assert demo.price(products, 2) == "210.00" and demo.price(products, 12345) == ""

    client = redis_with_batch()                       # rec:batch only for user 7
    assert demo.pick_users(spark, client, clean_events, as_of, until, 5, 42, warm=True) == [7]
    assert demo.pick_users(spark, client, clean_events, as_of, until, 5, 42, warm=False) == [8]
    assert demo.pick_users(spark, client, clean_events, as_of, until, 0, 42, warm=True) == []


def test_main_end_to_end(spark, clean_events, tmp_path, monkeypatch, capsys):
    from batch import redis_loader, spark_session

    monkeypatch.setattr(redis_loader.RedisTarget, "client", lambda self: redis_with_batch())
    monkeypatch.setattr(spark_session, "get_spark", lambda *a, **kw: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)          # the module fixture stops it
    report = tmp_path / "demo.json"
    demo.main(["--pick", "1", "--pick-cold", "1", "--user-id", "7", "--input", clean_events, "--no-s3",
               "--products", str(tmp_path / "products.parquet"), "--k", "3", "--json", str(report)])
    text = capsys.readouterr().out
    assert "meta:batch:last_run = 2026-11-12T01:00:00Z" in text and "User 7" in text and "User 8" in text
    users = json.loads(report.read_text())["users"]
    assert [u["user_id"] for u in users] == [7, 8]              # 7 given and picked: listed once
    assert users[0]["lists"]["ALS"][0]["test_event"] == "P" and users[0]["pop_key"] == rk.pop_cat(9)
    assert users[1]["has_rec_batch"] is False and users[1]["pop_key"] == rk.POP_GLOBAL
