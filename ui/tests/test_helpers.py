"""Unit tests of the Streamlit helpers (no Kafka, no API: fakeredis + a fake producer)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import fakeredis
import pytest

from common import redis_keys as rk
from common.schemas import Event
from ui import helpers as h

CLOCK = "2019-10-01T06:06:57Z"
CLOCK_EPOCH = int(datetime(2019, 10, 1, 6, 6, 57, tzinfo=timezone.utc).timestamp())


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value, on_delivery=None):
        self.sent.append((topic, key, value))

    def flush(self, timeout):
        return 0


@pytest.fixture
def r(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(h, "get_redis", lambda: client)
    client.hset(h.STREAM_META, mapping={"events_max_event_time": CLOCK, "events_total_rows": 10})
    client.hset("rt:prod:100", mapping={"category_id": 7, "category_code": "electronics.smartphone",
                                        "brand": "apple", "price": 999.5})
    return client


def test_recent_activity_newest_first_with_attributes(r):
    r.zadd(rk.user_recent(1), {rk.encode_recent(100, "view", CLOCK_EPOCH - 60): CLOCK_EPOCH - 60,
                               rk.encode_recent(200, "cart", CLOCK_EPOCH - 10): CLOCK_EPOCH - 10})
    df = h.recent_activity(1)
    assert list(df["product_id"]) == [200, 100]
    assert list(df["event"]) == ["cart", "view"]
    assert df.iloc[1]["brand"] == "apple" and df.iloc[1]["price"] == 999.5


def test_demo_event_time_follows_the_stream_clock_not_the_wall_clock(r):
    assert int(h.demo_event_time(1).timestamp()) == CLOCK_EPOCH + 1
    # A user whose newest event is after the clock (late batch) gets a time after that event.
    r.zadd(rk.user_recent(2), {rk.encode_recent(100, "view", CLOCK_EPOCH + 30): CLOCK_EPOCH + 30})
    assert int(h.demo_event_time(2).timestamp()) == CLOCK_EPOCH + 31


def test_send_event_uses_the_kafka_contract(r, monkeypatch):
    producer = FakeProducer()
    monkeypatch.setattr(h, "get_producer", lambda: producer)
    event = h.send_event(5, 100, "cart")
    topic, key, value = producer.sent[0]
    assert topic == h.KAFKA_TOPIC and key == "5"
    parsed = Event.from_json(value)
    assert parsed == event
    raw = json.loads(value)
    assert raw["event_time"] == "2019-10-01T06:06:58Z"
    assert (raw["category_id"], raw["brand"], raw["price"]) == (7, "apple", 999.5)


def test_wait_for_event_sees_the_processed_event(r):
    event = Event(event_time=datetime(2019, 10, 1, 6, 6, 58, tzinfo=timezone.utc), event_type="view",
                  product_id=100, category_id=7, category_code=None, brand=None, price=1.0,
                  user_id=9, user_session="demo-9")
    assert h.wait_for_event(9, event, timeout=0.6) is None
    epoch = int(event.event_time.timestamp())
    r.zadd(rk.user_recent(9), {rk.encode_recent(100, "view", epoch): epoch})
    assert h.wait_for_event(9, event, timeout=2) is not None      # no rec:rt for this user: done


def test_wait_for_event_waits_for_rec_rt(r):
    """user:recent is written before rec:rt in the same micro-batch: wait for both."""
    event = Event(event_time=datetime(2019, 10, 1, 7, 59, 23, tzinfo=timezone.utc), event_type="view",
                  product_id=100, category_id=7, category_code=None, brand=None, price=1.0,
                  user_id=9, user_session="demo-9")
    epoch = int(event.event_time.timestamp())
    r.zadd(rk.rec_rt(9), {"5": 1.0})
    r.set("rt:rect:9", epoch - 3600)                                  # rec:rt still from an older event
    r.zadd(rk.user_recent(9), {rk.encode_recent(100, "view", epoch): epoch})
    assert h.wait_for_event(9, event, timeout=0.6) is None
    r.set("rt:rect:9", epoch)                                         # rec:rt rewritten for the event
    assert h.wait_for_event(9, event, timeout=2) is not None
