"""The speed-layer exporter on fakeredis (no Kafka)."""

from __future__ import annotations

import fakeredis
from prometheus_client import CollectorRegistry, generate_latest

from streaming.monitoring.exporter import STREAM_META, KeyCounter, PipelineCollector, prefix_of


def render(r, counter=None) -> str:
    registry = CollectorRegistry()
    registry.register(PipelineCollector(r, consumer=None, key_counter=counter))
    return generate_latest(registry).decode()


def test_metrics_from_stream_meta():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(STREAM_META, mapping={
        "events_total_rows": 199862, "events_batch_id": 3, "events_batch_rows": 50000, "events_batch_ms": 2500,
        "events_processed_at": "2026-11-23T05:03:19Z", "events_max_event_time": "2019-10-01T06:06:57Z",
        "trending_batch_id": 2, "trending_updated_rows": 100, "trending_batch_ms": 800,
        "trending_window_end": "2019-10-01T06:10:00Z",
    })
    r.zadd("rec:rt:1", {"5": 1.0})
    r.zadd("sim:cooc:5", {"6": 0.5})
    r.set("other", 1)
    counter = KeyCounter(r)
    counter.counts = counter.count_once()
    text = render(r, counter)
    assert "stream_exporter_redis_up 1.0" in text
    assert "stream_events_total 199862.0" in text
    assert 'stream_batch_duration_seconds{query="events"} 2.5' in text
    assert 'stream_batch_rows{query="trending"} 100.0' in text
    assert "stream_event_time_seconds 1.569910017e+09" in text
    assert 'redis_keys_by_prefix{prefix="rec:rt"} 1.0' in text
    assert 'redis_keys_by_prefix{prefix="sim:cooc"} 1.0' in text
    assert "redis_keys 4.0" in text


def test_redis_down_reports_up_zero():
    class Down:
        def hgetall(self, *_):
            import redis
            raise redis.ConnectionError("down")
    text = render(Down())
    assert "stream_exporter_redis_up 0.0" in text
    assert "stream_events_total" not in text


def test_prefix_of():
    assert prefix_of("user:recent:1") == "user:recent"
    assert prefix_of("rt:meta:stream") is None
