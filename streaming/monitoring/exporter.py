"""Prometheus exporter of the speed layer (week 7, owner C).

Spark does not commit offsets to Kafka and the streaming job keeps its progress
in Redis (rt:meta:stream, written after every micro-batch by streaming/main.py),
so one small exporter publishes everything the Grafana dashboard needs about the
pipeline:

  stream_events_total                     events processed by the events query (after dedup)
  stream_batch_id{query}                  last micro-batch id
  stream_batch_rows{query}                rows of the last micro-batch
  stream_batch_duration_seconds{query}    duration of the last micro-batch
  stream_last_batch_timestamp_seconds{query}   wall-clock time of the last micro-batch
  stream_event_time_seconds               newest event time processed (the stream clock)
  stream_trending_window_end_seconds      end of the last published trending window
  kafka_topic_end_offset{topic,partition} messages written to each partition (the input)
  redis_used_memory_bytes, redis_keys, redis_connected_clients, redis_ops_per_second
  redis_keys_by_prefix{prefix}            counted every KEY_COUNT_SECONDS in a background thread

Run: python -m streaming.monitoring.exporter   (port EXPORTER_PORT, default 9108)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime

import redis
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from prometheus_client.registry import Collector

log = logging.getLogger("exporter")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_EVENTS", "events")
PORT = int(os.getenv("EXPORTER_PORT", "9108"))
KEY_COUNT_SECONDS = int(os.getenv("KEY_COUNT_SECONDS", "60"))

STREAM_META = "rt:meta:stream"
PREFIXES = ["user:recent", "user:bought", "rec:rt", "sim:cooc", "trend:cat", "rt:prod",
            "rec:batch", "sim:item", "pop:cat"]
QUERIES = ["events", "trending"]


def _iso_epoch(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _num(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def prefix_of(key: str) -> str | None:
    for p in PREFIXES:
        if key.startswith(p + ":"):
            return p
    return None


class KeyCounter(threading.Thread):
    """Counts keys per prefix with SCAN (never KEYS), off the scrape path."""

    def __init__(self, r: redis.Redis):
        super().__init__(daemon=True)
        self.r = r
        self.counts: dict[str, int] = {}

    def count_once(self) -> dict[str, int]:
        counts = {p: 0 for p in PREFIXES}
        for key in self.r.scan_iter(count=5000):
            p = prefix_of(key)
            if p:
                counts[p] += 1
        return counts

    def run(self) -> None:
        while True:
            try:
                self.counts = self.count_once()
            except redis.RedisError as exc:
                log.warning("key count failed: %s", exc)
            time.sleep(KEY_COUNT_SECONDS)


class PipelineCollector(Collector):
    def __init__(self, r: redis.Redis, consumer=None, key_counter: KeyCounter | None = None):
        self.r = r
        self.consumer = consumer
        self.key_counter = key_counter

    # -- Kafka ------------------------------------------------------------------
    def kafka_offsets(self) -> dict[int, int]:
        if self.consumer is None:
            return {}
        from confluent_kafka import TopicPartition
        meta = self.consumer.list_topics(TOPIC, timeout=2)
        topic = meta.topics.get(TOPIC)
        if topic is None or topic.error is not None:
            return {}
        out = {}
        for pid in topic.partitions:
            _, high = self.consumer.get_watermark_offsets(TopicPartition(TOPIC, pid), timeout=2, cached=False)
            out[pid] = high
        return out

    # -- collect ------------------------------------------------------------------
    def collect(self):
        up = GaugeMetricFamily("stream_exporter_redis_up", "1 if Redis answered the last scrape")
        try:
            meta = self.r.hgetall(STREAM_META)
            dbsize = self.r.dbsize()
            up.add_metric([], 1)
        except redis.RedisError:
            up.add_metric([], 0)
            yield up
            return
        yield up
        try:
            info = self.r.info()
        except redis.RedisError:            # INFO can be disabled (rename-command): skip those gauges
            info = {}

        total = GaugeMetricFamily("stream_events_total", "Events processed by the events query (after dedup)")
        total.add_metric([], _num(meta.get("events_total_rows")) or 0)
        yield total

        gauges = {
            "batch_id": GaugeMetricFamily("stream_batch_id", "Last micro-batch id", labels=["query"]),
            "rows": GaugeMetricFamily("stream_batch_rows", "Rows in the last micro-batch", labels=["query"]),
            "ms": GaugeMetricFamily("stream_batch_duration_seconds", "Duration of the last micro-batch",
                                    labels=["query"]),
            "at": GaugeMetricFamily("stream_last_batch_timestamp_seconds",
                                    "Wall-clock time of the last micro-batch", labels=["query"]),
        }
        for q in QUERIES:
            rows_field = "events_batch_rows" if q == "events" else "trending_updated_rows"
            values = {
                "batch_id": _num(meta.get(f"{q}_batch_id")),
                "rows": _num(meta.get(rows_field)),
                "ms": (_num(meta.get(f"{q}_batch_ms")) or 0) / 1000 if meta.get(f"{q}_batch_ms") else None,
                "at": _iso_epoch(meta.get(f"{q}_processed_at")),
            }
            for name, v in values.items():
                if v is not None:
                    gauges[name].add_metric([q], v)
        yield from gauges.values()

        clock = _iso_epoch(meta.get("events_max_event_time"))
        if clock is not None:
            g = GaugeMetricFamily("stream_event_time_seconds", "Newest event time processed (stream clock)")
            g.add_metric([], clock)
            yield g
        window = _iso_epoch(meta.get("trending_window_end"))
        if window is not None:
            g = GaugeMetricFamily("stream_trending_window_end_seconds", "End of the last published trending window")
            g.add_metric([], window)
            yield g

        try:
            offsets = self.kafka_offsets()
        except Exception as exc:                                # Kafka down: keep the Redis metrics
            log.warning("kafka offsets failed: %s", exc)
            offsets = {}
        if offsets:
            g = GaugeMetricFamily("kafka_topic_end_offset", "Messages written to the partition",
                                  labels=["topic", "partition"])
            for pid, high in sorted(offsets.items()):
                g.add_metric([TOPIC, str(pid)], high)
            yield g

        for name, field, doc in [
            ("redis_used_memory_bytes", "used_memory", "Redis memory in use"),
            ("redis_connected_clients", "connected_clients", "Redis client connections"),
            ("redis_ops_per_second", "instantaneous_ops_per_sec", "Redis commands per second"),
        ]:
            if field not in info:
                continue
            g = GaugeMetricFamily(name, doc)
            g.add_metric([], _num(info.get(field)) or 0)
            yield g
        g = GaugeMetricFamily("redis_keys", "Keys in the Redis database")
        g.add_metric([], dbsize)
        yield g

        if self.key_counter is not None and self.key_counter.counts:
            g = GaugeMetricFamily("redis_keys_by_prefix", "Keys per prefix (refreshed every minute)",
                                  labels=["prefix"])
            for p, n in self.key_counter.counts.items():
                g.add_metric([p], n)
            yield g


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=2, socket_timeout=5)
    consumer = None
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "pair2-exporter",
                             "enable.auto.commit": False})
    except Exception as exc:
        log.warning("no Kafka client, offsets not exported: %s", exc)
    counter = KeyCounter(r)
    counter.start()
    REGISTRY.register(PipelineCollector(r, consumer, counter))
    start_http_server(PORT)
    log.info("exporter on :%d (redis %s:%d, kafka %s, topic %s)", PORT, REDIS_HOST, REDIS_PORT,
             KAFKA_BOOTSTRAP, TOPIC)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
