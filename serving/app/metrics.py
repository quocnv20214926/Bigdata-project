"""Prometheus metrics of the API (week 7, owner C).

Exposed at GET /metrics, scraped by Prometheus (infra/monitoring/prometheus.yml).

  api_requests_total{route, method, status}          counter
  api_request_duration_seconds{route}                histogram (p50/p95/p99 in Grafana)
  api_recommend_requests_total{batch, realtime}      counter: which layers a user had
  api_recommended_items_total{source}                counter: batch / realtime / both / trending / popular

The image runs uvicorn with 2 worker processes. Each process has its own
counters, so with PROMETHEUS_MULTIPROC_DIR set (as in Docker) the processes
write to files in that folder and /metrics adds them up. Without the variable
(local run, tests) the default in-process registry is used.
"""

from __future__ import annotations

import os

from prometheus_client import (
    CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest, multiprocess,
)

BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)

REQUESTS = Counter("api_requests_total", "HTTP requests", ["route", "method", "status"])
LATENCY = Histogram("api_request_duration_seconds", "Time to answer a request", ["route"], buckets=BUCKETS)
RECOMMEND = Counter("api_recommend_requests_total", "/recommend requests by available layers",
                    ["batch", "realtime"])
ITEMS = Counter("api_recommended_items_total", "Recommended items by source", ["source"])


def observe(route: str, method: str, status: int, seconds: float) -> None:
    REQUESTS.labels(route, method, str(status)).inc()
    LATENCY.labels(route).observe(seconds)


def observe_recommendation(response) -> None:
    RECOMMEND.labels(str(response.batch_available).lower(), str(response.realtime_available).lower()).inc()
    counts: dict[str, int] = {}
    for item in response.items:
        counts[item.source] = counts.get(item.source, 0) + 1
    for source, n in counts.items():
        ITEMS.labels(source).inc(n)


def render() -> tuple[bytes, str]:
    if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return generate_latest(registry), CONTENT_TYPE_LATEST
    return generate_latest(), CONTENT_TYPE_LATEST
