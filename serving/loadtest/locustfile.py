"""Locust load test of the recommendation API (week 6, owner D).

Traffic mix (per simulated client): 70 % /recommend, 15 % /similar, 10 % /trending/{category},
5 % /trending. User and product ids are real ids read from Redis before the test:
active users (with user:recent), so the API does the full merge, plus 10 % unknown
users that take the fallback path.

Two modes:
  - closed loop (default): each client sends its next request as soon as the
    previous one returns, so the test finds the maximum throughput;
  - fixed rate: LOADTEST_CLIENT_RPS=10 makes each client send 10 requests/s, so
    -u 50 targets 500 requests/s and -u 100 targets 1,000 requests/s. This is how
    the p50/p95/p99 of the plan (500-1,000 requests/s) are measured.

Run from the repository root (API and Redis up, Redis filled by the streaming job):

    $env:LOADTEST_CLIENT_RPS=10          # PowerShell; bash: export LOADTEST_CLIENT_RPS=10
    locust -f serving/loadtest/locustfile.py --host http://localhost:8000 \
        --headless -u 50 -r 50 -t 60s --csv docs/loadtest/rate500 --only-summary
    python -m serving.loadtest.summarize docs/loadtest/rate500 --label "500 requests/s"
"""

from __future__ import annotations

import os
import random

import redis
from locust import FastHttpUser, constant_throughput, events, task

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
SAMPLE = int(os.getenv("LOADTEST_SAMPLE", "5000"))
K = int(os.getenv("LOADTEST_K", "10"))
CLIENT_RPS = float(os.getenv("LOADTEST_CLIENT_RPS", "0"))   # 0 = closed loop

USERS: list[int] = []
PRODUCTS: list[int] = []
CATEGORIES: list[int] = []


def _ids(r: redis.Redis, pattern: str, limit: int) -> list[int]:
    out = []
    for key in r.scan_iter(match=pattern, count=1000):
        out.append(int(key.rsplit(":", 1)[1]))
        if len(out) >= limit:
            break
    return out


@events.test_start.add_listener
def load_ids(environment, **_):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    USERS[:] = _ids(r, "user:recent:*", SAMPLE)
    PRODUCTS[:] = _ids(r, "sim:cooc:*", SAMPLE) or _ids(r, "rt:prod:*", SAMPLE)
    CATEGORIES[:] = _ids(r, "trend:cat:*", 500)
    if not USERS:
        raise RuntimeError("No user:recent:* keys in Redis: run the streaming job (or the replayer) first")
    print(f"load test ids: {len(USERS)} users, {len(PRODUCTS)} products, {len(CATEGORIES)} categories")


class ApiClient(FastHttpUser):
    if CLIENT_RPS > 0:
        wait_time = constant_throughput(CLIENT_RPS)

    def _user(self) -> int:
        if random.random() < 0.10:
            return random.randint(10**9, 2 * 10**9)          # unknown user: fallback path
        return random.choice(USERS)

    @task(70)
    def recommend(self):
        self.client.get(f"/recommend/{self._user()}?k={K}", name="/recommend/{user_id}")

    @task(15)
    def similar(self):
        if PRODUCTS:
            self.client.get(f"/similar/{random.choice(PRODUCTS)}?k={K}", name="/similar/{product_id}")

    @task(10)
    def trending_category(self):
        if CATEGORIES:
            self.client.get(f"/trending/{random.choice(CATEGORIES)}?k={K}", name="/trending/{category_id}")

    @task(5)
    def trending(self):
        self.client.get(f"/trending?k={K}", name="/trending")
