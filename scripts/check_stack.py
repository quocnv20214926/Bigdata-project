"""End-to-end check of the running system (week 8, Pair 2).

Run from the repository root while the Docker stack is up:

    python scripts/check_stack.py                 # all checks
    python scripts/check_stack.py --user 516407514

Checks, in the order data flows: Kafka topic -> streaming job progress ->
Redis keys of both pairs -> API (health, recommend with both layers, fallback,
similar, trending) -> UI, Prometheus, Grafana. Prints one line per check and a
summary; exit code 1 if a required check fails. Only needs `redis`
(pip install redis); everything else is HTTP.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import redis

OK, WARN, FAIL = "ok", "warn", "FAIL"
results: list[tuple[str, str, str]] = []


def report(status: str, name: str, detail: str) -> None:
    results.append((status, name, detail))
    print(f"[{status:>4}] {name:<34} {detail}")


def http_json(url: str, timeout: float = 5.0):
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        body = resp.read()
        ms = (time.perf_counter() - started) * 1000
        return resp.status, (json.loads(body) if body[:1] in (b"{", b"[") else body.decode()), ms


def http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False


def count(r: redis.Redis, pattern: str, limit: int = 1000) -> int:
    n = 0
    for _ in r.scan_iter(match=pattern, count=1000):
        n += 1
        if n >= limit:
            break
    return n


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default=os.getenv("API_URL", "http://localhost:8000"))
    p.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    p.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    p.add_argument("--user", type=int, default=None, help="user to query (default: one with rec:rt and rec:batch)")
    a = p.parse_args()
    api = a.api.rstrip("/")

    # ---- Redis and the streaming job -------------------------------------------------
    r = redis.Redis(host=a.redis_host, port=a.redis_port, decode_responses=True, socket_timeout=5)
    try:
        r.ping()
        report(OK, "redis", f"{a.redis_host}:{a.redis_port}, {r.dbsize():,} keys, "
                            f"{r.info('memory')['used_memory_human']}")
    except redis.RedisError as exc:
        report(FAIL, "redis", str(exc))
        return summary()

    meta = r.hgetall("rt:meta:stream")
    if not meta:
        report(FAIL, "streaming job progress", "rt:meta:stream missing: is the streaming container running?")
    else:
        at = meta.get("events_processed_at")
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(at.replace("Z", "+00:00"))).total_seconds() if at else None
        status = OK if age is not None and age < 120 else WARN
        report(status, "streaming job progress",
               f"{int(meta.get('events_total_rows', 0)):,} events, last batch {meta.get('events_batch_rows')} rows "
               f"in {meta.get('events_batch_ms')} ms, {age:.0f} s ago, stream clock {meta.get('events_max_event_time')}"
               if age is not None else str(meta))

    speed = {k: count(r, f"{k}:*") for k in ("user:recent", "rec:rt", "sim:cooc", "trend:cat")}
    speed["trend:global"] = int(r.exists("trend:global"))
    report(OK if all(speed.values()) else FAIL, "speed layer keys (Pair 2)",
           ", ".join(f"{k} {'1000+' if v >= 1000 else v}" for k, v in speed.items()))
    batch = {k: count(r, f"{k}:*") for k in ("rec:batch", "sim:item")}
    batch["pop:global"] = int(r.exists("pop:global"))
    last_run = r.get("meta:batch:last_run")
    report(OK if all(batch.values()) else WARN, "batch layer keys (Pair 1)",
           ", ".join(f"{k} {'1000+' if v >= 1000 else v}" for k, v in batch.items())
           + f", last run {last_run or 'never'}"
           + ("" if all(batch.values()) else "  (run the nightly batch: the API then serves real-time + trending)"))

    # ---- API ---------------------------------------------------------------------------
    try:
        _, health, ms = http_json(f"{api}/health")
        report(OK if health.get("status") == "ok" else FAIL, "api /health", f"{health} ({ms:.0f} ms)")
    except (urllib.error.URLError, OSError) as exc:
        report(FAIL, "api /health", f"{api}: {exc}")
        return summary()

    user = a.user
    if user is None:                         # prefer a user with both layers
        for key in r.scan_iter(match="rec:rt:*", count=1000):
            uid = int(key.rsplit(":", 1)[1])
            if r.exists(f"rec:batch:{uid}"):
                user = uid
                break
            user = user or uid
    if user is not None:
        _, rec, ms = http_json(f"{api}/recommend/{user}?k=10")
        sources = sorted({i["source"] for i in rec["items"]})
        both = rec["batch_available"] and rec["realtime_available"]
        report(OK if rec["items"] else FAIL, "api /recommend (known user)",
               f"user {user}: {len(rec['items'])} items, sources {sources}, batch={rec['batch_available']} "
               f"realtime={rec['realtime_available']} ({ms:.0f} ms)" + ("" if both else "  (not both layers)"))
    _, rec, ms = http_json(f"{api}/recommend/1999999999?k=10")
    report(OK if rec["items"] and {i["source"] for i in rec["items"]} <= {"trending", "popular"} else FAIL,
           "api /recommend (unknown user)", f"{len(rec['items'])} items from {sorted({i['source'] for i in rec['items']})} "
                                           f"({ms:.0f} ms)")
    _, tr, ms = http_json(f"{api}/trending?k=5")
    report(OK if tr["items"] else WARN, "api /trending", f"{len(tr['items'])} items, source "
           f"{tr['items'][0]['source'] if tr['items'] else '-'} ({ms:.0f} ms)")
    if tr["items"]:
        pid = tr["items"][0]["product_id"]
        _, sim, ms = http_json(f"{api}/similar/{pid}?k=5")
        report(OK if sim["items"] else WARN, "api /similar", f"product {pid}: {len(sim['items'])} items, "
               f"similarity {sim['similarity']} ({ms:.0f} ms)")
    status, metrics, _ = http_json(f"{api}/metrics")
    report(OK if "api_requests_total" in str(metrics) else WARN, "api /metrics", "Prometheus metrics exposed")

    # ---- UI and monitoring --------------------------------------------------------------
    for name, url in (("streamlit ui", "http://localhost:8501/_stcore/health"),
                      ("prometheus", "http://localhost:9090/-/ready"),
                      ("grafana", "http://localhost:3000/api/health"),
                      ("kafka ui", "http://localhost:8085/")):
        report(OK if http_ok(url) else WARN, name, url)
    try:
        _, targets, _ = http_json("http://localhost:9090/api/v1/targets")
        states = {t["labels"]["job"]: t["health"] for t in targets["data"]["activeTargets"]}
        report(OK if states and all(v == "up" for v in states.values()) else WARN, "prometheus targets", str(states))
    except (urllib.error.URLError, OSError, KeyError):
        pass
    return summary()


def summary() -> int:
    fails = [n for s, n, _ in results if s == FAIL]
    warns = [n for s, n, _ in results if s == WARN]
    print(f"\n{len(results) - len(fails) - len(warns)} ok, {len(warns)} warnings, {len(fails)} failures")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
