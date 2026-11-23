"""Small concurrent API load test (stdlib only).

Example:
  python serving/loadtest/quick_check.py --base-url http://localhost:8000 --requests 500 --concurrency 20
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.error
import urllib.request


def percentile(values, p):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * p)))
    return ordered[index]


def request_once(url, timeout):
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = json.loads(response.read())
            return (time.perf_counter() - start) * 1000, response.status, len(body.get("items", []))
    except Exception:
        return (time.perf_counter() - start) * 1000, 0, 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--user-id", type=int, default=541312143)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1 or not 1 <= args.k <= 50:
        parser.error("requests/concurrency must be positive and k must be in [1, 50]")
    url = f"{args.base_url.rstrip('/')}/recommend/{args.user_id}?k={args.k}"
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(lambda _: request_once(url, args.timeout), range(args.requests)))
    elapsed = time.perf_counter() - started
    latencies = [x[0] for x in results]
    successes = sum(x[1] == 200 for x in results)
    print(json.dumps({
        "requests": args.requests, "concurrency": args.concurrency, "successes": successes,
        "errors": args.requests - successes, "elapsed_seconds": round(elapsed, 3),
        "throughput_rps": round(successes / elapsed, 2) if elapsed else 0,
        "latency_ms": {"mean": round(statistics.mean(latencies), 2),
                       "p50": round(percentile(latencies, .50), 2),
                       "p95": round(percentile(latencies, .95), 2),
                       "p99": round(percentile(latencies, .99), 2)},
        "returned_items_avg": round(statistics.mean(x[2] for x in results), 2),
    }, indent=2))


if __name__ == "__main__":
    main()
