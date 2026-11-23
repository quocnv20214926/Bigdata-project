# API load test (week 6, Pair 2)

`locustfile.py` sends the traffic of a client application to the API: 70 %
`/recommend/{user}`, 15 % `/similar/{product}`, 10 % `/trending/{category}`,
5 % `/trending`. Ids are real ids read from Redis when the test starts (users
with `user:recent`, products with `sim:cooc`, categories with `trend:cat`), plus
10 % unknown users that take the fallback path. Redis must therefore already be
filled by the streaming job.

```bash
pip install locust==2.47.0 redis

# Fixed rate (the plan's target: p95 < 100 ms at 500-1,000 requests/s)
# PowerShell: $env:LOADTEST_CLIENT_RPS=20      bash: export LOADTEST_CLIENT_RPS=20
locust -f serving/loadtest/locustfile.py --host http://localhost:8000 \
    --headless -u 25 -r 50 -t 60s --csv docs/loadtest/rate500 --only-summary
locust -f serving/loadtest/locustfile.py --host http://localhost:8000 \
    --headless -u 50 -r 50 -t 60s --csv docs/loadtest/rate1000 --only-summary

# Closed loop (maximum throughput): unset LOADTEST_CLIENT_RPS, then e.g. -u 50

# Markdown table for the report, and a JSON copy
python -m serving.loadtest.summarize docs/loadtest/rate500 --label "500 requests/s" --json docs/loadtest/rate500.json
```

Clients × `LOADTEST_CLIENT_RPS` = target rate. Locust reports client-side
latency (including the network and Locust's own scheduling); the API also returns
its own time in the `X-Process-Time-Ms` header.

`quick_check.py` is a dependency-free alternative (standard library only) for a
quick sanity check of one endpoint:

```bash
python serving/loadtest/quick_check.py --base-url http://localhost:8000 --requests 500 --concurrency 20
```

Results of week 6 are in [docs/week-06.md](../../docs/week-06.md) and the JSON
files in [docs/loadtest/](../../docs/loadtest/).

**Note on measuring.** Run the API as it runs in Docker (`uvicorn[standard]`,
which brings `uvloop` and `httptools`). With plain `uvicorn` and `--workers 2`,
every response on a kept-alive connection waited about 40 ms (Nagle's algorithm
with delayed acknowledgements), which made the first measurements meaningless.
