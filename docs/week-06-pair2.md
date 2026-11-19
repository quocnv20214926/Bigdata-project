# Week 06 — Pair 2: interactive demo and serving load test

## 1. Goals

- Demonstrate the real-time recommendation flow through a browser UI.
- Publish synthetic user events into the same Kafka topic and event schema used by the replayer.
- Show personalized recommendations, similar products, trending products, and service health.
- Provide a repeatable API load-test script and report template.

## 2. Implementation

| Work item | Implementation |
|---|---|
| Streamlit application | `ui/app.py` |
| Event simulation | UI sends `view`, `cart`, or `purchase` JSON events to Kafka topic `events` |
| Recommendation refresh | UI calls `/recommend/{user_id}` and shows source availability, result list, and request latency |
| Additional views | UI exposes similar-product, trending, and health endpoints |
| Container integration | `ui/Dockerfile` and `ui` service in `infra/compose.stream.yml`, port 8501 |
| API load test | `evaluation/load_test.py` uses concurrent standard-library HTTP requests and reports throughput, p50, p95, p99, and errors |

## 3. Run the demo

From the repository root:

```bash
docker compose -f infra/compose.base.yml -f infra/compose.stream.yml up -d --build
```

Open:
- Streamlit: http://localhost:8501
- FastAPI docs: http://localhost:8000/docs
- Kafka UI: http://localhost:8085

The default `API_MODE` for the Pair 2 compose stack is `redis`. Use `API_MODE=mock` in `.env` only when intentionally demonstrating the mock API. To publish a simulated event, enter a user/product/category ID and choose an event type. Wait for the next streaming micro-batch, then refresh recommendations.

## 4. Load test

With the API and Redis running:

```bash
python evaluation/load_test.py --base-url http://localhost:8000 --requests 500 --concurrency 20 --k 10
```

The script prints JSON with request count, success/error count, throughput, mean latency, p50, p95, p99, and average number of returned items.

## 5. Results

No performance numbers are hard-coded in this report. Run the test on the target machine and append the actual JSON output, including machine specs, Docker memory limit, Redis dataset size, API mode, concurrency, and request count. A local mock-mode result is not a valid measurement of Redis-backed serving performance.

## 6. Limitations / next steps

- Streamlit event simulation uses synthetic event payloads; it does not change the source dataset.
- Product metadata is not yet part of the Redis contract, so Redis-mode recommendations display product IDs and scores only.
- This demo does not add authentication or expose Kafka to an untrusted network.
- Week 7 monitoring (Prometheus/Grafana) remains separate work.
