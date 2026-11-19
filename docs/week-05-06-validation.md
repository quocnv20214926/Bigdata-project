# Week 5–6 validation log

Date: 2026-11-19

## Checks completed in the implementation environment

| Check | Result |
|---|---|
| Python compile check (`compileall`) | Passed for the repository source |
| Compose YAML parse | Passed; `ui` service and port mapping are present |
| Existing mock API tests | 6 passed |
| Load-test CLI (`python evaluation/load_test.py --help`) | Passed |
| Redis repository behavior against a small in-memory async Redis-compatible stub | Passed: source merge, purchased-item exclusion, cold-start trending fallback, hybrid similarity, category trending, health |
| Real-time sink behavior against a small in-memory Redis-compatible stub | Passed: view/cart weighted propagation, self-item exclusion, zero-weight event ignored |

## Checks not completed

- `fakeredis` and `redis` were not installed in the execution environment, and package installation could not reach the package index. Therefore the new fakeredis-backed Redis repository and streaming sink pytest suites were not executed here.
- Docker was not available in this environment, so Kafka, Spark Structured Streaming, Redis, FastAPI Redis mode, and Streamlit were not started together.
- The load test was not run against a live API. No throughput or latency numbers are claimed.

## Required before merge/demo

1. Install `requirements-dev.txt` and run `python -m pytest streaming/tests serving/tests -q`.
2. Build and start the Compose stack on a machine with Docker Desktop.
3. Publish a test event from Streamlit and verify that `user:recent`, `sim:cooc`, and `rec:rt` change in Redis.
4. Verify that `/recommend/{user_id}` excludes products in `user:bought:{user_id}` and falls back when both personalized sources are empty.
5. Run `evaluation/load_test.py` against Redis mode and record machine specs and output in `docs/week-06-pair2.md`.
