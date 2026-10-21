# Week 2 Report: Contracts, Replayer, Mock API

## Pair 1: Batch 

_Written by Pair 1._

## Pair 2: Real-time 

### 1. Weekly goals

- Freeze the shared contracts in `common/` with Pair 1 (event schema, Redis keys, API spec, event weights).
- Event Replayer reading the raw CSV and publishing to Kafka topic `events`.
- FastAPI skeleton implementing the API contract with mock data.

### 2. Completed work

| Task | Files | 
|---|---|
| Contract review with Pair 1; OpenAPI spec exported | `common/`, `docs/openapi.json` |
| Replayer: streaming CSV reader (no full load into memory), malformed rows skipped and counted, filters by time range and event type | `ingestion/replayer.py` (`read_events`) |
| Pacing: `--speed` keeps original gaps between events (e.g. 60× real time), `--max-rate` caps events per second; scheduling relative to the first event so delays do not accumulate | `ingestion/replayer.py` (`Pacer`) |
| Kafka producer (`confluent-kafka`): key = `user_id` (per-user ordering), value = `Event.to_json()`, lz4 compression, batching, back-pressure on full queue, clean flush on Ctrl+C, `--dry-run` | `ingestion/replayer.py` |
| Replayer image and optional `replayer` service (profile `replay`) | `ingestion/Dockerfile`, `infra/compose.stream.yml` |
| Unit tests: parsing, filters, Kafka message contract, pacing, rate cap, limit, inspector report (7 tests) | `ingestion/tests/test_replayer.py` |
| API endpoints `/recommend/{user_id}`, `/similar/{product_id}`, `/trending`, `/trending/{category_id}`, `/health`, validated against `common/models.py`; `k` limited to 1–50 | `serving/app/main.py` |
| Repository pattern: routes depend on a `Repository` interface; `MockRepository` now, `RedisRepository` in week 5 (switch with `API_MODE`) | `serving/app/repository.py` |
| Deterministic mock catalogue (8 REES46-like categories, 120 products); mock covers every fallback case: new user, batch only, real-time only, both | `serving/app/mock_data.py` |
| Latency header `X-Process-Time-Ms` and mode header `X-API-Mode` | `serving/app/main.py` |
| API image (2 uvicorn workers) and `api` service | `serving/Dockerfile`, `infra/compose.stream.yml` |
| Contract tests for the API (6 tests) | `serving/tests/test_mock_api.py` |

### 3. Results / metrics

| Metric | Result |
|---|---|
| Unit tests (`pytest ingestion serving`) | 13 / 13 passed |
| Replayer throughput with `--speed 0` (events/s) | ≈ 8,500 (200,000 events of the October sample in 23.6 s, from a Windows host into Kafka in Docker) |
| Messages in topic `events` after replaying the sample | 200,000 (matches events sent; 22 MB on disk with lz4 compression) |
| Skipped (malformed) rows | 0 of 200,000 |
| All 6 partitions receive data (Kafka UI) | Yes: 32,966 / 33,632 / 32,867 / 33,815 / 33,375 / 33,345 messages (min–max within 2.9%, keys = `user_id` spread evenly) |
| Mock API server-side time per request (`X-Process-Time-Ms`, 20 requests to `/recommend`) | average 1.13 ms, max 4.82 ms (mock data, no Redis; real latency test with load is in week 6) |

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| UI work would be blocked until real data exists | Mock repository returns every response shape the UI must handle, chosen by `user_id % 10` |
| Replaying 42M rows in real time is too slow for development | `--speed` and `--max-rate` options; 200k-row sample file for daily work |
| Events of one user must stay in order for `user:recent` | Kafka key = `user_id`, so all of a user's events go to one partition |

### 5. Plan for next week

- streaming core: parse Kafka JSON, validation, watermark, de-duplication, checkpoint.
- `foreachBatch` sink writing `user:recent` and `user:bought` to Redis.
