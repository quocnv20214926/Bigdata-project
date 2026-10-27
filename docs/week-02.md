# Week 2 Report: Contracts, Replayer, Mock API

## Pair 1: Batch 

### 1. Weekly goals

- Freeze the shared contracts in `common/` with Pair 2.
- EDA on the raw data, to choose cleaning rules and ALS filtering thresholds.
- ETL from the raw CSV to clean Parquet on MinIO, and the time-based train/test split.

### 2. Completed work

| Task | Files |
|---|---|
| Batch services: Spark master/worker, Airflow (standalone), MinIO bucket `lake` (profile `lake`); spark-master healthcheck fixed (the master binds to the container IP, not localhost) | `infra/compose.batch.yml` |
| EDA on full October 2019: size and sparsity, funnel, daily activity, missing values, prices, long tail, user segments, ALS filter thresholds | `notebooks/01_eda.ipynb` |
| SparkSession factory: local or cluster, MinIO over `s3a://`, session time zone UTC | `batch/spark_session.py` |
| ETL: raw CSV to Parquet partitioned by `event_date`; invalid rows dropped and counted by reason; empty strings to null; exact duplicates removed in the same shuffle as the date partitioning; re-running replaces only the dates in the input | `batch/etl.py` |
| Time-based split (train Oct 1 to Nov 15, test Nov 16 to Nov 30) as a filter on the partition column, no data copy | `batch/split.py` |
| Validation report for both months | `docs/etl-report.json` |
| Unit tests on a tiny CSV with every edge case (4 tests) | `batch/tests/test_etl.py` |

### 3. Results / metrics

| Metric | Result |
|---|---|
| Rows read (Oct + Nov) | 109,950,743; none dropped as invalid |
| Exact duplicates removed | 130,739 |
| Clean rows / users / products | 109,820,004 / 5,316,649 / 206,876 |
| ETL time (write to MinIO) | 296.5 s on a laptop, 366.1 s with the validation report |
| Train split | 74,971,956 rows, 4,205,076 users, 191,909 products |
| Test split | 34,848,048 rows, 2,438,946 users, 174,010 products |
| Test users not seen in train | 1,111,573 (45.6%): cold start is common, so the popularity fallback matters |
| Test buyers / buyers not seen in train | 302,610 / 87,440 |
| Sparsity (October) | 3,022,290 users x 166,794 products, 99.9954% empty |
| Event mix (October) | view 96.07%, cart 2.18%, purchase 1.75% |
| ALS filter chosen | product >= 5 users, then user >= 2 products: keeps 76.7% of products, 67.2% of users, 95.4% of user-product pairs (October) |
| User segments for week 7 | new <= 2 products (46.6% of users), light 3 to 10 (34.2%), heavy > 10 (19.2%) |

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| More users purchase than add to cart (347,118 vs 337,117 in October) | ETL and scoring never assume a cart event before a purchase |
| `category_code` empty in 31.8% and `brand` in 14.4% of rows | Kept as null; `pop:cat` uses `category_id`, which is always present |
| 0.16% of events have price 0 | Kept (ALS does not use price); excluded only from price statistics and the UI |
| Times shown in the machine's local time zone | Spark session time zone set to UTC; the EDA prints UTC |

### 5. Plan for next week

- Interaction matrix (summed event weights) with the ALS filter.
- Metric functions, Popularity baseline, first implicit ALS model and its comparison with the baseline.

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
