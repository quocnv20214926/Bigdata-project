# Week 05

## Pair 1: Batch

### 1. Weekly goals

- Redis bulk loader for the batch keys (`rec:batch`, `sim:item`, `pop:global`, `pop:cat`, `meta:batch:last_run`) within the week 4 budget.
- Airflow DAG `nightly_batch` that runs the whole batch layer every night.

### 2. Completed work

| Task | Files |
|---|---|
| Redis bulk loader: one ZSET per user, product or category, written by each Spark partition in batches of 200 keys. A batch is one MULTI/EXEC of DEL + ZADD, so the API never reads a half-written list. Scores rounded to 4 decimals, size checked against the budget before anything is written, keys missing from the new tables removed with SCAN + UNLINK, `meta:batch:last_run` written last | `batch/redis_loader.py` |
| The nightly batch as 8 steps, each one command that also runs by hand: check, popularity, train, recommend, sim, load, validate, cleanup. The steps share their tables under `s3a://lake/batch/<run-id>/` and only use events before `as_of` (default 2019-11-16, the first day Pair 2 replays) | `batch/nightly.py` |
| `sim:item` from the Item-kNN table, the week 4 decision (`--sim-source als` keeps the ALS one for the ablation) | `batch/nightly.py`, `batch/item_knn.py` |
| `pop:global` and `pop:cat` stay at 100 products when `batch_top_n` changes (PR #8 makes it 50 for `rec:batch`) | `batch/nightly.py` |
| DAG `nightly_batch`: every day at 02:00 UTC, no catch-up, one run at a time, one retry per task, optional ETL; params `as_of`, `train_days`, `max_users`, `run_etl` | `batch/dags/nightly_batch.py` |
| Airflow image with Java 17, PySpark and the s3a jars; Spark runs in local mode inside the Airflow container. The standalone Spark cluster moved to the `spark-cluster` profile: a laptop's Docker VM has no room for it | `infra/airflow/Dockerfile`, `infra/compose.batch.yml` |
| Run summaries | `docs/nightly-week05-host.json`, `docs/nightly-week05-airflow.json` |
| Unit tests (6 new, 40 in total): the loader against a real Redis (rounding, replacing, stale keys, budget), every nightly step on tiny data, the DAG wiring inside the Airflow image | `batch/tests/` |

### 3. Results / metrics

**Full nightly run on the laptop** (Spark local, 14 GB driver). Run `20261027T191004`, `as_of` 2019-11-16: all 74,971,956 events from 2019-10-01 to 11-15, 4,205,076 users. ALS rank 128, regParam 0.1, alpha 5.

| Step | Result | Time |
|---|---|---|
| check | 46 days of events, the last one 2019-11-15 | 8 s |
| popularity | 191,909 products in 659 categories | 12 s |
| train | ALS on 38,195,148 pairs (2,890,519 users, 155,423 products) | 1,359 s |
| recommend | top-100 for the 566,441 most recently active ALS users (last event on or after 2019-11-13 23:12) | 981 s |
| sim | Item-kNN: 148,414 products, 5,408,264 neighbours, mean similarity 0.072 | 170 s |
| load | 715,515 batch keys, then `meta:batch:last_run` | 97 s |
| validate | every family present, Redis at 952.0 MB of 2,048 MB | 3 s |
| cleanup | kept the 2 newest run folders | 0.4 s |

ALS training and scoring take 39 minutes, most of the run. The first pass of this run filled `sim:item` from the ALS factors (34 s); after the week 4 comparison the sim, load and validate steps were run again on the same run folder with Item-kNN. The loader then removed the 7,009 `sim:item` keys of products that have no Item-kNN neighbours.

**What Redis holds now:**

| Key | Keys | Members |
|---|---|---|
| `rec:batch:{user_id}` | 566,441 | 56,644,100 |
| `sim:item:{product_id}` | 148,414 | 5,408,264 |
| `pop:cat:{category_id}` | 659 | 43,482 |
| `pop:global` | 1 | 100 |
| `meta:batch:last_run` | 1 | time of the last load (UTC) |

Only batch keys are in the database, so its `used_memory` (952.0 MB) is the batch layer's share, under the 1 GiB budget of week 4.

**The DAG in Airflow** (Docker Desktop, 7.6 GB VM, Spark driver 4 GB). With the Docker defaults (last 7 days, 50,000 users) the run of 2026-10-26 02:00 succeeded on all 9 tasks in 4 min 44 s:

| Task | Result | Time |
|---|---|---|
| etl | skipped (`run_etl` off, the data is static) | 0 s |
| check | 19,093,006 events from 2019-11-09 to 11-15 | 6 s |
| popularity | 156,958 products | 7 s |
| train | ALS on 9,524,920 pairs | 123 s |
| recommend | top-100 for 50,000 users | 50 s |
| sim | 103,597 products | 27 s |
| load | 154,246 batch keys | 21 s |
| validate, cleanup | | under 1 s |

This run still used the earlier defaults (ALS rank 64, alpha 10, `sim:item` from ALS factors); the image build took 211 s. Afterwards the DAG was paused and the container stopped, so its next 02:00 run does not replace the full load above.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| The official Airflow image has no Java and no s3a jars for Spark | Own image (`infra/airflow/Dockerfile`): Java 17 JRE, PySpark, jars downloaded once at build time |
| A 14 GB Spark driver does not fit the 7.6 GB Docker VM | The full run uses the same commands on the host; in Docker the DAG defaults to 7 days and 50,000 users (`NIGHTLY_TRAIN_DAYS`, `NIGHTLY_MAX_USERS`) |
| A float32 similarity rounded in Spark read back from Redis as 0.9122999906539917, losing the memory saving | Cast to double before rounding (`batch/redis_loader.py`) |
| Two runs on the same day would share a run folder (`ds_nodash`) | Run id = logical time (`ts_nodash`, e.g. `20261027T020000`): unique, and sorting by name sorts by time for cleanup |
| An Airflow container left running replaces the full host load with the 7-day demo at 02:00 | DAG paused after the demo; `batch/README.md` says to stop the container |

### 5. Plan for next week

- Spark optimisation of the nightly job: ALS training and scoring take 39 minutes, so try fewer iterations, a larger block size for scoring, and caching the matrix between steps.
- Batch demo script: a user's history next to their `rec:batch` and the `sim:item` of their last products.
- When PR #8 (contract 1.2.0) is merged, the next run writes top-50 for 1.08 million users instead of top-100 for 566,441.

## Pair 2: Real-time

### 1. Weekly goals

- **C:** the API reads real Redis data (`API_MODE=redis`) with the fallback logic of the plan (section 3.3).
- **D:** `rec:rt`: real-time scores propagated through `sim:cooc` with time decay.
- Deliverable: API serving real-time recommendations.

### 2. Completed work

| Task | Files | Owner |
|---|---|---|
| `rec:rt` sink: for each event, `rt[j] += w_e · sim(i, j) · exp(−λ·Δt)` over the top 20 neighbours of `sim:cooc:{i}`, half-life 30 min, using `common.scoring.propagate` / `time_decay`. Scores are kept decayed to the user's newest event (`rt:rect:{user}`), top 100, TTL 6 h, rewritten atomically; late events add an already decayed increment | `streaming/sinks/realtime.py` | D |
| `rec:rt` runs after `sim:cooc` in the same micro-batch, so a new co-view is used at once; covered by the batch-id guard (not idempotent) | `streaming/main.py` | D |
| Product attributes for the API (`rt:prod:{product}`: category, brand, price; TTL 30 days) | `streaming/sinks/products.py` | C |
| `RedisRepository`: `rec:batch` + `rec:rt` merged with `common.scoring.merge_scores` (alpha 0.5, `API_ALPHA`), purchased products removed, short lists filled from `trend:cat` of the last viewed category → `trend:global` → `pop:global`, attributes added | `serving/app/redis_repository.py` | C |
| `/similar` from `sim:cooc` and `sim:item` (`cooc`, `item` or `hybrid`), `/trending` with `pop:*` fallback, `/health` with `meta:batch:last_run` | `serving/app/redis_repository.py` | C |
| Reads `user:recent` / `user:bought` both as ZSET (contract 1.1.0) and LIST / SET (1.0.0), so it works whichever version is on `master` | `serving/app/redis_repository.py` | C |
| `503` with a clear message when Redis is unreachable | `serving/app/main.py` | C |
| `API_MODE=redis` is the default in Docker | `infra/compose.stream.yml`, `.env.example` | C |
| Tests: 5 new sink tests (propagation, decay over one half-life, late events, top N, attributes), 9 API tests on fakeredis (merge, bought filter, fallback chain, contract 1.0 types, similar, trending, health, HTTP 503), end-to-end test extended with `rec:rt` | `streaming/tests/`, `serving/tests/test_redis_api.py` | C, D |

### 3. Results / metrics

Tests: 41 / 41 pass (`streaming`, `serving`, `ingestion`) on PySpark 3.5.3 and 4.1.3.

**Run on real data.** October sample (200,000 events, 2019-10-01 00:00–06:06 UTC), 4 micro-batches of 50,000 events, local Spark on 2 cores, then the API in `redis` mode on the same Redis. There is no `rec:batch` yet (Pair 1 loads it into its own stack), so `/recommend` shows the real-time path and the fallback.

| Metric | Result |
|---|---|
| Users with `rec:rt` | 35,712 of 38,440 active users (92.9 %), 36.5 products on average |
| Users without `rec:rt` | their products have no `sim:cooc` neighbours yet: served from `trend:cat` of the last viewed category |
| Time per 50,000-event micro-batch | 14–25 s for the `events` query on a loaded 2-core machine (the unchanged `trending` query slowed down by the same amount) |
| Redis memory, all Pair 2 keys of the sample | 128 MB (79 MB in week 4) |
| Example: user 516407514 viewed smartphones 1005100 and 1004750 | `/recommend` returns five smartphones (vivo, oppo), `source` = `realtime`, with brand and price |
| `/similar/1005115` | `cooc`: 1005105 (0.308), 1004249, 1003317, all Apple smartphones |
| `/recommend` latency, 2,000 sequential requests on random users | server p50 2.1 ms, p95 3.3 ms, p99 4.2 ms; client p50 3.1 ms, p95 4.5 ms |

The same checks on the Docker stack (Kafka source instead of a file) are repeated in the end-to-end run of week 8.

### 4. Issues & resolutions

| Issue | Resolution |
|---|---|
| Decaying every `rec:rt` score on each read would cost a write per request | Scores are stored decayed to the user's newest event; a uniform decay does not change the ranking, and the merge min-max normalises |
| Contract 1.1.0 (ZSET user state) is still on a Pair 1 branch, `master` says LIST / SET | The API reads both types |
| Fallback products come from sources with different scales (weighted counts, popularity) | Filled products are scored by position, strictly below the lowest merged score |
| `/similar` first showed min-max normalised values (always 1.0 for the first product) | Raw similarity when there is one source; normalised merge only for `hybrid` |
| The local `.env` set `API_MODE=mock`, which overrides the compose default | `API_MODE=redis` in `.env.example` and the local `.env` |
| `rec:batch` is not in the Pair 2 stack, so the batch + real-time merge is only covered by tests | Measured with both layers in week 8 (integration) |

### 5. Plan for next week

- **C:** Streamlit pages 1–2: recommendation demo (recent activity, recommendations with their source, "simulate viewing product X" sending an event to Kafka) and trending / similar products.
- **D:** load test with Locust: p50 / p95 / p99 of `/recommend` at 500–1,000 requests per second.
