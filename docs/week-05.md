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
