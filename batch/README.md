# batch/: Batch Layer (Pair 1)

Raw REES46 CSV -> clean Parquet -> ALS -> Redis. Code here follows the contracts in `common/`.

| File | Purpose |
|---|---|
| `spark_session.py` | `get_spark()`: local or cluster SparkSession, MinIO over `s3a://` (settings from `common/config.py`) |
| `etl.py` | Raw CSV -> clean Parquet partitioned by `event_date`, with a validation report |
| `split.py` | Time-based split: train 2019-10-01 .. 11-15, test 11-16 .. 11-30 (`load_split()`) |
| `interactions.py` | Interaction matrix: summed event weights per (user, product); ALS filter (product >= 5 users, then user >= 2 products) |
| `popularity.py` | Long-term popularity: Popularity baseline, and the lists for `pop:global` / `pop:cat` |
| `als.py` | Implicit ALS (Spark MLlib) trained on `log(1 + score)`, so its confidence equals `common.weights.als_confidence` |
| `item_knn.py` | Item-kNN baseline: cosine between product vectors over users (`log(1 + score)`), top 50 neighbours per product |
| `sim_item.py` | `sim:item`: exact cosine top-50 between ALS item factors (broadcast factors, numpy block product per task) |
| `rec_batch.py` | Redis budget of the batch keys (1 GiB) and which users get `rec:batch` (most recently active ALS users that fit) |
| `redis_sizing.py` | Measures Redis memory per key on a live Redis; validation-week coverage of top-50 vs top-100 |
| `redis_loader.py` | Bulk loader: `pop:*`, `sim:item`, `rec:batch` from Spark, budget check, stale keys removed, `meta:batch:last_run` last |
| `nightly.py` | The nightly batch as separate steps (check, popularity, train, recommend, sim, load, validate, cleanup) |
| `dags/nightly_batch.py` | Airflow DAG `nightly_batch` running those steps every night at 02:00 UTC |
| `tests/` | Unit tests on tiny data (local Spark, no MinIO needed; the Redis tests use database 15 of a local Redis if there is one) |

## Setup

PySpark needs Java 17. From the repository root:

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r batch/requirements.txt pytest
export JAVA_HOME=$(/usr/libexec/java_home -v 17)    # macOS
python -m pytest batch/tests evaluation/tests -q
# Redis tests: REDIS_TEST_PORT=6379 (default) uses database 15 and skips if it is not empty
```

`java_home -v 17` silently falls back to another version when no Java 17 is registered
(check with `/usr/libexec/java_home -V`). With Homebrew's `openjdk@17` on Apple silicon, use
`export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` instead.
"Java gateway process exited before sending its port number" usually means a wrong `JAVA_HOME`.

## Run the ETL

```bash
# 1. MinIO, Redis, Spark, Airflow
docker compose --profile lake -f infra/compose.base.yml -f infra/compose.batch.yml up -d

# 2. Upload the raw CSVs (data/raw/2019-Oct.csv, 2019-Nov.csv) to s3://lake/raw/
docker run --rm --network recsys_default -v "$PWD/data/raw:/raw:ro" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url http://minio:9000 \
  s3 cp /raw/ s3://lake/raw/ --recursive --exclude "*" --include "*.csv"

# 3. ETL: s3a://lake/raw/ -> s3a://lake/clean/events/ and the validation report
SPARK_DRIVER_MEMORY=14g python -m batch.etl --report docs/etl-report.json

# Without MinIO (local files only)
python -m batch.etl --input data/raw/2019-Oct.csv --output data/clean/events --no-s3
```

The first run downloads the Hadoop AWS jars (about 280 MB) for `s3a://`.

## Cleaning rules

Based on `notebooks/01_eda.ipynb`:

- Drop and count: unparseable `event_time`, unknown `event_type`, missing `product_id` / `user_id`, dates outside `--start` / `--end`.
- Keep with null: empty `category_code`, `brand`, `user_session`.
- Keep `price = 0` (not used by ALS). Do not assume a cart event before a purchase.
- Remove exact duplicate rows.

Output: raw columns with `event_time` as a UTC timestamp, plus `event_date`; one Parquet file per date.
Re-running replaces only the dates present in the input.

## Use the split

```python
from batch.spark_session import get_spark
from batch.split import load_split

spark = get_spark("als")
train, test = load_split(spark)    # reads s3a://lake/clean/events/
```

## Train and evaluate (Popularity, Item-kNN, ALS)

```bash
# MinIO must be running and hold s3a://lake/clean/events/ (see "Run the ETL")
SPARK_DRIVER_MEMORY=14g python -m evaluation.offline --report docs/eval-week03.json

# Faster first run: fewer evaluated users, smaller model
SPARK_DRIVER_MEMORY=14g python -m evaluation.offline --eval-users 20000 --rank 32 --iter 5

# Only some of the models
SPARK_DRIVER_MEMORY=14g python -m evaluation.offline --models popularity item_knn --eval-users 20000
```

The protocol and the metric definitions are in [`evaluation/README.md`](../evaluation/README.md).
`--model-out s3a://lake/models/als/` also saves the model for `sim:item`.

## Tune ALS and build sim:item (week 4)

```bash
# Parameter search on the validation week (fit 10-01..11-08, validate 11-09..11-15), then the best
# parameters retrained on the full train split, evaluated on test and saved for sim:item
SPARK_DRIVER_MEMORY=14g python -m evaluation.tune_als --report docs/eval-week04-tuning.json \
  --final-report docs/eval-week04-als.json --model-out s3a://lake/models/als/week4/

# sim:item from that model, compared with Item-kNN
SPARK_DRIVER_MEMORY=14g python -m evaluation.similarity --model s3a://lake/models/als/week4/ \
  --sim-out s3a://lake/models/sim_item/week4/ --report docs/eval-week04-sim.json

# How big rec:batch can be (Redis from infra/compose.base.yml must be running)
python -m batch.redis_sizing --report docs/redis-sizing-week04.json
```

## Nightly batch and Redis (week 5)

Each step of `batch/nightly.py` is one command. They share their tables under
`s3a://lake/batch/<run-id>/` and only use events before `--as-of` (default 2019-11-16, the first
day Pair 2 replays), so the same commands run by hand on the host and as Airflow tasks:

```bash
run=$(date -u +%Y%m%dT%H%M%S)      # same form as the Airflow run ids, so cleanup keeps the newest
for step in check popularity train recommend sim load validate cleanup; do
  SPARK_DRIVER_MEMORY=14g python -m batch.nightly $step --run-id $run || break
done
```

What lands in Redis (keys from `common/redis_keys.py`, scores rounded to 4 decimals):

| Key | Content | Size |
|---|---|---|
| `rec:batch:{user_id}` | top `batch_top_n` ALS products for the most recently active ALS users | as many users as fit 860 MiB |
| `sim:item:{product_id}` | top 50 neighbours by cosine of the ALS item factors, sim > 0 | about 125 MB |
| `pop:global`, `pop:cat:{category_id}` | top 100 products by summed event weights | a few MB |
| `meta:batch:last_run` | ISO 8601 UTC time of the load, written last | |

The loader refuses to write when a family would pass its budget in `batch/rec_batch.py`: batch
keys have no TTL, and Redis (`volatile-lru`) never evicts them.

### Airflow

```bash
docker compose --profile lake -f infra/compose.base.yml -f infra/compose.batch.yml up -d --build
docker compose -f infra/compose.base.yml -f infra/compose.batch.yml logs airflow | grep -i password
docker exec airflow airflow dags unpause nightly_batch     # starts the latest 02:00 run at once
# or http://localhost:8080 -> nightly_batch -> Trigger DAG w/ config, or:
docker exec airflow airflow dags trigger nightly_batch -c '{"train_days": 14, "max_users": 50000}'
docker exec airflow python -m pytest /opt/project/batch/tests/test_dag.py -q -p no:cacheprovider
```

The image (`infra/airflow/Dockerfile`) adds Java 17, PySpark and the s3a jars to Airflow; Spark runs
in local mode inside the container with `NIGHTLY_DRIVER_MEMORY` (default 4g). A full run needs a
14 GB driver, more than a laptop's Docker VM usually has, so in Docker the DAG defaults to the last
`NIGHTLY_TRAIN_DAYS` (7) days and `NIGHTLY_MAX_USERS` (50000) users. Run the commands above on the
host for the full data, or set `NIGHTLY_TRAIN_DAYS=0 NIGHTLY_MAX_USERS=0 NIGHTLY_DRIVER_MEMORY=14g`
on a machine that has the memory. Stop the container (`docker compose ... stop airflow`) after a
demo, otherwise its 02:00 run replaces what a full host run loaded. Run ids are the logical time
(`ts_nodash`, e.g. `20261028T020000`); task logs are in the Airflow UI and each step's summary is
written to `data/batch-runs/<run-id>/<step>.json`.
