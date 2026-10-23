# batch/: Batch Layer (Pair 1)

Raw REES46 CSV -> clean Parquet -> ALS -> Redis. Code here follows the contracts in `common/`.

| File | Purpose |
|---|---|
| `spark_session.py` | `get_spark()`: local or cluster SparkSession, MinIO over `s3a://` (settings from `common/config.py`) |
| `etl.py` | Raw CSV -> clean Parquet partitioned by `event_date`, with a validation report |
| `split.py` | Time-based split: train 2019-10-01 .. 11-15, test 11-16 .. 11-30 (`load_split()`) |
| `tests/` | Unit tests on a tiny CSV (local Spark, no MinIO needed) |

## Setup

PySpark needs Java 17. From the repository root:

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r batch/requirements.txt pandas pyarrow pytest
export JAVA_HOME=$(/usr/libexec/java_home -v 17)    # macOS
python -m pytest batch/tests -q
```

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
