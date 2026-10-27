"""Airflow DAG `nightly_batch` (Pair 1, week 5): the batch layer, every night at 02:00 UTC.

Each task runs one step of batch/nightly.py inside the Airflow container, Spark in local mode
(infra/airflow/Dockerfile). The steps share their tables under s3a://lake/batch/<ts_nodash>/
(the run's logical time, e.g. 20261028T020000: unique per run, and sorted by time for cleanup),
so a failed task can be retried on its own.

  etl (optional) -> check -> popularity -------------------------> load -> validate -> cleanup
                         \\-> train -> recommend, sim -------------/

Params ("Trigger DAG w/ config" in the UI, or `airflow dags trigger -c '{...}'`):
  as_of       use the events before this date; default 2019-11-16, the first day Pair 2 replays
  train_days  days before as_of to train on, 0 = all; default NIGHTLY_TRAIN_DAYS, 7 in
              infra/compose.batch.yml, since the full data does not fit a laptop's Docker VM
  max_users   rec:batch users, 0 = as many as the Redis budget allows (batch/rec_batch.py);
              default NIGHTLY_MAX_USERS, 50000 in infra/compose.batch.yml
  run_etl     rebuild the clean Parquet from the raw CSVs first (off: the data is static)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"
ARGS = ("--run-id {{ ts_nodash }} --as-of {{ params.as_of }} "
        "--train-days {{ params.train_days }} --max-users {{ params.max_users }}")
SKIP = 99   # exit code that marks a task as skipped


def step(name: str, **kwargs) -> BashOperator:
    return BashOperator(task_id=name, bash_command=f"cd {PROJECT} && python -m batch.nightly {name} " + ARGS, **kwargs)


with DAG(
    dag_id="nightly_batch",
    description="Pair 1 batch layer: ALS, sim:item, popularity -> Redis",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "pair1", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(hours=2)},
    params={
        "as_of": Param("2019-11-16", type="string", format="date"),
        "train_days": Param(int(os.getenv("NIGHTLY_TRAIN_DAYS", "0")), type="integer", minimum=0),
        "max_users": Param(int(os.getenv("NIGHTLY_MAX_USERS", "0")), type="integer", minimum=0),
        "run_etl": Param(False, type="boolean"),
    },
    tags=["pair1", "batch"],
) as dag:
    etl = BashOperator(
        task_id="etl",
        bash_command=('if [ "{{ params.run_etl }}" != "True" ]; then echo "run_etl is off: reuse the clean Parquet"; '
                      f"exit {SKIP}; fi; cd {PROJECT} && python -m batch.etl "
                      "--report data/batch-runs/{{ ts_nodash }}/etl-report.json"),
        skip_on_exit_code=SKIP,
    )
    check = step("check", trigger_rule="none_failed")    # also runs when etl was skipped
    popularity = step("popularity")
    train = step("train")
    recommend = step("recommend")
    sim = step("sim")
    load = step("load")
    validate = step("validate", retries=0)
    cleanup = step("cleanup", retries=0)

    etl >> check >> [popularity, train]
    train >> [recommend, sim]
    [popularity, recommend, sim] >> load >> validate >> cleanup
