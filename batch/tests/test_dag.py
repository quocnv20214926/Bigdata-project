"""The nightly_batch DAG loads and wires its tasks as documented (needs Airflow: skipped without it).

Run inside the Airflow container:
  docker exec airflow python -m pytest /opt/project/batch/tests/test_dag.py -q -p no:cacheprovider
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


def test_nightly_batch_dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("nightly_batch")
    assert set(dag.task_ids) == {"etl", "check", "popularity", "train", "recommend", "sim", "load", "validate", "cleanup"}
    up = lambda t: dag.get_task(t).upstream_task_ids
    assert up("check") == {"etl"} and dag.get_task("check").trigger_rule == "none_failed"
    assert up("popularity") == {"check"} and up("train") == {"check"}
    assert up("recommend") == {"train"} and up("sim") == {"train"}
    assert up("load") == {"popularity", "recommend", "sim"}
    assert up("validate") == {"load"} and up("cleanup") == {"validate"}
    assert dag.params["as_of"] == "2019-11-16" and dag.catchup is False
    assert dag.params["train_days"] == int(os.getenv("NIGHTLY_TRAIN_DAYS", "0"))
    assert dag.params["max_users"] == int(os.getenv("NIGHTLY_MAX_USERS", "0"))
    assert "python -m batch.nightly train --run-id {{ ts_nodash }}" in dag.get_task("train").bash_command
