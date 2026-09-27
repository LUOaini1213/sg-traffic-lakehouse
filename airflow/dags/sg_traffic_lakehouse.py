"""Batch pipeline DAG: ingest -> profile -> dbt build (models + tests) -> quality checks -> publish metrics.

Each task calls the same CLI that CI and `make pipeline` use (python -m lakehouse <command>), in a separate
virtualenv inside the Airflow image so dbt's dependencies stay apart from Airflow's. Speed-band bronze is also fed by
the Kafka consumer; the ingest_speedbands task backfills any snapshot the stream missed, and because bronze writes
are idempotent it never duplicates what the consumer already wrote.

Schedule: manual by default (a local demo). Set SGTL_SCHEDULE, e.g. "0 6 * * *", to run it daily.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PY = os.environ.get("PIPELINE_PYTHON", "python")
CLI = f"cd /opt/project && {PY} -m lakehouse"
UNTIL = "{{ ('--until ' ~ params.until) if params.until else '' }}"

with DAG(
    dag_id="sg_traffic_lakehouse",
    description="LTA DataMall batch ingest, dbt build and data-quality gate on the local lakehouse",
    schedule=os.environ.get("SGTL_SCHEDULE") or None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"until": Param("", type="string",
                           description="Use speed-band snapshots up to YYYYmmddHHMM (empty: all complete files)")},
    tags=["lakehouse", "dbt", "datamall"],
) as dag:
    ingest_batch = BashOperator(task_id="ingest_batch", bash_command=f"{CLI} ingest-batch")
    ingest_speedbands = BashOperator(task_id="ingest_speedbands", bash_command=f"{CLI} ingest-speedbands {UNTIL}")
    profile = BashOperator(task_id="profile_bronze", bash_command=f"{CLI} profile")
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{CLI} dbt build")
    quality = BashOperator(task_id="quality_checks", bash_command=f"{CLI} quality --strict")
    publish = BashOperator(task_id="publish_metrics", bash_command=f"{CLI} publish && {CLI} report")

    # Sequential rather than parallel ingest: run side by side, the OD load (DuckDB) and the snapshot backfill
    # exceeded the container's memory limit on this laptop (Errno 12 in the first attempts).
    ingest_batch >> ingest_speedbands >> profile >> dbt_build >> quality >> publish
