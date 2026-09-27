"""Batch pipeline DAG: ingest -> profile -> dbt build (models + tests) -> quality checks -> publish metrics.

Each task calls the same CLI that CI and `make pipeline` use (python -m lakehouse <command>), in a separate
virtualenv inside the Airflow image so dbt's dependencies stay apart from Airflow's. Speed-band bronze is also fed by
the Kafka consumer; the ingest_speedbands task backfills any snapshot the stream missed. Both write with a Delta
MERGE that only inserts new keys, so the backfill never duplicates or overwrites what the consumer wrote, even when
both run at the same time.

Snapshot cutoff: the run's data_interval_end (for a manual run without a schedule, its logical date), converted to
Singapore time by the CLI. A run therefore always sees the same set of snapshot files, however late it starts, and
a re-run of the same interval sees the same input. There is no free-text parameter.

Retries: ingest, dbt and publish retry once (they are idempotent). The strict quality gate does not retry: its
result depends only on the data, so a retry would just repeat the failure and delay the alert.

Schedule: manual by default (a local demo). Set SGTL_SCHEDULE, e.g. "0 6 * * *", to run it daily.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PY = os.environ.get("PIPELINE_PYTHON", "python")
CLI = f"cd /opt/project && {PY} -m lakehouse"
CUTOFF = "--cutoff-utc {{ (data_interval_end or dag_run.run_after).isoformat() }}"

with DAG(
    dag_id="sg_traffic_lakehouse",
    description="LTA DataMall batch ingest, dbt build and data-quality gate on the local lakehouse",
    schedule=os.environ.get("SGTL_SCHEDULE") or None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["lakehouse", "dbt", "datamall", "delta"],
) as dag:
    ingest_batch = BashOperator(task_id="ingest_batch", bash_command=f"{CLI} ingest-batch")
    ingest_speedbands = BashOperator(task_id="ingest_speedbands", bash_command=f"{CLI} ingest-speedbands {CUTOFF}")
    profile = BashOperator(task_id="profile_bronze", bash_command=f"{CLI} profile")
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{CLI} dbt build")
    quality = BashOperator(task_id="quality_checks", bash_command=f"{CLI} quality --strict {CUTOFF}", retries=0)
    publish = BashOperator(task_id="publish_metrics", bash_command=f"{CLI} publish && {CLI} report")

    # Sequential rather than parallel ingest: run side by side, the OD load (DuckDB) and the snapshot backfill
    # exceeded the container's memory limit on this laptop (Errno 12 in the first attempts).
    ingest_batch >> ingest_speedbands >> profile >> dbt_build >> quality >> publish
