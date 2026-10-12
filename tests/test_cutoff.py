"""A rerun's snapshot cutoff also applies to bronze already populated by the stream."""
import argparse
import datetime as dt
import runpy
import sys
import types
from contextlib import nullcontext

import duckdb
import pytest

from lakehouse import batch_ingest, cli
from lakehouse.speedbands import load_files
from lakehouse.storage import Lake


@pytest.mark.parametrize('option,value', [
    ('--until', '202609050820'),
    ('--cutoff-utc', '2026-09-05T00:20:00+00:00'),
])
def test_dbt_cli_propagates_the_same_sgt_cutoff(settings, monkeypatch, option, value):
    calls = []
    monkeypatch.setattr(cli.config, 'load', lambda: settings)
    monkeypatch.setattr(cli, 'run_dbt', lambda s, args, **kw: calls.append((args, kw)) or 0)
    cli.main(['dbt', option, value, 'build', '--select', 'stg_speedband_obs'])
    assert calls == [(['build', '--select', 'stg_speedband_obs'],
                     {'until': dt.datetime(2026, 9, 5, 8, 20)})]


def test_dbt_cutoff_env_does_not_leak_between_runs(settings, monkeypatch):
    monkeypatch.setenv('SNAPSHOT_CUTOFF_SGT', '2099-01-01 00:00:00')
    assert cli.dbt_env(settings)['SNAPSHOT_CUTOFF_SGT'] == ''
    assert cli.dbt_env(settings, until=dt.datetime(2026, 9, 5, 8, 20))[
        'SNAPSHOT_CUTOFF_SGT'] == '2026-09-05 08:20:00'


def test_airflow_ingest_transform_and_quality_share_one_cutoff(monkeypatch):
    # Inspect the real DAG's constructed task commands without starting Airflow.
    class Task:
        def __init__(self, **kw):
            self.__dict__.update(kw)

        def __rshift__(self, other):
            return other

    for name in ('airflow', 'airflow.providers', 'airflow.providers.standard',
                 'airflow.providers.standard.operators', 'airflow.providers.standard.operators.bash',
                 'airflow.sdk'):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules['airflow.providers.standard.operators.bash'].BashOperator = Task
    sys.modules['airflow.sdk'].DAG = lambda **kw: nullcontext()
    dag = runpy.run_path(str(cli.REPO / 'airflow/dags/sg_traffic_lakehouse.py'))
    cutoff = dag['CUTOFF']
    assert '--cutoff-utc' in cutoff
    assert all(cutoff in dag[task].bash_command for task in ('ingest_speedbands', 'dbt_build', 'quality'))
    # The wrapper's cutoff option precedes dbt's own remainder arguments.
    assert f'dbt {cutoff} build' in dag['dbt_build'].bash_command


def test_cutoff_filters_existing_bronze_before_silver_and_gold(settings):
    # Populate all ten snapshots first, as a streaming consumer or a later run would.
    # Do not hide the defect by limiting file discovery during this test's ingest.
    batch_ingest.run(settings)
    lake = Lake(settings)
    load_files(settings, lake)
    bronze_before = lake.delta_stats('bronze/speedbands')
    assert bronze_before['rows'] == 120

    cli.cmd_dbt(settings, argparse.Namespace(
        until='202609050820', cutoff_utc=None, dbt_args=['build']))
    with duckdb.connect(str(settings.warehouse), read_only=True) as con:
        assert con.execute('select count(*), max(snapshot_ts) from silver.stg_speedband_obs').fetchone() == (
            24, dt.datetime(2026, 9, 5, 8, 20))
        assert con.execute('select count(*) from silver.dim_snapshot').fetchone() == (2,)
        assert con.execute('select sum(n_obs) from gold.gold_link_speed_hourly').fetchone() == (23,)
        assert con.execute('select max(obs_hour_ts) from gold.gold_link_hour_features').fetchone() == (
            dt.datetime(2026, 9, 5, 8),)
    # This is a bounded transformation, not deletion of newer data in the shared lake.
    assert lake.delta_stats('bronze/speedbands') == bronze_before

    # Rebuild without a cutoff in the same warehouse: dbt must not reuse the previous
    # cutoff from its environment or partial-parse cache.
    cli.cmd_dbt(settings, argparse.Namespace(until=None, cutoff_utc=None, dbt_args=['build']))
    with duckdb.connect(str(settings.warehouse), read_only=True) as con:
        assert con.execute('select count(*), max(snapshot_ts) from silver.stg_speedband_obs').fetchone() == (
            120, dt.datetime(2026, 9, 7, 7, 55))
