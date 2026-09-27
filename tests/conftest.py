import dataclasses
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from lakehouse import config  # noqa: E402

FIXTURE = REPO / "tests" / "fixtures" / "raw" / "datamall"


def local_settings(tmp: Path, **kw):
    """Settings for a run against a local-directory lake (no MinIO, no Kafka)."""
    s = config.load(source_dir=FIXTURE, lake_root=str(tmp / "lake"), warehouse=tmp / "warehouse.duckdb",
                    results_dir=tmp / "results", s3_endpoint="", s3_key="", s3_secret="", snapshot_min_age_s=0)
    return dataclasses.replace(s, **kw)


@pytest.fixture
def settings(tmp_path):
    return local_settings(tmp_path)


@pytest.fixture(scope="session")
def pipeline_run(tmp_path_factory):
    """The whole batch pipeline (ingest, profile, dbt build, quality, publish, report) on the fixture, once."""
    from lakehouse.cli import run_pipeline
    tmp = tmp_path_factory.mktemp("e2e")
    s = local_settings(tmp)
    timings = run_pipeline(s)
    return s, timings
