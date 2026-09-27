import dataclasses
import datetime as dt
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tests"))

from lakehouse import config  # noqa: E402

FIXTURE = REPO / "tests" / "fixtures" / "raw" / "datamall"
# A second copy of (2026-09-05 08:00, link 101), planted in bronze by a raw append that bypasses the MERGE, with a
# different band and a later ingest time. Silver must keep the first copy (band 2) and stay unique.
PLANTED = {"snapshot_ts": dt.datetime(2026, 9, 5, 8, 0), "link_id": 101, "speed_band": 7}


def local_settings(tmp: Path, **kw):
    """Settings for a run against a local-directory lake (no MinIO, no Kafka)."""
    s = config.load(source_dir=FIXTURE, lake_root=str(tmp / "lake"), warehouse=tmp / "warehouse.duckdb",
                    results_dir=tmp / "results", s3_endpoint="", s3_key="", s3_secret="", snapshot_min_age_s=0)
    return dataclasses.replace(s, **kw)


@pytest.fixture
def settings(tmp_path):
    return local_settings(tmp_path)


def plant_duplicate(settings):
    """Append a duplicate key to bronze in a second write, as a buggy or foreign writer could."""
    import pyarrow as pa

    from lakehouse.speedbands import BRONZE_SCHEMA, TABLE, to_bronze
    from lakehouse.storage import Lake
    lake = Lake(settings)
    ts = PLANTED["snapshot_ts"]
    row = pa.table({"snapshot_ts": pa.array([ts], pa.timestamp("us")), "link_id": pa.array([PLANTED["link_id"]], pa.int64()),
                    "speed_band": pa.array([PLANTED["speed_band"]], pa.int16()),
                    "min_speed": pa.array([60], pa.int16()), "max_speed": pa.array([69], pa.int16())})
    lake.write_delta(TABLE, to_bronze(row, "file", dt.datetime(2099, 1, 1), part=101 % 3).cast(BRONZE_SCHEMA),
                     mode="append", metadata={"writer": "test: planted duplicate"})


@pytest.fixture(scope="session")
def pipeline_run(tmp_path_factory):
    """The whole batch pipeline (ingest, profile, dbt build, quality, publish, report) on the fixture, once, with a
    duplicate key planted in bronze after the speed-band backfill."""
    from lakehouse.cli import run_pipeline
    tmp = tmp_path_factory.mktemp("e2e")
    s = local_settings(tmp)
    timings = run_pipeline(s, after_step=lambda name: plant_duplicate(s) if name == "ingest-speedbands" else None)
    return s, timings
