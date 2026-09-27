"""Runtime settings, read from environment variables only (see .env.example).

LAKE_ROOT is either an S3 URI (s3://bucket/prefix, served locally by MinIO) or a local directory. The same code path
writes to both, so the fixture pipeline in CI and the full Docker stack exercise identical logic.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    source_dir: Path
    lake_root: str
    warehouse: Path
    results_dir: Path
    s3_endpoint: str = ""
    s3_key: str = field(default="", repr=False)
    s3_secret: str = field(default="", repr=False)
    kafka_bootstrap: str = "localhost:29092"
    topic: str = "sg.speedbands.v1"
    partitions: int = 3
    snapshot_min_age_s: int = 60
    duckdb_memory: str = "1500MB"
    duckdb_threads: int = 4

    @property
    def is_s3(self) -> bool:
        return self.lake_root.startswith("s3://")


def load(**overrides) -> Settings:
    env = os.environ
    s = dict(
        source_dir=Path(env.get("SOURCE_DIR", REPO / "tests" / "fixtures" / "raw" / "datamall")),
        lake_root=env.get("LAKE_ROOT", str(REPO / ".lake")).rstrip("/"),
        warehouse=Path(env.get("DUCKDB_PATH", REPO / ".lake" / "warehouse.duckdb")),
        results_dir=Path(env.get("RESULTS_DIR", REPO / "results")),
        s3_endpoint=env.get("S3_ENDPOINT", ""),
        s3_key=env.get("AWS_ACCESS_KEY_ID", ""),
        s3_secret=env.get("AWS_SECRET_ACCESS_KEY", ""),
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        topic=env.get("KAFKA_TOPIC", "sg.speedbands.v1"),
        partitions=int(env.get("KAFKA_PARTITIONS", "3")),
        snapshot_min_age_s=int(env.get("SNAPSHOT_MIN_AGE_S", "60")),
        duckdb_memory=env.get("DUCKDB_MEMORY", "1500MB"),
        duckdb_threads=int(env.get("DUCKDB_THREADS", "4")),
    )
    s.update(overrides)
    return Settings(**s)
