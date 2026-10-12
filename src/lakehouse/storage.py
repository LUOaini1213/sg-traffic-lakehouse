"""Object-store access for the lake.

Every bronze and gold dataset is a Delta Lake table written with the `deltalake` package (delta-rs) and read by
DuckDB's `delta` extension (`delta_scan`). A Delta commit is one new file, _delta_log/<version>.json, written with a
put-if-absent (on S3/MinIO: a conditional PUT with If-None-Match; on a local disk: an atomic rename that fails if the
target exists). Of two writers that race for the same version exactly one wins; the other re-reads the log, checks
whether the winner touched the data it read, and either commits at the next version or fails with a
CommitFailedError. Readers only ever see whole commits.

Paths inside the lake are "<layer>/<dataset>" relative to LAKE_ROOT (s3://bucket[/prefix] or a local directory).
"""
from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
from deltalake import CommitProperties, DeltaTable, write_deltalake

from .config import Settings


class Lake:
    """A thin wrapper so every writer and reader resolves lake paths and credentials the same way."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._gold_snapshot = None
        root = settings.lake_root
        if settings.is_s3:
            self.fs = pafs.S3FileSystem(
                endpoint_override=settings.s3_endpoint or None,
                scheme="http" if settings.s3_endpoint else "https",
                access_key=settings.s3_key or None,
                secret_key=settings.s3_secret or None,
                region="us-east-1",
                allow_bucket_creation=True,
            )
            self.base = root[len("s3://"):]
        else:
            self.fs = pafs.LocalFileSystem()
            self.base = str(Path(root).resolve())
            Path(self.base).mkdir(parents=True, exist_ok=True)

    # -- paths -------------------------------------------------------------------------------------------------------
    def path(self, rel: str) -> str:
        # Logical gold aliases are bound once per Lake reader. Related queries
        # cannot silently switch cohorts when a new publication commits.
        from .publication import TABLES
        if rel in {f"gold/{table}" for table in TABLES}:
            table = rel.split("/", 1)[1]
            snapshot = self.gold_snapshot()
            snapshot.delta(table)
            rel = snapshot.tables[table]["path"]
        return f"{self.base}/{rel.lstrip('/')}"

    def gold_snapshot(self):
        from .publication import read_snapshot
        if self._gold_snapshot is None:
            self._gold_snapshot = read_snapshot(self)
        return self._gold_snapshot

    def refresh_gold_snapshot(self):
        """Explicitly start a new coherent read; an existing snapshot stays valid."""
        from .publication import read_snapshot
        self._gold_snapshot = read_snapshot(self)
        return self._gold_snapshot

    def uri(self, rel: str) -> str:
        """URI that DuckDB and delta-rs understand (s3://... or an absolute local path)."""
        if self.settings.is_s3:
            return f"s3://{self.path(rel)}"
        return self.path(rel)

    @property
    def storage_options(self) -> dict | None:
        s = self.settings
        if not s.is_s3:
            return None
        opts = {"AWS_REGION": "us-east-1"}
        if s.s3_endpoint:
            opts.update(AWS_ENDPOINT_URL=f"http://{s.s3_endpoint}", AWS_ALLOW_HTTP="true")
        if s.s3_key:
            opts.update(AWS_ACCESS_KEY_ID=s.s3_key, AWS_SECRET_ACCESS_KEY=s.s3_secret)
        return opts

    # -- bucket / files ----------------------------------------------------------------------------------------------
    def ensure_bucket(self) -> None:
        if self.settings.is_s3:
            bucket = self.base.split("/", 1)[0]
            if self.fs.get_file_info(bucket).type == pafs.FileType.NotFound:
                self.fs.create_dir(bucket)

    def exists(self, rel: str) -> bool:
        return self.fs.get_file_info(self.path(rel)).type == pafs.FileType.File

    def delete_prefix(self, rel_prefix: str) -> None:
        """Remove a whole dataset. Used only to start a benchmark or a reproducibility run from empty storage."""
        info = self.fs.get_file_info(self.path(rel_prefix))
        if info.type == pafs.FileType.Directory:
            self.fs.delete_dir(self.path(rel_prefix))

    def sha256_file(self, full_path: str) -> str:
        h = hashlib.sha256()
        with self.fs.open_input_stream(full_path) as f:
            while chunk := f.read(1 << 20):
                h.update(chunk)
        return h.hexdigest()

    # -- Delta tables ------------------------------------------------------------------------------------------------
    def delta(self, rel: str) -> DeltaTable | None:
        """The table at its latest version, or None if there is no Delta table at rel."""
        uri = self.uri(rel)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            return None
        return DeltaTable(uri, storage_options=self.storage_options)

    def ensure_delta(self, rel: str, schema: pa.Schema, partition_by: list[str] | None = None) -> DeltaTable:
        """Create an empty table if there is none. Two processes may race here; the commit of version 0 is a
        put-if-absent, so one of them fails, and that one simply opens the table the other created."""
        t = self.delta(rel)
        if t is not None:
            return t
        try:
            DeltaTable.create(self.uri(rel), schema=schema, partition_by=partition_by, mode="ignore",
                              storage_options=self.storage_options)
        except Exception:
            if self.delta(rel) is None:
                raise
        return self.delta(rel)

    def write_delta(self, rel: str, data, *, mode: str, predicate: str | None = None,
                    partition_by: list[str] | None = None, metadata: dict | None = None,
                    schema_mode: str | None = None) -> None:
        """One Delta commit. mode='overwrite' with a predicate atomically replaces exactly the rows matching it
        (replaceWhere): readers see either the old rows or the new ones, never an empty or half-written partition."""
        from .publication import TABLES
        if rel in {f"gold/{table}" for table in TABLES}:
            raise ValueError("Write gold through a complete publication, not a logical read alias")
        if rel.startswith("gold/releases/") and (mode != "error" or self.delta(rel) is not None):
            raise ValueError("Gold release tables are immutable")
        write_deltalake(self.uri(rel), data, mode=mode, predicate=predicate, partition_by=partition_by,
                        storage_options=self.storage_options, schema_mode=schema_mode,
                        commit_properties=CommitProperties(custom_metadata={k: str(v) for k, v in (metadata or {}).items()}))

    def delta_stats(self, rel: str) -> dict | None:
        """Version, number of data files and rows of the current version (from the Delta log, no data read)."""
        t = self.delta(rel)
        if t is None:
            return None
        adds = pa.table(t.get_add_actions(flatten=True))
        return {"version": t.version(), "files": adds.num_rows,
                "rows": int(sum(v or 0 for v in adds["num_records"].to_pylist())) if adds.num_rows else 0}

    def delta_file_hashes(self, rel: str) -> list[str]:
        """SHA-256 of every data file of the current version, sorted (file names contain random UUIDs)."""
        return sorted(self.sha256_file(uri.removeprefix("s3://").removeprefix("file://"))
                      for uri in self.delta(rel).file_uris())

    def scan(self, rel: str) -> str:
        """SQL table expression for DuckDB."""
        return f"delta_scan('{self.uri(rel)}')"


def _load_ext(con, name: str) -> None:
    try:
        con.execute(f"LOAD {name}")
    except (duckdb.IOException, duckdb.CatalogException):  # not cached yet (fresh machine or CI runner)
        con.execute(f"INSTALL {name}")
        con.execute(f"LOAD {name}")


def duckdb_connect(settings: Settings, database: str | Path = ":memory:", read_only: bool = False):
    con = duckdb.connect(str(database), read_only=read_only)
    con.execute(f"SET memory_limit='{settings.duckdb_memory}'")
    con.execute(f"SET threads={int(settings.duckdb_threads)}")
    con.execute(f"SET temp_directory='{tempfile.gettempdir()}/duckdb_spill'")
    _load_ext(con, "delta")
    if settings.is_s3:
        _load_ext(con, "httpfs")
        q = lambda v: "'" + str(v).replace("'", "''") + "'"  # noqa: E731  (CREATE SECRET takes no parameters)
        con.execute(
            f"CREATE OR REPLACE SECRET lake (TYPE s3, KEY_ID {q(settings.s3_key)}, SECRET {q(settings.s3_secret)}, "
            f"ENDPOINT {q(settings.s3_endpoint)}, URL_STYLE 'path', USE_SSL {str(not settings.s3_endpoint).lower()}, "
            "REGION 'us-east-1')"
        )
    return con
