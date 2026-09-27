"""Object-store access for the lake: pyarrow filesystems for writing, DuckDB (httpfs) for reading.

Paths inside the lake are always written as "<layer>/<dataset>/<partition>/<file>" relative to LAKE_ROOT.
"""
from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from .config import Settings

PARQUET_OPTS = dict(compression="zstd", use_dictionary=True, write_statistics=True)


class Lake:
    """A thin wrapper so every writer and reader resolves lake paths the same way."""

    def __init__(self, settings: Settings):
        self.settings = settings
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
        return f"{self.base}/{rel.lstrip('/')}"

    def uri(self, rel: str) -> str:
        """URI that DuckDB understands (s3://... or an absolute local path)."""
        if self.settings.is_s3:
            return f"s3://{self.path(rel)}"
        return self.path(rel)

    # -- bucket / files ----------------------------------------------------------------------------------------------
    def ensure_bucket(self) -> None:
        if self.settings.is_s3:
            bucket = self.base.split("/", 1)[0]
            if self.fs.get_file_info(bucket).type == pafs.FileType.NotFound:
                self.fs.create_dir(bucket)

    def exists(self, rel: str) -> bool:
        return self.fs.get_file_info(self.path(rel)).type == pafs.FileType.File

    def list_files(self, rel_prefix: str, suffix: str = ".parquet") -> list[str]:
        sel = pafs.FileSelector(self.path(rel_prefix), recursive=True, allow_not_found=True)
        out = [i.path for i in self.fs.get_file_info(sel) if i.type == pafs.FileType.File and i.path.endswith(suffix)]
        return sorted(p[len(self.base) + 1:] for p in out)

    def delete_prefix(self, rel_prefix: str) -> None:
        info = self.fs.get_file_info(self.path(rel_prefix))
        if info.type == pafs.FileType.Directory:
            self.fs.delete_dir(self.path(rel_prefix))

    def write_parquet(self, rel: str, table: pa.Table) -> None:
        parent = self.path(rel).rsplit("/", 1)[0]
        self.fs.create_dir(parent, recursive=True)
        pq.write_table(table, self.path(rel), filesystem=self.fs, **PARQUET_OPTS)

    def read_parquet(self, rel: str) -> pa.Table:
        return pq.read_table(self.path(rel), filesystem=self.fs)

    def num_rows(self, rel: str) -> int:
        with self.fs.open_input_file(self.path(rel)) as f:
            return pq.ParquetFile(f).metadata.num_rows

    def sha256(self, rel: str) -> str:
        h = hashlib.sha256()
        with self.fs.open_input_stream(self.path(rel)) as f:
            while chunk := f.read(1 << 20):
                h.update(chunk)
        return h.hexdigest()

    def put_bytes(self, rel: str, data: bytes) -> None:
        parent = self.path(rel).rsplit("/", 1)[0]
        self.fs.create_dir(parent, recursive=True)
        with self.fs.open_output_stream(self.path(rel)) as f:
            f.write(data)


def duckdb_connect(settings: Settings, database: str | Path = ":memory:", read_only: bool = False):
    con = duckdb.connect(str(database), read_only=read_only)
    con.execute(f"SET memory_limit='{settings.duckdb_memory}'")
    con.execute(f"SET threads={int(settings.duckdb_threads)}")
    con.execute(f"SET temp_directory='{tempfile.gettempdir()}/duckdb_spill'")
    if settings.is_s3:
        con.execute("LOAD httpfs")
        q = lambda v: "'" + str(v).replace("'", "''") + "'"  # noqa: E731  (CREATE SECRET takes no parameters)
        con.execute(
            f"CREATE OR REPLACE SECRET lake (TYPE s3, KEY_ID {q(settings.s3_key)}, SECRET {q(settings.s3_secret)}, "
            f"ENDPOINT {q(settings.s3_endpoint)}, URL_STYLE 'path', USE_SSL {str(not settings.s3_endpoint).lower()}, "
            "REGION 'us-east-1')"
        )
    return con
