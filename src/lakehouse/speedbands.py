"""Speed-band snapshots: discovery, parsing, bronze partitioning and idempotent (merge-on-write) upserts.

A snapshot file is named sb_YYYYMMDD_HHMM.csv.gz. The name is the event time: the local Singapore time (SGT, UTC+8)
at which the sampler started the fetch. All timestamps here are naive SGT.

Bronze layout: one object per snapshot and Kafka partition (link_id % P), so each object has exactly one writer
(the consumer that owns that partition) and consumers can run in parallel:
    bronze/speedbands/obs_date=YYYY-MM-DD/obs_hour=HH/snapshot=YYYYMMDDTHHMM-pN.parquet
The file backfill splits snapshots the same way, so both paths produce the same keys.

Idempotency: the object key is a pure function of the event time and the link, and rows are keyed by
(snapshot_ts, link_id). Writing to an object that already exists merges the new rows into the stored ones; the first
stored copy of a key wins, so replaying the same data (a re-run, or at-least-once redelivery) leaves the object
byte-for-byte unchanged.
"""
from __future__ import annotations

import datetime as dt
import gzip
import re
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

SNAPSHOT_RE = re.compile(r"^sb_(\d{8})_(\d{4})\.csv\.gz$")
SOURCE_HEADER = ["LinkID", "SpeedBand", "MinimumSpeed", "MaximumSpeed"]

BRONZE_SCHEMA = pa.schema([
    ("snapshot_ts", pa.timestamp("s")),
    ("link_id", pa.int64()),
    ("speed_band", pa.int16()),
    ("min_speed", pa.int16()),
    ("max_speed", pa.int16()),
    ("_source", pa.string()),
    ("_ingested_at", pa.timestamp("ms")),
])
KEY = ["snapshot_ts", "link_id"]


class SchemaDriftError(ValueError):
    """The source file does not have the columns the pipeline was built for."""


def parse_snapshot_name(name: str) -> dt.datetime:
    m = SNAPSHOT_RE.match(name)
    if not m:
        raise ValueError(f"not a speed-band snapshot name: {name}")
    return dt.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")


def snapshot_id(ts: dt.datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M")


def partition_path(ts: dt.datetime, part: int) -> str:
    return (f"bronze/speedbands/obs_date={ts:%Y-%m-%d}/obs_hour={ts:%H}/"
            f"snapshot={snapshot_id(ts)}-p{int(part)}.parquet")


def split_by_partition(table: pa.Table, partitions: int) -> dict[int, pa.Table]:
    """Rows of a snapshot grouped by Kafka partition (link_id % partitions), as the producer routes them."""
    ids = table["link_id"]
    part = pc.subtract(ids, pc.multiply(pc.divide(ids, partitions), partitions))  # link_id % partitions (ids > 0)
    out = {}
    for p in range(partitions):
        sub = table.filter(pc.equal(part, p))
        if sub.num_rows:
            out[p] = sub
    return out


def is_complete_gzip(path: Path) -> bool:
    """False for a file the sampler is still writing (truncated gzip stream)."""
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1 << 20):
                pass
        return True
    except (EOFError, OSError, zlib.error):
        return False


def discover_snapshots(directory: Path, until: dt.datetime | None = None, since: dt.datetime | None = None,
                       min_age_s: int = 60, now: float | None = None) -> tuple[list[tuple[dt.datetime, Path]], list[str]]:
    """Return ([(event_ts, path)] sorted by event time, [skipped reasons]).

    Files younger than min_age_s, or whose gzip stream is truncated, are skipped: the sampler may still be writing
    them. They are picked up on the next run, which is safe because writes are idempotent.
    """
    now = time.time() if now is None else now
    found, skipped = [], []
    for p in sorted(Path(directory).glob("sb_*.csv.gz")):
        try:
            ts = parse_snapshot_name(p.name)
        except ValueError:
            skipped.append(f"{p.name}: bad name")
            continue
        if (until and ts > until) or (since and ts < since):
            continue
        if now - p.stat().st_mtime < min_age_s:
            skipped.append(f"{p.name}: younger than {min_age_s}s")
            continue
        if not is_complete_gzip(p):
            skipped.append(f"{p.name}: truncated gzip")
            continue
        found.append((ts, p))
    found.sort(key=lambda x: x[0])
    return found, skipped


def read_snapshot(path: Path, ts: dt.datetime | None = None) -> pa.Table:
    """Parse one snapshot with an enforced schema. Returns columns snapshot_ts, link_id, speed_band, min_speed, max_speed."""
    ts = ts or parse_snapshot_name(Path(path).name)
    with gzip.open(path, "rt", newline="") as f:
        header = f.readline().strip().lstrip("﻿").split(",")
    if header != SOURCE_HEADER:
        raise SchemaDriftError(f"{Path(path).name}: header {header} != {SOURCE_HEADER}")
    t = pacsv.read_csv(
        path,
        convert_options=pacsv.ConvertOptions(
            column_types={"LinkID": pa.int64(), "SpeedBand": pa.int16(),
                          "MinimumSpeed": pa.int16(), "MaximumSpeed": pa.int16()},
            strings_can_be_null=True,
        ),
    )
    n = t.num_rows
    return pa.table({
        "snapshot_ts": pa.array([ts] * n, pa.timestamp("s")),
        "link_id": t["LinkID"],
        "speed_band": t["SpeedBand"],
        "min_speed": t["MinimumSpeed"],
        "max_speed": t["MaximumSpeed"],
    })


def dedupe(table: pa.Table) -> pa.Table:
    """Keep the first row (in input order) for each (snapshot_ts, link_id); sort by the key."""
    if table.num_rows == 0:
        return table
    idx = pa.array(range(table.num_rows), pa.int64())
    firsts = (table.select(KEY).append_column("_i", idx)
              .group_by(KEY, use_threads=False).aggregate([("_i", "min")]))
    kept = table.take(firsts["_i_min"])
    return kept.sort_by([("snapshot_ts", "ascending"), ("link_id", "ascending")])


def to_bronze(table: pa.Table, source: str, ingested_at: dt.datetime) -> pa.Table:
    n = table.num_rows
    cols = {c: table[c] for c in ["snapshot_ts", "link_id", "speed_band", "min_speed", "max_speed"]}
    cols["_source"] = pa.array([source] * n, pa.string())
    cols["_ingested_at"] = pa.array([ingested_at] * n, pa.timestamp("ms"))
    return pa.table(cols).cast(BRONZE_SCHEMA)


@dataclass
class UpsertResult:
    path: str
    rows_in: int
    rows_existing: int
    rows_new: int
    rows_after: int
    written: bool


class BronzeSpeedbandWriter:
    def __init__(self, lake, clock=None):
        self.lake = lake
        self.clock = clock or dt.datetime.now

    def upsert(self, ts: dt.datetime, rows: pa.Table, source: str, part: int, partitions: int) -> UpsertResult:
        """Merge rows of one snapshot partition into its bronze object. Existing rows win; nothing is written if
        nothing is new."""
        if rows.num_rows and pc.any(pc.not_equal(rows["snapshot_ts"], pa.scalar(ts, pa.timestamp("s")))).as_py():
            raise ValueError("all rows passed to upsert must belong to the same snapshot")
        if rows.num_rows and set(split_by_partition(rows, partitions)) != {part}:
            raise ValueError(f"all rows passed to upsert must belong to partition {part}")
        rel = partition_path(ts, part)
        incoming = to_bronze(rows, source, self.clock())
        existing = self.lake.read_parquet(rel).cast(BRONZE_SCHEMA) if self.lake.exists(rel) else BRONZE_SCHEMA.empty_table()
        merged = dedupe(pa.concat_tables([existing, incoming]))
        new = merged.num_rows - existing.num_rows
        if new > 0 or not self.lake.exists(rel):
            self.lake.write_parquet(rel, merged)
        return UpsertResult(rel, rows.num_rows, existing.num_rows, new, merged.num_rows, new > 0)


def load_files(settings, lake, until: dt.datetime | None = None, since: dt.datetime | None = None, log=print) -> dict:
    """Backfill bronze straight from snapshot files (no Kafka). Same partitioning and upsert as the stream consumer,
    so running both, or running either twice, gives the same bronze objects."""
    import hashlib
    lake.ensure_bucket()
    snaps, skipped = discover_snapshots(Path(settings.source_dir) / "speedbands", until=until, since=since,
                                        min_age_s=settings.snapshot_min_age_s)
    writer = BronzeSpeedbandWriter(lake)
    files = []
    for ts, path in snaps:
        table = read_snapshot(path, ts)
        res = [writer.upsert(ts, sub, "file", p, settings.partitions)
               for p, sub in split_by_partition(table, settings.partitions).items()]
        files.append({"file": path.name, "snapshot_ts": ts.isoformat(), "bytes": path.stat().st_size,
                      "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "rows_in": table.num_rows,
                      "rows_new_in_bronze": sum(r.rows_new for r in res),
                      "rows_in_bronze": sum(r.rows_after for r in res), "bronze_objects": len(res)})
        log(f"{path.name}: {table.num_rows} rows read, {files[-1]['rows_new_in_bronze']} new in bronze")
    for s in skipped:
        log(f"skipped {s}")
    return {"snapshots": files, "skipped": skipped}
