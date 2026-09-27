"""Speed-band snapshots: discovery, parsing with row-level quarantine, and the bronze Delta table.

A snapshot file is named sb_YYYYMMDD_HHMM.csv.gz. The name is the event time: the local Singapore time (SGT, UTC+8)
at which the sampler started the fetch. Event times here are naive SGT; _ingested_at is naive UTC.

Bronze is one Delta Lake table, bronze/speedbands, partitioned by event time (obs_date, obs_hour) and by `part`, the
Kafka partition of the link (link_id % P). Rows are keyed by (snapshot_ts, link_id).

Writes. Every write of one (snapshot, part) slice is a Delta MERGE that inserts only the keys the table does not
hold yet and never updates a stored row, so the first writer of a key wins and replaying the same data (a re-run,
at-least-once redelivery, or the file backfill after the stream) changes nothing. Before merging, the writer reads
the slice once: to skip the commit when nothing is new, and to count keys that arrive again with a different
payload (reported, not silently dropped).

Concurrency. The Kafka consumers own disjoint `part` values, so their commits never conflict. The file backfill can
write the same slice as a consumer at the same moment. Delta commits are put-if-absent (see storage.py); if another
writer committed files into the slice after this writer read it, delta-rs rejects the MERGE with CommitFailedError
and the upsert starts again from the new version. Neither writer can overwrite the other's rows.
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import re
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from deltalake.exceptions import CommitFailedError

SNAPSHOT_RE = re.compile(r"^sb_(\d{8})_(\d{4})\.csv\.gz$")
SOURCE_HEADER = ["LinkID", "SpeedBand", "MinimumSpeed", "MaximumSpeed"]
TABLE = "bronze/speedbands"
QUARANTINE = "bronze/_quarantine/speedbands"

PAYLOAD = ["speed_band", "min_speed", "max_speed"]
KEY = ["snapshot_ts", "link_id"]
PARTITION_BY = ["obs_date", "obs_hour", "part"]
BRONZE_SCHEMA = pa.schema([
    ("snapshot_ts", pa.timestamp("us")),
    ("link_id", pa.int64()),
    ("speed_band", pa.int16()),
    ("min_speed", pa.int16()),
    ("max_speed", pa.int16()),
    ("_source", pa.string()),            # 'stream' (Kafka consumer) or 'file' (file backfill)
    ("_ingested_at", pa.timestamp("us")),  # UTC wall clock of the write
    ("_late", pa.bool_()),               # the consumer's watermark verdict; NULL when written by the file backfill
    ("obs_date", pa.date32()),
    ("obs_hour", pa.int16()),
    ("part", pa.int16()),
])
QUARANTINE_SCHEMA = pa.schema([
    ("snapshot_ts", pa.timestamp("us")),
    ("LinkID", pa.string()), ("SpeedBand", pa.string()), ("MinimumSpeed", pa.string()), ("MaximumSpeed", pa.string()),
    ("_raw_line", pa.string()),          # the source line when it could not be split into the four columns
    ("_reject", pa.string()),
    ("snapshot", pa.string()),
])


class SchemaDriftError(ValueError):
    """The source file does not have the columns the pipeline was built for."""


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def parse_snapshot_name(name: str) -> dt.datetime:
    m = SNAPSHOT_RE.match(name)
    if not m:
        raise ValueError(f"not a speed-band snapshot name: {name}")
    return dt.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")


def snapshot_id(ts: dt.datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M")


def slice_label(ts: dt.datetime, part: int) -> str:
    """Human-readable name of one (snapshot, part) slice of the bronze table."""
    return f"obs_date={ts:%Y-%m-%d}/obs_hour={ts:%H}/part={int(part)}/snapshot={snapshot_id(ts)}"


def partition_of(link_ids, partitions: int):
    return pc.subtract(link_ids, pc.multiply(pc.divide(link_ids, partitions), partitions))  # link_id % P (ids > 0)


def split_by_partition(table: pa.Table, partitions: int) -> dict[int, pa.Table]:
    """Rows of a snapshot grouped by Kafka partition (link_id % partitions), as the producer routes them."""
    part = partition_of(table["link_id"], partitions)
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


# -- parsing ----------------------------------------------------------------------------------------------------------
_INT = r"^\s*-?[0-9]{1,18}\s*$"
_COLUMNS = [("LinkID", "link_id", pa.int64(), False), ("SpeedBand", "speed_band", pa.int16(), False),
            ("MinimumSpeed", "min_speed", pa.int16(), True), ("MaximumSpeed", "max_speed", pa.int16(), True)]
_INT16 = (-32768, 32767)


@dataclass
class ParsedSnapshot:
    ts: dt.datetime
    table: pa.Table                     # valid rows, typed, in file order (duplicates of a link still present)
    rejects: pa.Table                   # QUARANTINE_SCHEMA
    source_rows: int                    # data rows counted independently with the csv module
    reasons: dict = field(default_factory=dict)


def _count_rows(path: Path) -> int:
    with gzip.open(path, "rt", newline="", encoding="utf-8-sig") as f:
        r = csv.reader(f)
        next(r, None)
        return sum(1 for row in r if row)


def parse_snapshot(path: Path, ts: dt.datetime | None = None) -> ParsedSnapshot:
    """Parse one snapshot. The header must match exactly (otherwise SchemaDriftError: the whole file is refused).
    Rows that cannot be split into four fields, or whose values are not integers in range (LinkID and SpeedBand
    must be present; the speed bounds may be empty, as DataMall publishes for band 0), go to the rejects table with
    the first failing rule; the other rows are returned typed."""
    ts = ts or parse_snapshot_name(Path(path).name)
    with gzip.open(path, "rt", newline="", encoding="utf-8-sig") as f:
        header = f.readline().strip().split(",")
    if header != SOURCE_HEADER:
        raise SchemaDriftError(f"{Path(path).name}: header {header} != {SOURCE_HEADER}")
    malformed = []

    def on_invalid(row):
        malformed.append(row.text)
        return "skip"

    raw = pacsv.read_csv(
        path,
        parse_options=pacsv.ParseOptions(invalid_row_handler=on_invalid),
        convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in SOURCE_HEADER},
                                             strings_can_be_null=False),
    )
    n = raw.num_rows
    reject = pa.nulls(n, pa.string())
    typed = {}
    for src, name, typ, nullable in _COLUMNS:
        s = raw[src]
        empty = pc.equal(pc.utf8_trim_whitespace(s), "")
        fmt = pc.match_substring_regex(s, _INT)
        v = pc.cast(pc.utf8_trim_whitespace(pc.if_else(fmt, s, pa.scalar(None, pa.string()))), pa.int64())
        lo, hi = _INT16 if typ == pa.int16() else (1, 2 ** 62)
        in_range = pc.and_kleene(pc.greater_equal(v, lo), pc.less_equal(v, hi))
        bad = pc.or_(pc.and_(pc.invert(empty), pc.invert(pc.fill_null(in_range, False))),
                     pc.and_(empty, pa.scalar(not nullable)))
        reason = ("missing_or_uncastable_" if not nullable else "uncastable_") + name
        reject = pc.if_else(pc.and_(pc.is_null(reject), bad), pa.scalar(reason), reject)
        typed[name] = pc.cast(pc.if_else(pc.fill_null(in_range, False), v, pa.scalar(None, pa.int64())), typ)
    ok = pc.is_null(reject)
    table = pa.table({"snapshot_ts": pa.array([ts] * n, pa.timestamp("us")), **typed}).filter(ok)
    bad_rows = raw.filter(pc.invert(ok))
    nq = bad_rows.num_rows + len(malformed)
    rejects = pa.table({
        "snapshot_ts": pa.array([ts] * nq, pa.timestamp("us")),
        **{c: pa.concat_arrays([bad_rows[c].combine_chunks().cast(pa.string()), pa.nulls(len(malformed), pa.string())])
           for c in SOURCE_HEADER},
        "_raw_line": pa.array([None] * bad_rows.num_rows + malformed, pa.string()),
        "_reject": pa.concat_arrays([reject.filter(pc.invert(ok)).combine_chunks() if bad_rows.num_rows
                                     else pa.array([], pa.string()),
                                     pa.array(["malformed_row"] * len(malformed), pa.string())]),
        "snapshot": pa.array([snapshot_id(ts)] * nq, pa.string()),
    }, schema=QUARANTINE_SCHEMA)
    reasons = {}
    for r in rejects["_reject"].to_pylist():
        reasons[r] = reasons.get(r, 0) + 1
    return ParsedSnapshot(ts, table, rejects, _count_rows(path), dict(sorted(reasons.items())))


def read_snapshot(path: Path, ts: dt.datetime | None = None) -> pa.Table:
    """Valid rows of one snapshot: columns snapshot_ts, link_id, speed_band, min_speed, max_speed."""
    return parse_snapshot(path, ts).table


# -- dedupe and collisions --------------------------------------------------------------------------------------------
def _same(a, b):
    """Null-aware equality of two arrays."""
    return pc.or_(pc.fill_null(pc.equal(a, b), False), pc.and_(pc.is_null(a), pc.is_null(b)))


def dedupe(table: pa.Table) -> pa.Table:
    """Keep the first row (in input order) for each (snapshot_ts, link_id); sort by the key."""
    return dedupe_counting(table)[0]


def dedupe_counting(table: pa.Table) -> tuple[pa.Table, int]:
    """dedupe(), plus the number of dropped repeats whose payload differs from the kept row."""
    if table.num_rows == 0:
        return table, 0
    idx = pa.array(range(table.num_rows), pa.int64())
    firsts = (table.select(KEY).append_column("_i", idx)
              .group_by(KEY, use_threads=False).aggregate([("_i", "min")]))
    kept = table.take(firsts["_i_min"])
    conflicts = 0
    if kept.num_rows < table.num_rows:
        conflicts = _payload_conflicts(table.append_column("_i", idx), kept.append_column("_i", firsts["_i_min"]),
                                       KEY, exclude_same_row=True)
    return kept.sort_by([("snapshot_ts", "ascending"), ("link_id", "ascending")]), conflicts


def _payload_conflicts(rows: pa.Table, reference: pa.Table, keys: list[str], exclude_same_row: bool = False) -> int:
    """Rows of `rows` whose key is in `reference` with a different payload."""
    cols = keys + PAYLOAD + (["_i"] if exclude_same_row else [])
    j = rows.select(cols).join(reference.select(cols), keys=keys, join_type="inner", right_suffix="_ref",
                               use_threads=False)
    if j.num_rows == 0:
        return 0
    same = _same(j["speed_band"], j["speed_band_ref"])
    for c in PAYLOAD[1:]:
        same = pc.and_(same, _same(j[c], j[c + "_ref"]))
    differs = pc.invert(same)
    if exclude_same_row:
        differs = pc.and_(differs, pc.not_equal(j["_i"], j["_i_ref"]))
    return int(pc.sum(pc.cast(differs, pa.int64())).as_py() or 0)


def to_bronze(table: pa.Table, source: str, ingested_at: dt.datetime, part: int, late: bool | None = None) -> pa.Table:
    n = table.num_rows
    ts = table["snapshot_ts"].cast(pa.timestamp("us"))
    cols = {"snapshot_ts": ts, **{c: table[c] for c in ["link_id", *PAYLOAD]}}
    cols["_source"] = pa.array([source] * n, pa.string())
    cols["_ingested_at"] = pa.array([ingested_at] * n, pa.timestamp("us"))
    cols["_late"] = pa.array([late] * n, pa.bool_())
    cols["obs_date"] = pc.cast(ts, pa.date32())
    cols["obs_hour"] = pc.cast(pc.hour(ts), pa.int16())
    cols["part"] = pa.array([part] * n, pa.int16())
    return pa.table(cols).cast(BRONZE_SCHEMA)


# -- writer -----------------------------------------------------------------------------------------------------------
@dataclass
class UpsertResult:
    path: str
    rows_in: int
    rows_existing: int
    rows_new: int
    rows_after: int
    written: bool
    collisions_payload_differs: int = 0   # repeats of a key with a different payload (first writer kept)
    retries: int = 0                      # MERGE attempts rejected because another writer committed into the slice


class BronzeSpeedbandWriter:
    max_retries = 20

    def __init__(self, lake, clock=None):
        self.lake = lake
        self.clock = clock or utcnow
        self.table = None
        self.before_commit = lambda ts, part: None   # test hook: runs between reading the slice and the MERGE

    def _slice(self, ts: dt.datetime, part: int) -> pa.Table:
        f = ((pc.field("obs_date") == pa.scalar(ts.date(), pa.date32()))
             & (pc.field("obs_hour") == pa.scalar(ts.hour, pa.int16()))
             & (pc.field("part") == pa.scalar(part, pa.int16()))
             & (pc.field("snapshot_ts") == pa.scalar(ts, pa.timestamp("us"))))
        return self.table.to_pyarrow_dataset().to_table(filter=f, columns=["snapshot_ts", "link_id", *PAYLOAD])

    def upsert(self, ts: dt.datetime, rows: pa.Table, source: str, part: int, partitions: int,
               late: bool | None = None) -> UpsertResult:
        """Insert the rows of one snapshot partition that bronze does not hold yet. Stored rows are never changed;
        nothing is committed if nothing is new."""
        if rows.num_rows and pc.any(pc.not_equal(rows["snapshot_ts"].cast(pa.timestamp("us")),
                                                 pa.scalar(ts, pa.timestamp("us")))).as_py():
            raise ValueError("all rows passed to upsert must belong to the same snapshot")
        if rows.num_rows and set(split_by_partition(rows, partitions)) != {part}:
            raise ValueError(f"all rows passed to upsert must belong to partition {part}")
        rows = rows.set_column(0, "snapshot_ts", rows["snapshot_ts"].cast(pa.timestamp("us")))
        incoming, in_batch = dedupe_counting(rows)
        if self.table is None:
            self.table = self.lake.ensure_delta(TABLE, BRONZE_SCHEMA, PARTITION_BY)
        pred = (f"t.obs_date = cast('{ts:%Y-%m-%d}' as date) and t.obs_hour = {ts.hour} and t.part = {int(part)} "
                "and t.snapshot_ts = s.snapshot_ts and t.link_id = s.link_id")
        for attempt in range(self.max_retries + 1):
            self.table.update_incremental()
            existing = self._slice(ts, part)
            new = incoming.join(existing.select(["link_id"]), keys="link_id", join_type="left anti",
                                use_threads=False).sort_by("link_id")
            collisions = in_batch + _payload_conflicts(incoming, existing, ["snapshot_ts", "link_id"])
            if new.num_rows == 0:
                return UpsertResult(slice_label(ts, part), rows.num_rows, existing.num_rows, 0, existing.num_rows,
                                    False, collisions, attempt)
            self.before_commit(ts, part)
            try:
                m = (self.table.merge(to_bronze(new, source, self.clock(), part, late), predicate=pred,
                                      source_alias="s", target_alias="t",
                                      commit_properties=_props(source, ts, part))
                     .when_not_matched_insert_all().execute())
            except CommitFailedError:
                time.sleep(min(0.05 * (attempt + 1), 1.0))
                continue
            inserted = int(m["num_target_rows_inserted"])
            return UpsertResult(slice_label(ts, part), rows.num_rows, existing.num_rows, inserted,
                                existing.num_rows + inserted, True, collisions, attempt)
        raise RuntimeError(f"{slice_label(ts, part)}: {self.max_retries + 1} MERGE attempts conflicted")


def _props(source, ts, part):
    from deltalake import CommitProperties
    return CommitProperties(custom_metadata={"writer": source, "snapshot": snapshot_id(ts), "part": str(part)})


def write_quarantine(lake, parsed: ParsedSnapshot) -> None:
    """Replace this snapshot's quarantined rows in one commit (also when there are none now but were before)."""
    t = lake.delta(QUARANTINE)
    if parsed.rejects.num_rows == 0:
        if t is None:
            return
        held = t.to_pyarrow_dataset().count_rows(filter=pc.field("snapshot") == snapshot_id(parsed.ts))
        if held == 0:
            return
    lake.write_delta(QUARANTINE, parsed.rejects, mode="overwrite", partition_by=["snapshot"],
                     predicate=f"snapshot = '{snapshot_id(parsed.ts)}'",
                     metadata={"rows": parsed.rejects.num_rows})


def load_files(settings, lake, until: dt.datetime | None = None, since: dt.datetime | None = None, log=print) -> dict:
    """Backfill bronze straight from snapshot files (no Kafka). Same partitioning and MERGE as the stream consumer,
    so running both, or running either twice, gives the same bronze rows."""
    import hashlib
    lake.ensure_bucket()
    snaps, skipped = discover_snapshots(Path(settings.source_dir) / "speedbands", until=until, since=since,
                                        min_age_s=settings.snapshot_min_age_s)
    writer = BronzeSpeedbandWriter(lake)
    files = []
    for ts, path in snaps:
        parsed = parse_snapshot(path, ts)
        write_quarantine(lake, parsed)
        res = [writer.upsert(ts, sub, "file", p, settings.partitions)
               for p, sub in split_by_partition(parsed.table, settings.partitions).items()]
        distinct = sum(r.rows_after for r in res)
        files.append({"file": path.name, "snapshot_ts": ts.isoformat(), "bytes": path.stat().st_size,
                      "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                      "source_rows": parsed.source_rows, "rows_in": parsed.table.num_rows,
                      "rows_quarantined": parsed.rejects.num_rows, "quarantine_reasons": parsed.reasons,
                      "duplicate_rows_in_file": parsed.table.num_rows - dedupe(parsed.table).num_rows,
                      "collisions_payload_differs": sum(r.collisions_payload_differs for r in res),
                      "rows_new_in_bronze": sum(r.rows_new for r in res), "rows_in_bronze": distinct,
                      "merge_retries": sum(r.retries for r in res), "bronze_slices": len(res)})
        log(f"{path.name}: {parsed.source_rows} rows, {parsed.rejects.num_rows} quarantined, "
            f"{files[-1]['rows_new_in_bronze']} new in bronze")
    for s in skipped:
        log(f"skipped {s}")
    return {"snapshots": files, "skipped": skipped}

