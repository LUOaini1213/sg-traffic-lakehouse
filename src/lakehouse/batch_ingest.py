"""Batch ingest of the monthly passenger-volume files, the static bus network and the link metadata into bronze.

Schema enforcement, in order:
  1. the header must match the expected column list exactly (a renamed, added or missing column stops the load:
     that is a change of contract, not a bad row);
  2. a line that cannot be split into the expected number of fields is not fatal: DuckDB's `store_rejects` sets it
     aside and it is quarantined as `malformed_row`, with the raw line;
  3. every value is read as text; integer columns must look like an integer (DuckDB's TRY_CAST alone would turn
     '5.5' into 6) and fit the type, and the domain rules below must hold. A row failing any rule is quarantined
     with the first failing rule; the other rows go to bronze.

Bronze and quarantine are Delta tables partitioned by month. A load replaces its month in one commit
(overwrite with predicate month = 'YYYY-MM', i.e. replaceWhere): if the load fails half-way, the month keeps its
previous rows. The data table and the quarantine table are two commits, not one transaction; the reconciliation in
profiling.py (bronze + quarantine = source) detects a load that stopped between them.

Reconciliation totals are counted independently of DuckDB: a Python pass over the raw file (csv module) counts
rows and sums the volume columns and computes the file's SHA-256. The SHA-256 and the counts are also stored in the
Delta commit's metadata, next to the data they describe.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from .storage import Lake, duckdb_connect

DAY_TYPES = ("WEEKDAY", "WEEKENDS/HOLIDAY")
INT_SQL = "-?[0-9]+"
INT_RE = re.compile(INT_SQL)


@dataclass(frozen=True)
class CsvDataset:
    name: str
    pattern: str
    columns: dict           # source column -> DuckDB type (source order)
    key: tuple
    code_columns: tuple     # 5-digit bus stop codes, kept as text (leading zeros)
    volume_columns: tuple


PV_STOP = CsvDataset(
    "pv_stop", r"^transport_node_bus_(\d{4})(\d{2})\.csv$",
    {"YEAR_MONTH": "VARCHAR", "DAY_TYPE": "VARCHAR", "TIME_PER_HOUR": "TINYINT", "PT_TYPE": "VARCHAR",
     "PT_CODE": "VARCHAR", "TOTAL_TAP_IN_VOLUME": "BIGINT", "TOTAL_TAP_OUT_VOLUME": "BIGINT"},
    ("YEAR_MONTH", "DAY_TYPE", "TIME_PER_HOUR", "PT_CODE"), ("PT_CODE",),
    ("TOTAL_TAP_IN_VOLUME", "TOTAL_TAP_OUT_VOLUME"))

PV_OD = CsvDataset(
    "pv_od", r"^origin_destination_bus_(\d{4})(\d{2})\.csv$",
    {"YEAR_MONTH": "VARCHAR", "DAY_TYPE": "VARCHAR", "TIME_PER_HOUR": "TINYINT", "PT_TYPE": "VARCHAR",
     "ORIGIN_PT_CODE": "VARCHAR", "DESTINATION_PT_CODE": "VARCHAR", "TOTAL_TRIPS": "BIGINT"},
    ("YEAR_MONTH", "DAY_TYPE", "TIME_PER_HOUR", "ORIGIN_PT_CODE", "DESTINATION_PT_CODE"),
    ("ORIGIN_PT_CODE", "DESTINATION_PT_CODE"), ("TOTAL_TRIPS",))

LINKS_COLUMNS = {"LinkID": "BIGINT", "RoadName": "VARCHAR", "RoadCategory": "TINYINT", "StartLat": "DOUBLE",
                 "StartLon": "DOUBLE", "EndLat": "DOUBLE", "EndLon": "DOUBLE"}

STATIC_JSON = {
    "bus_stops": ("BusStops", {"BusStopCode": "VARCHAR", "RoadName": "VARCHAR", "Description": "VARCHAR",
                               "Latitude": "DOUBLE", "Longitude": "DOUBLE"}),
    "bus_services": ("BusServices", {"ServiceNo": "VARCHAR", "Operator": "VARCHAR", "Direction": "TINYINT",
                                     "Category": "VARCHAR", "OriginCode": "VARCHAR", "DestinationCode": "VARCHAR",
                                     "AM_Peak_Freq": "VARCHAR", "AM_Offpeak_Freq": "VARCHAR",
                                     "PM_Peak_Freq": "VARCHAR", "PM_Offpeak_Freq": "VARCHAR",
                                     "LoopDesc": "VARCHAR"}),
    "bus_routes": ("BusRoutes", {"ServiceNo": "VARCHAR", "Operator": "VARCHAR", "Direction": "TINYINT",
                                 "StopSequence": "SMALLINT", "BusStopCode": "VARCHAR", "Distance": "DOUBLE",
                                 "WD_FirstBus": "VARCHAR", "WD_LastBus": "VARCHAR", "SAT_FirstBus": "VARCHAR",
                                 "SAT_LastBus": "VARCHAR", "SUN_FirstBus": "VARCHAR", "SUN_LastBus": "VARCHAR"}),
}


class SchemaDriftError(ValueError):
    pass


def sql_str(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def read_header(path: Path) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 22):
            h.update(chunk)
    return h.hexdigest()


class _HashingReader(io.RawIOBase):
    def __init__(self, f):
        self.f, self.h = f, hashlib.sha256()

    def readable(self):
        return True

    def readinto(self, b):
        n = self.f.readinto(b)
        if n:
            self.h.update(memoryview(b)[:n])
        return n


def count_source(path: Path, columns: list[str], volume_columns=()) -> dict:
    """Row count, volume sums and SHA-256 of a raw CSV, computed with the csv module (not DuckDB), so the
    reconciliation compares the lake with an independent count. A volume counts towards the sum when its row has the
    expected number of fields and the value is an integer (the same rule the load applies)."""
    idx = [columns.index(c) for c in volume_columns]
    rows = well_formed = 0
    sums = [0] * len(idx)
    with open(path, "rb") as raw:
        hr = _HashingReader(raw)
        with io.TextIOWrapper(io.BufferedReader(hr, 1 << 20), encoding="utf-8-sig", errors="replace",
                              newline="") as f:
            r = csv.reader(f)
            next(r, None)
            for row in r:
                if not row:
                    continue
                rows += 1
                if len(row) != len(columns):
                    continue
                well_formed += 1
                for k, i in enumerate(idx):
                    v = row[i].strip(" ")
                    if INT_RE.fullmatch(v):
                        sums[k] += int(v)
            while hr.readinto(bytearray(1 << 20)):  # hash any bytes the csv reader did not need
                pass
    return {"sha256": hr.h.hexdigest(), "rows": rows, "well_formed_rows": well_formed,
            "sums": dict(zip(volume_columns, sums))}


def rule_sql(ds: CsvDataset, year_month: str) -> str:
    """CASE expression returning the first failed rule for a row of raw text columns, or NULL if the row is valid."""
    checks = []
    for col, typ in ds.columns.items():
        if typ != "VARCHAR":
            checks.append((f"{col} IS NULL OR NOT regexp_full_match(trim({col}), '{INT_SQL}') "
                           f"OR TRY_CAST(trim({col}) AS {typ}) IS NULL", f"missing_or_uncastable_{col.lower()}"))
    checks += [
        (f"coalesce(YEAR_MONTH, '') <> {sql_str(year_month)}", "year_month_mismatch"),
        (f"coalesce(DAY_TYPE, '') NOT IN ({', '.join(sql_str(d) for d in DAY_TYPES)})", "bad_day_type"),
        ("TRY_CAST(trim(TIME_PER_HOUR) AS TINYINT) NOT BETWEEN 0 AND 23", "bad_hour"),
        ("coalesce(PT_TYPE, '') <> 'BUS'", "bad_pt_type"),
    ]
    for c in ds.code_columns:
        checks.append((f"NOT regexp_full_match(coalesce({c}, ''), '[0-9]{{5}}')", f"bad_{c.lower()}"))
    for c in ds.volume_columns:
        checks.append((f"TRY_CAST(trim({c}) AS BIGINT) < 0", f"negative_{c.lower()}"))
    return "CASE " + " ".join(f"WHEN {cond} THEN '{name}'" for cond, name in checks) + " END"


def _reader(con, sql: str) -> pa.RecordBatchReader:
    """Stream a query result to the Delta writer (tests replace this to make a load fail half-way)."""
    return con.execute(sql).to_arrow_reader(500_000)


def _read_with_rejects(con, path: Path, columns: dict, as_text: bool) -> str:
    """read_csv expression that sets malformed lines aside in the temp table reject_errors."""
    con.execute("DROP TABLE IF EXISTS reject_errors")
    con.execute("DROP TABLE IF EXISTS reject_scans")
    cols = ", ".join(f"{sql_str(c)}: {sql_str('VARCHAR' if as_text else t)}" for c, t in columns.items())
    return (f"read_csv({sql_str(str(path))}, header=true, auto_detect=false, columns={{{cols}}}, "
            "store_rejects=true, rejects_table='reject_errors', rejects_scan='reject_scans')")


def _malformed(con) -> list[tuple]:
    """(line number, raw line, error types) for every line read_csv set aside."""
    return con.execute("SELECT line, any_value(csv_line), string_agg(DISTINCT error_type, '; ' ORDER BY error_type) "
                       "FROM reject_errors GROUP BY line ORDER BY line").fetchall()


def _bad_lines_table(con, bad: list[tuple]) -> None:
    con.execute("CREATE OR REPLACE TEMP TABLE bad_lines (line BIGINT, csv_line VARCHAR, error VARCHAR)")
    if bad:
        con.executemany("INSERT INTO bad_lines VALUES (?, ?, ?)", bad)


def _write_quarantine(lake: Lake, rel: str, con, sql: str, predicate: str | None, partition_by, meta: dict) -> int:
    """Replace the quarantined rows of one load (predicate) in one commit, including when there are none now."""
    t = con.execute(sql).to_arrow_table()
    if t.num_rows == 0 and lake.delta(rel) is None:
        return 0
    lake.write_delta(rel, t, mode="overwrite", predicate=predicate, partition_by=partition_by, metadata=meta)
    return t.num_rows


def ingest_csv(con, lake: Lake, ds: CsvDataset, path: Path) -> dict:
    m = re.match(ds.pattern, path.name)
    year_month = f"{m.group(1)}-{m.group(2)}"
    header = read_header(path)
    if header != list(ds.columns):
        raise SchemaDriftError(f"{path.name}: header {header} != {list(ds.columns)}")
    source = count_source(path, list(ds.columns), ds.volume_columns)
    raw = _read_with_rejects(con, path, ds.columns, as_text=True)
    typed = ", ".join(f"CAST(trim({c}) AS {t}) AS {c}" if t != "VARCHAR" else c for c, t in ds.columns.items())
    rule = rule_sql(ds, year_month)
    # One pass over the source file; the queries below read DuckDB's copy (spills to disk if needed).
    con.execute(f"CREATE OR REPLACE TEMP TABLE src AS SELECT *, {rule} AS _reject FROM {raw}")
    bad_lines = _malformed(con)
    _bad_lines_table(con, bad_lines)
    meta = {"source_file": path.name, "source_sha256": source["sha256"], "source_rows": source["rows"]}
    pred = f"month = '{year_month}'"
    lake.write_delta(f"bronze/{ds.name}", _reader(con, f"SELECT {typed}, '{year_month}' AS month FROM src "
                                                      "WHERE _reject IS NULL"),
                     mode="overwrite", predicate=pred, partition_by=["month"], metadata=meta)
    text_cols = ", ".join(ds.columns)
    nulls = ", ".join(f"NULL::VARCHAR AS {c}" for c in ds.columns)
    _write_quarantine(
        lake, f"bronze/_quarantine/{ds.name}", con,
        f"SELECT {text_cols}, NULL::VARCHAR AS _raw_line, _reject, '{year_month}' AS month FROM src "
        f"WHERE _reject IS NOT NULL UNION ALL "
        f"SELECT {nulls}, csv_line AS _raw_line, 'malformed_row' AS _reject, '{year_month}' AS month FROM bad_lines",
        pred, ["month"], meta)
    sums = ", ".join(f"sum(TRY_CAST(trim({c}) AS BIGINT)) FILTER (WHERE _reject IS NULL) AS {c}"
                     for c in ds.volume_columns)
    stats = con.execute(f"SELECT count(*), count(*) FILTER (WHERE _reject IS NULL), {sums} FROM src").fetchone()
    reasons = dict(con.execute("SELECT _reject, count(*) FROM src WHERE _reject IS NOT NULL GROUP BY 1").fetchall())
    if bad_lines:
        reasons["malformed_row"] = len(bad_lines)
    qsums = con.execute("SELECT " + ", ".join(
        f"coalesce(sum(CASE WHEN regexp_full_match(trim({c}), '{INT_SQL}') THEN TRY_CAST(trim({c}) AS BIGINT) END), 0)"
        for c in ds.volume_columns) + " FROM src WHERE _reject IS NOT NULL").fetchone()
    con.execute("DROP TABLE src")
    rows_read = stats[0] + len(bad_lines)
    return {
        "dataset": ds.name, "file": path.name, "year_month": year_month, "bytes": path.stat().st_size,
        "sha256": source["sha256"], "source": source,
        "rows_read": rows_read, "rows_valid": stats[1], "rows_quarantined": rows_read - stats[1],
        "quarantine_reasons": dict(sorted(reasons.items())),
        "valid_sums": dict(zip(ds.volume_columns, (int(x or 0) for x in stats[2:]))),
        "quarantined_sums": dict(zip(ds.volume_columns, (int(x) for x in qsums))),
        "bronze_table": f"bronze/{ds.name}", "partition": pred,
    }


def ingest_links(con, lake: Lake, path: Path) -> dict:
    header = read_header(path)
    if header != list(LINKS_COLUMNS):
        raise SchemaDriftError(f"{path.name}: header {header} != {list(LINKS_COLUMNS)}")
    source = count_source(path, list(LINKS_COLUMNS))
    raw = _read_with_rejects(con, path, LINKS_COLUMNS, as_text=False)   # typed: cast errors are rejects too
    con.execute(f"CREATE OR REPLACE TEMP TABLE src AS SELECT * FROM {raw}")
    bad = _malformed(con)
    meta = {"source_file": path.name, "source_sha256": source["sha256"], "source_rows": source["rows"]}
    lake.write_delta("bronze/speedband_links", _reader(con, "SELECT * FROM src ORDER BY LinkID"), mode="overwrite",
                     metadata=meta)
    _bad_lines_table(con, bad)
    _write_quarantine(lake, "bronze/_quarantine/speedband_links", con,
                      "SELECT csv_line AS _raw_line, 'rejected_row: ' || error AS _reject FROM bad_lines",
                      None, None, meta)
    n = con.execute("SELECT count(*) FROM src").fetchone()[0]
    con.execute("DROP TABLE src")
    return {"dataset": "speedband_links", "file": path.name, "bytes": path.stat().st_size,
            "sha256": source["sha256"], "source": source, "rows_read": n + len(bad), "rows_valid": n,
            "rows_quarantined": len(bad), "quarantine_reasons": {"rejected_row": len(bad)} if bad else {},
            "bronze_table": "bronze/speedband_links"}


def ingest_static(con, lake: Lake, dataset: str, path: Path, as_of: str) -> dict:
    """The bus network pulls (JSON arrays). The key set must match exactly; a value of the wrong type stops the
    load (these files are small API pulls that are refetched, not row-level feeds)."""
    stem, columns = STATIC_JSON[dataset]
    rows = json.loads(path.read_text(encoding="utf-8"))
    keys = set().union(*(r.keys() for r in rows)) if rows else set()
    if keys != set(columns):
        raise SchemaDriftError(f"{path.name}: keys {sorted(keys)} != {sorted(columns)}")
    cols = ", ".join(f"{sql_str(c)}: {sql_str(t)}" for c, t in columns.items())
    sha = sha256_file(path)
    lake.write_delta(f"bronze/{dataset}",
                     _reader(con, f"SELECT *, '{as_of}' AS as_of FROM read_json({sql_str(str(path))}, "
                                  f"format='array', columns={{{cols}}})"),
                     mode="overwrite", predicate=f"as_of = '{as_of}'", partition_by=["as_of"],
                     metadata={"source_file": path.name, "source_sha256": sha, "source_rows": len(rows)})
    return {"dataset": dataset, "file": path.name, "bytes": path.stat().st_size, "sha256": sha,
            "source": {"sha256": sha, "rows": len(rows)}, "rows_read": len(rows), "rows_valid": len(rows),
            "rows_quarantined": 0, "bronze_table": f"bronze/{dataset}", "partition": f"as_of = '{as_of}'"}


def run(settings, log=print) -> list[dict]:
    lake = Lake(settings)
    lake.ensure_bucket()
    con = duckdb_connect(settings)
    src = Path(settings.source_dir)
    out = []
    for ds in (PV_STOP, PV_OD):
        for p in sorted(src.iterdir()):
            if re.match(ds.pattern, p.name):
                out.append(ingest_csv(con, lake, ds, p))
                log(json.dumps({k: v for k, v in out[-1].items() if k not in ("sha256", "source")}))
    links = src / "speedband_links.csv"
    if links.exists():
        out.append(ingest_links(con, lake, links))
    for dataset, (stem, _) in STATIC_JSON.items():
        files = sorted(src.glob(f"{stem}_*.json"))
        for p in files[-1:]:  # latest pull only
            as_of = re.match(rf"{stem}_(\d{{8}})\.json", p.name).group(1)
            out.append(ingest_static(con, lake, dataset, p, as_of))
            log(json.dumps({k: v for k, v in out[-1].items() if k not in ("sha256", "source")}))
    con.close()
    return out
