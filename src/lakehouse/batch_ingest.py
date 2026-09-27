"""Batch ingest of the monthly passenger-volume files, the static bus network and the link metadata into bronze.

Schema enforcement, in order:
  1. the header must match the expected column list exactly (a renamed, added or missing column stops the load);
  2. every value is read as text and cast with TRY_CAST, so a value of the wrong type does not stop the load;
  3. rows that fail a cast or a domain rule go to bronze/_quarantine/<dataset>/ with the first failing rule;
     valid rows go to bronze/<dataset>/.

DuckDB reads each CSV once into a temporary table (the OD files are about 6 million rows each; DuckDB spills to disk
beyond its memory limit), writes valid and rejected rows to Parquet and computes the reconciliation totals from it. Each load replaces its whole partition, so re-running a load does not duplicate rows.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from .storage import Lake, duckdb_connect

DAY_TYPES = ("WEEKDAY", "WEEKENDS/HOLIDAY")


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


def rule_sql(ds: CsvDataset, year_month: str) -> str:
    """CASE expression returning the first failed rule for a row of raw text columns, or NULL if the row is valid."""
    checks = []
    for col, typ in ds.columns.items():
        if typ != "VARCHAR":
            checks.append((f"{col} IS NULL OR TRY_CAST({col} AS {typ}) IS NULL", f"missing_or_uncastable_{col.lower()}"))
    checks += [
        (f"coalesce(YEAR_MONTH, '') <> {sql_str(year_month)}", "year_month_mismatch"),
        (f"coalesce(DAY_TYPE, '') NOT IN ({', '.join(sql_str(d) for d in DAY_TYPES)})", "bad_day_type"),
        ("TRY_CAST(TIME_PER_HOUR AS TINYINT) NOT BETWEEN 0 AND 23", "bad_hour"),
        ("coalesce(PT_TYPE, '') <> 'BUS'", "bad_pt_type"),
    ]
    for c in ds.code_columns:
        checks.append((f"NOT regexp_full_match(coalesce({c}, ''), '[0-9]{{5}}')", f"bad_{c.lower()}"))
    for c in ds.volume_columns:
        checks.append((f"TRY_CAST({c} AS BIGINT) < 0", f"negative_{c.lower()}"))
    return "CASE " + " ".join(f"WHEN {cond} THEN '{name}'" for cond, name in checks) + " END"


def ingest_csv(con, lake: Lake, ds: CsvDataset, path: Path) -> dict:
    m = re.match(ds.pattern, path.name)
    year_month = f"{m.group(1)}-{m.group(2)}"
    header = read_header(path)
    if header != list(ds.columns):
        raise SchemaDriftError(f"{path.name}: header {header} != {list(ds.columns)}")
    as_text = ", ".join(f"{sql_str(c)}: 'VARCHAR'" for c in ds.columns)
    raw = f"read_csv({sql_str(str(path))}, header=true, auto_detect=false, columns={{{as_text}}})"
    typed = ", ".join(f"CAST({c} AS {t}) AS {c}" if t != "VARCHAR" else c for c, t in ds.columns.items())
    rule = rule_sql(ds, year_month)
    part = f"bronze/{ds.name}/month={year_month}"
    qpart = f"bronze/_quarantine/{ds.name}/month={year_month}"
    lake.delete_prefix(part)
    lake.delete_prefix(qpart)
    if not lake.settings.is_s3:
        lake.fs.create_dir(lake.path(part), recursive=True)
    # One pass over the source file; the three queries below then read DuckDB's copy (spills to disk if needed).
    con.execute(f"CREATE OR REPLACE TEMP TABLE src AS SELECT *, {rule} AS _reject FROM {raw}")
    con.execute(f"COPY (SELECT {typed} FROM src WHERE _reject IS NULL) TO {sql_str(lake.uri(part + '/part-0.parquet'))} "
                "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 500000)")
    sums = ", ".join(f"sum(TRY_CAST({c} AS BIGINT)) FILTER (WHERE _reject IS NULL) AS {c}" for c in ds.volume_columns)
    stats = con.execute(f"SELECT count(*), count(*) FILTER (WHERE _reject IS NULL), {sums} FROM src").fetchone()
    reasons = dict(con.execute("SELECT _reject, count(*) FROM src WHERE _reject IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall())
    qsums = con.execute("SELECT " + ", ".join(f"coalesce(sum(TRY_CAST({c} AS BIGINT)), 0)" for c in ds.volume_columns)
                        + " FROM src WHERE _reject IS NOT NULL").fetchone()
    if reasons:
        if not lake.settings.is_s3:
            lake.fs.create_dir(lake.path(qpart), recursive=True)
        con.execute(f"COPY (SELECT * FROM src WHERE _reject IS NOT NULL) TO "
                    f"{sql_str(lake.uri(qpart + '/part-0.parquet'))} (FORMAT parquet)")
    con.execute("DROP TABLE src")
    return {
        "dataset": ds.name, "file": path.name, "year_month": year_month, "bytes": path.stat().st_size,
        "sha256": sha256_file(path), "rows_read": stats[0], "rows_valid": stats[1],
        "rows_quarantined": stats[0] - stats[1], "quarantine_reasons": reasons,
        "valid_sums": dict(zip(ds.volume_columns, stats[2:])),
        "quarantined_sums": dict(zip(ds.volume_columns, (int(x) for x in qsums))), "bronze_path": part,
    }


def ingest_links(con, lake: Lake, path: Path) -> dict:
    header = read_header(path)
    if header != list(LINKS_COLUMNS):
        raise SchemaDriftError(f"{path.name}: header {header} != {list(LINKS_COLUMNS)}")
    cols = ", ".join(f"{sql_str(c)}: {sql_str(t)}" for c, t in LINKS_COLUMNS.items())
    rel = "bronze/speedband_links/data.parquet"
    if not lake.settings.is_s3:
        lake.fs.create_dir(lake.path("bronze/speedband_links"), recursive=True)
    con.execute(f"COPY (SELECT * FROM read_csv({sql_str(str(path))}, header=true, auto_detect=false, columns={{{cols}}}) "
                f"ORDER BY LinkID) TO {sql_str(lake.uri(rel))} (FORMAT parquet, COMPRESSION zstd)")
    n = con.execute(f"SELECT count(*) FROM read_parquet({sql_str(lake.uri(rel))})").fetchone()[0]
    return {"dataset": "speedband_links", "file": path.name, "bytes": path.stat().st_size,
            "sha256": sha256_file(path), "rows_read": n, "rows_valid": n, "rows_quarantined": 0, "bronze_path": rel}


def ingest_static(con, lake: Lake, dataset: str, path: Path, as_of: str) -> dict:
    stem, columns = STATIC_JSON[dataset]
    rows = json.loads(path.read_text(encoding="utf-8"))
    keys = set().union(*(r.keys() for r in rows)) if rows else set()
    if keys != set(columns):
        raise SchemaDriftError(f"{path.name}: keys {sorted(keys)} != {sorted(columns)}")
    cols = ", ".join(f"{sql_str(c)}: {sql_str(t)}" for c, t in columns.items())
    rel = f"bronze/{dataset}/as_of={as_of}/data.parquet"
    lake.delete_prefix(f"bronze/{dataset}/as_of={as_of}")
    if not lake.settings.is_s3:
        lake.fs.create_dir(lake.path(f"bronze/{dataset}/as_of={as_of}"), recursive=True)
    con.execute(f"COPY (SELECT * FROM read_json({sql_str(str(path))}, format='array', columns={{{cols}}})) "
                f"TO {sql_str(lake.uri(rel))} (FORMAT parquet, COMPRESSION zstd)")
    return {"dataset": dataset, "file": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path),
            "rows_read": len(rows), "rows_valid": len(rows), "rows_quarantined": 0, "bronze_path": rel}


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
                log(json.dumps({k: v for k, v in out[-1].items() if k != "sha256"}))
    links = src / "speedband_links.csv"
    if links.exists():
        out.append(ingest_links(con, lake, links))
    for dataset, (stem, _) in STATIC_JSON.items():
        files = sorted(src.glob(f"{stem}_*.json"))
        for p in files[-1:]:  # latest pull only
            as_of = re.match(rf"{stem}_(\d{{8}})\.json", p.name).group(1)
            out.append(ingest_static(con, lake, dataset, p, as_of))
            log(json.dumps({k: v for k, v in out[-1].items() if k != "sha256"}))
    con.close()
    return out
