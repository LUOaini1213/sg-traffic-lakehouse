"""Publish the gold layer to the lake as Delta tables and record row counts per layer and hashes of every gold table.

All five gold tables are staged under one immutable release ID. One Delta
ledger commit activates their manifest only after every file is verified.
A Lake reader captures that manifest once, so a later publication cannot mix
old and new tables within its read. Two hashes are recorded per table:
  file_sha256    - SHA-256 over the SHA-256s of the table's Parquet data files (delta-rs writes the same bytes for
                   the same input);
  content_sha256 - SHA-256 of the rows serialised as CSV in key order (independent of the Parquet writer).
The reproducibility check compares both between two independent runs.
"""
from __future__ import annotations

import dataclasses
import hashlib
import io
import json
import os
import tempfile
import uuid
from pathlib import Path

import pyarrow.csv as pacsv

from .profiling import DATASETS, DEAD_LETTER, QUARANTINE
from .publication import activate, release_path
from .storage import Lake, duckdb_connect

GOLD = {
    "gold_link_speed_hourly": ["link_id", "day_type", "hour_of_day"],
    "gold_congestion_episodes": ["link_id", "start_ts"],
    "gold_stop_demand_hourly": ["year_month", "day_type", "hour", "stop_code"],
    "gold_top_od_flows": ["year_month", "day_type", "flow_rank"],
    "gold_link_hour_features": ["link_id", "obs_hour_ts"],
}
SILVER = ["stg_speedband_obs", "dim_snapshot", "dim_link", "dim_calendar", "dim_month_day_counts", "dim_bus_stop",
          "dim_bus_service", "fct_bus_route_stop", "stg_stop_pv", "stg_od"]


def content_sha256(con, table: str, order: list[str]) -> str:
    h = hashlib.sha256()
    reader = con.execute(f"SELECT * FROM {table} ORDER BY {', '.join(order)}").to_arrow_reader(100_000)
    for batch in reader:
        buf = io.BytesIO()
        pacsv.write_csv(batch, buf, pacsv.WriteOptions(include_header=False))
        h.update(buf.getvalue())
    return h.hexdigest()


def export_gold(settings, con, lake: Lake, release_id: str) -> dict:
    lake.ensure_bucket()
    out = {}
    for t, order in GOLD.items():
        rel = release_path(release_id, t)
        reader = con.execute(f"SELECT * FROM gold.{t} ORDER BY {', '.join(order)}").to_arrow_reader(122_880)
        lake.write_delta(rel, reader, mode="error")
        st = lake.delta_stats(rel)
        files = lake.delta_file_hashes(rel)
        out[t] = {"path": rel, "delta_version": st["version"], "rows": st["rows"], "files": st["files"],
                  "file_sha256": hashlib.sha256("".join(files).encode()).hexdigest(),
                  "content_sha256": content_sha256(con, f"gold.{t}", order)}
    return out


def layer_counts(settings, con, lake: Lake) -> dict:
    bronze = {}
    for name in DATASETS:
        st = lake.delta_stats(f"bronze/{name}")
        bronze[name] = {"files": st["files"], "rows": st["rows"]} if st else {"files": 0, "rows": 0}
    for group, names in (("_quarantine", QUARANTINE), ("_dead_letter", DEAD_LETTER)):
        sts = [s for n in names if (s := lake.delta_stats(f"bronze/{group}/{n}"))]
        bronze[group] = {"files": sum(s["files"] for s in sts), "rows": sum(s["rows"] for s in sts)}
    silver = {t: con.execute(f"SELECT count(*) FROM silver.{t}").fetchone()[0] for t in SILVER}
    gold = {t: con.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0] for t in GOLD}
    return {"bronze": bronze, "silver": silver, "gold": gold}


def _atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".publication-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as output:
            json.dump(value, output, indent=2, allow_nan=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _source_fingerprints() -> dict:
    root = Path(__file__).parent
    return {path.name: hashlib.sha256(path.read_text(encoding="utf-8").replace("\r\n", "\n").encode()).hexdigest()
            for path in sorted(root.glob("*.py"))}


def run(settings) -> dict:
    from . import report
    lake = Lake(settings)
    # threads=1: DuckDB then hands over rows strictly in order, which keeps the Parquet bytes reproducible.
    con = duckdb_connect(dataclasses.replace(settings, duckdb_threads=1), settings.warehouse, read_only=True)
    release_id = uuid.uuid4().hex
    try:
        con.execute("BEGIN TRANSACTION")
        manifest = export_gold(settings, con, lake, release_id)
        counts = layer_counts(settings, con, lake)
        page = report.prepare(con, settings, release_id).encode("utf-8")
        report_path = f"gold/releases/{release_id}/report.html"
        if lake.exists(report_path):
            raise ValueError("Publication report is immutable")
        with lake.fs.open_output_stream(lake.path(report_path)) as output:
            output.write(page)
    finally:
        con.close()
    out = Path(settings.results_dir)
    publication = {"schema_version": 1, "release_id": release_id, "tables": manifest,
                   "layers": counts, "source_sha256_utf8_lf": _source_fingerprints(),
                   "report": {"path": report_path, "bytes": len(page), "sha256": hashlib.sha256(page).hexdigest()}}
    # Receipt preparation can fail without changing the active ledger. These
    # per-release files also remain recoverable if the process dies after commit.
    receipt_dir = out / "publications" / release_id
    _atomic_json(receipt_dir / "manifest.json", publication)
    committed = activate(lake, publication)
    warnings = []
    for name, value in (("gold_manifest.json", manifest), ("layer_counts.json", counts),
                        ("publication.json", {**publication, "ledger_version": committed.ledger_version})):
        try:
            _atomic_json(out / name, value)
        except OSError:
            # A local report failure cannot undo a committed object-store
            # transaction. Report that distinction and keep the ledger authoritative.
            warnings.append(f"Publication committed; local {name} receipt could not be replaced")
    return {"gold": manifest, "layers": counts, "publication": {**publication,
            "ledger_version": committed.ledger_version}, "receipt_warnings": warnings}
