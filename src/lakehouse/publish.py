"""Publish the gold layer to the lake as Parquet and record row counts per layer and hashes of every gold table.

Each gold table is exported in a fixed row order. Two hashes are recorded per table:
  file_sha256    - SHA-256 of the Parquet object's bytes;
  content_sha256 - SHA-256 of the rows serialised as CSV in key order (independent of the Parquet writer).
The reproducibility check compares both between two independent runs.
"""
from __future__ import annotations

import dataclasses
import hashlib
import io
import json
from pathlib import Path

import pyarrow.csv as pacsv

from .profiling import DATASETS
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
    reader = con.execute(f"SELECT * FROM {table} ORDER BY {', '.join(order)}").fetch_record_batch(100_000)
    for batch in reader:
        buf = io.BytesIO()
        pacsv.write_csv(batch, buf, pacsv.WriteOptions(include_header=False))
        h.update(buf.getvalue())
    return h.hexdigest()


def export_gold(settings, con, lake: Lake) -> dict:
    lake.ensure_bucket()
    out = {}
    for t, order in GOLD.items():
        rel = f"gold/{t}/data.parquet"
        if not settings.is_s3:
            lake.fs.create_dir(lake.path(f"gold/{t}"), recursive=True)
        con.execute(f"COPY (SELECT * FROM gold.{t} ORDER BY {', '.join(order)}) TO '{lake.uri(rel)}' "
                    "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)")
        out[t] = {"path": rel, "rows": con.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0],
                  "file_sha256": lake.sha256(rel), "content_sha256": content_sha256(con, f"gold.{t}", order)}
    return out


def layer_counts(settings, con, lake: Lake) -> dict:
    bronze = {}
    for name in DATASETS:
        files = lake.list_files(f"bronze/{name}")
        bronze[name] = {"files": len(files), "rows": sum(lake.num_rows(f) for f in files)}
    q = lake.list_files("bronze/_quarantine")
    bronze["_quarantine"] = {"files": len(q), "rows": sum(lake.num_rows(f) for f in q)}
    silver = {t: con.execute(f"SELECT count(*) FROM silver.{t}").fetchone()[0] for t in SILVER}
    gold = {t: con.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0] for t in GOLD}
    return {"bronze": bronze, "silver": silver, "gold": gold}


def run(settings) -> dict:
    lake = Lake(settings)
    # threads=1: DuckDB then writes row groups strictly in order, which keeps the Parquet bytes reproducible.
    con = duckdb_connect(dataclasses.replace(settings, duckdb_threads=1), settings.warehouse, read_only=True)
    manifest = export_gold(settings, con, lake)
    counts = layer_counts(settings, con, lake)
    con.close()
    out = Path(settings.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "gold_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (out / "layer_counts.json").write_text(json.dumps(counts, indent=2), encoding="utf-8")
    return {"gold": manifest, "layers": counts}
