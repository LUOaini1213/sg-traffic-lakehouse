"""Profile every bronze table and reconcile it with the source files.

For each table: row count, per-column null count / min / max / approximate distinct count, and the number of
duplicate keys.

Reconciliation (the post-load validation step of a migration) compares what is in the lake now with totals counted
from the raw files by a separate Python pass at load time (batch_ingest.count_source, speedbands.parse_snapshot), not
with numbers the loading query produced:
  - monthly CSVs and the link file: source rows = bronze rows + quarantined rows of that month, and for each volume
    column, source sum = bronze sum + sum over quarantined rows;
  - speed-band snapshots: source rows = distinct links in bronze for that snapshot + quarantined rows + repeated
    links in the file.
"""
from __future__ import annotations

import json
from pathlib import Path

from .batch_ingest import INT_SQL, PV_OD, PV_STOP
from .storage import Lake, duckdb_connect

DATASETS = {
    "speedbands": ("snapshot_ts", "link_id"),
    "speedband_links": ("LinkID",),
    "pv_stop": PV_STOP.key,
    "pv_od": PV_OD.key,
    "bus_stops": ("as_of", "BusStopCode"),
    "bus_services": ("as_of", "ServiceNo", "Direction"),
    "bus_routes": ("as_of", "ServiceNo", "Direction", "StopSequence"),
}
QUARANTINE = ["pv_stop", "pv_od", "speedband_links", "speedbands"]
DEAD_LETTER = ["speedband_stream"]


def profile_dataset(con, lake: Lake, name: str, key: tuple) -> dict | None:
    st = lake.delta_stats(f"bronze/{name}")
    if st is None:
        return None
    con.execute(f"CREATE OR REPLACE TEMP VIEW p AS SELECT * FROM {lake.scan(f'bronze/{name}')}")
    cols = [r[0] for r in con.execute("DESCRIBE p").fetchall()]
    n = con.execute("SELECT count(*) FROM p").fetchone()[0]
    stats = {}
    for c in cols:
        nulls, lo, hi, nd = con.execute(
            f'SELECT count(*) - count("{c}"), min("{c}")::VARCHAR, max("{c}")::VARCHAR, '
            f'approx_count_distinct("{c}") FROM p').fetchone()
        stats[c] = {"nulls": nulls, "min": lo, "max": hi, "approx_distinct": nd}
    k = ", ".join(f'"{c}"' for c in key)
    dup = con.execute(f"SELECT count(*) FROM (SELECT {k} FROM p GROUP BY {k} HAVING count(*) > 1)").fetchone()[0]
    return {"dataset": name, "files": st["files"], "delta_version": st["version"], "rows": n, "key": list(key),
            "duplicate_keys": dup, "columns": stats}


def _qsum(c: str) -> str:
    return f"coalesce(sum(CASE WHEN regexp_full_match(trim({c}), '{INT_SQL}') THEN TRY_CAST(trim({c}) AS BIGINT) END), 0)"


def reconcile(con, lake: Lake, loads: list[dict]) -> list[dict]:
    out = []
    for x in loads:
        if x["dataset"] not in ("pv_stop", "pv_od", "speedband_links"):
            continue
        src = x["source"]
        cols = list(src.get("sums", {}))
        where = f"WHERE {x['partition']}" if x.get("partition") else ""
        b = con.execute(f"SELECT count(*) {''.join(f', sum({c})' for c in cols)} "
                        f"FROM {lake.scan(x['bronze_table'])} {where}").fetchone()
        qrel = f"bronze/_quarantine/{x['dataset']}"
        if lake.delta(qrel) is not None:
            q = con.execute(f"SELECT count(*) {''.join(f', {_qsum(c)}' for c in cols)} "
                            f"FROM {lake.scan(qrel)} {where}").fetchone()
        else:
            q = (0, *([0] * len(cols)))
        bronze_sums = {c: int(v or 0) for c, v in zip(cols, b[1:])}
        q_sums = {c: int(v or 0) for c, v in zip(cols, q[1:])}
        lake_sums = {c: bronze_sums[c] + q_sums[c] for c in cols}
        out.append({"dataset": x["dataset"], "file": x["file"], "source_sha256": src["sha256"],
                    "source_rows": src["rows"], "bronze_rows": b[0], "quarantined_rows": q[0],
                    "source_sums": src.get("sums", {}), "bronze_plus_quarantine_sums": lake_sums,
                    "match": b[0] + q[0] == src["rows"] and b[0] == x["rows_valid"] and lake_sums == src.get("sums", {})})
    return out


def reconcile_speedbands(con, lake: Lake, files: list[dict]) -> list[dict]:
    if not files or lake.delta("bronze/speedbands") is None:
        return []
    bronze = dict(con.execute(f"SELECT strftime(snapshot_ts, '%Y-%m-%dT%H:%M:%S'), count(DISTINCT link_id) "
                              f"FROM {lake.scan('bronze/speedbands')} GROUP BY 1").fetchall())
    quar = {}
    if lake.delta("bronze/_quarantine/speedbands") is not None:
        quar = dict(con.execute(f"SELECT strftime(snapshot_ts, '%Y-%m-%dT%H:%M:%S'), count(*) "
                                f"FROM {lake.scan('bronze/_quarantine/speedbands')} GROUP BY 1").fetchall())
    out = []
    for f in files:
        ts = f["snapshot_ts"]
        b, q = bronze.get(ts, 0), quar.get(ts, 0)
        out.append({"file": f["file"], "source_rows": f["source_rows"], "bronze_distinct_links": b,
                    "quarantined_rows": q, "repeated_links_in_file": f["duplicate_rows_in_file"],
                    "match": f["source_rows"] == b + q + f["duplicate_rows_in_file"]})
    return out


def run(settings) -> dict:
    lake = Lake(settings)
    con = duckdb_connect(settings)
    prof = [p for name, key in DATASETS.items() if (p := profile_dataset(con, lake, name, key))]
    res = Path(settings.results_dir)
    ingest, sbf = res / "batch_ingest.json", res / "speedband_files.json"
    rec = reconcile(con, lake, json.loads(ingest.read_text(encoding="utf-8"))["loads"]) if ingest.exists() else []
    srec = (reconcile_speedbands(con, lake, json.loads(sbf.read_text(encoding="utf-8"))["snapshots"])
            if sbf.exists() else [])
    con.close()
    report = {"datasets": prof, "reconciliation": rec, "speedband_reconciliation": srec,
              "all_reconciled": all(r["match"] for r in rec + srec) and bool(rec)}
    res.mkdir(parents=True, exist_ok=True)
    (res / "profile_bronze.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (res / "profile_bronze.md").write_text(to_markdown(report), encoding="utf-8")
    return report


def to_markdown(report) -> str:
    lines = ["# Bronze profile", "", "Generated by `python -m lakehouse profile`.", "",
             "| Dataset | Delta version | Files | Rows | Key | Duplicate keys |", "|---|---|---|---|---|---|"]
    for d in report["datasets"]:
        lines.append(f"| {d['dataset']} | {d['delta_version']} | {d['files']} | {d['rows']:,} | {', '.join(d['key'])} "
                     f"| {d['duplicate_keys']} |")
    lines += ["", "## Source-to-bronze reconciliation", "",
              "Source totals come from a separate Python pass over the raw file at load time.", "",
              "| File | Source rows | Bronze rows | Quarantined | Source sums | Bronze + quarantine sums | Match |",
              "|---|---|---|---|---|---|---|"]
    for r in report["reconciliation"]:
        fmt = lambda d: ", ".join(f"{k} {v:,}" for k, v in d.items()) or "-"  # noqa: E731
        lines.append(f"| {r['file']} | {r['source_rows']:,} | {r['bronze_rows']:,} | {r['quarantined_rows']:,} | "
                     f"{fmt(r['source_sums'])} | {fmt(r['bronze_plus_quarantine_sums'])} | {r['match']} |")
    if report.get("speedband_reconciliation"):
        s = report["speedband_reconciliation"]
        lines += ["", f"Speed-band snapshots: {sum(r['match'] for r in s)}/{len(s)} files reconcile "
                      "(source rows = distinct links in bronze + quarantined + repeated links in the file).", ""]
        for r in s:
            if not r["match"]:
                lines.append(f"- {r['file']}: source {r['source_rows']}, bronze {r['bronze_distinct_links']}, "
                             f"quarantined {r['quarantined_rows']}, repeated {r['repeated_links_in_file']}")
    for d in report["datasets"]:
        lines += ["", f"## {d['dataset']}", "", "| Column | Nulls | Min | Max | Approx. distinct |",
                  "|---|---|---|---|---|"]
        for c, s in d["columns"].items():
            lines.append(f"| {c} | {s['nulls']:,} | {s['min']} | {s['max']} | {s['approx_distinct']:,} |")
    return "\n".join(lines) + "\n"
