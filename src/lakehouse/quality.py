"""Custom data-quality checks on the silver layer and the source files, with thresholds from
config/quality_thresholds.yml. Writes quality_report.json and quality_report.md to the results directory.

The dbt tests check that each model is well formed (keys, ranges, relationships). These checks answer questions
dbt tests cannot express as a single row-level rule: is the sampling cadence regular, is a snapshot stale, do two
DataMall products agree with each other.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from .config import REPO
from .storage import Lake, duckdb_connect

THRESHOLDS = REPO / "config" / "quality_thresholds.yml"


def _result(name, cfg, passed, value, threshold, details=None):
    sev = cfg["severity"]
    status = "PASS" if passed else {"error": "FAIL", "warn": "WARN", "info": "INFO"}[sev]
    return {"check": name, "severity": sev, "status": status, "value": value, "threshold": threshold,
            "details": details or {}, "why": " ".join(cfg.get("why", "").split())}


def check_snapshot_completeness(con, cfg):
    rows = con.execute("""
        with meta as (select count(*) as n from silver.dim_link)
        select s.snapshot_ts, count(l.link_id) as present, any_value(meta.n) as expected
        from silver.dim_snapshot s
        left join silver.stg_speedband_obs o using (snapshot_ts)
        left join silver.dim_link l on l.link_id = o.link_id
        cross join meta
        group by s.snapshot_ts order by s.snapshot_ts""").fetchall()
    ratios = [(ts, p / e if e else 0.0) for ts, p, e in rows]
    bad = [{"snapshot_ts": ts.isoformat(), "ratio": round(r, 4)} for ts, r in ratios if r < cfg["min_ratio"]]
    worst = round(min((r for _, r in ratios), default=0.0), 4)
    return _result("snapshot_completeness", cfg, not bad and bool(rows), worst, cfg["min_ratio"],
                   {"snapshots": len(rows), "incomplete": bad})


def check_stale(con, cfg):
    rows = con.execute("""
        with o as (
            select s.snapshot_seq, o.snapshot_ts, o.link_id, coalesce(o.speed_band, 0) as band
            from silver.stg_speedband_obs o join silver.dim_snapshot s using (snapshot_ts))
        select b.snapshot_ts, a.snapshot_ts as prev_ts, count(*) as n_common,
               count(*) filter (where a.band <> b.band) as n_changed
        from o b join o a on a.link_id = b.link_id and a.snapshot_seq = b.snapshot_seq - 1
        group by b.snapshot_ts, a.snapshot_ts order by b.snapshot_ts""").fetchall()
    shares = [(ts, prev, ch / n if n else 0.0) for ts, prev, n, ch in rows]
    stale = [{"snapshot_ts": ts.isoformat(), "previous": prev.isoformat(), "changed_share": round(s, 4)}
             for ts, prev, s in shares if s < cfg["min_changed_share"]]
    lo = min((s for *_, s in shares), default=None)
    hi = max((s for *_, s in shares), default=None)
    return _result("stale_snapshot", cfg, not stale, None if lo is None else round(lo, 4), cfg["min_changed_share"],
                   {"pairs_compared": len(rows), "stale": stale,
                    "changed_share_range": [None if lo is None else round(lo, 4), None if hi is None else round(hi, 4)]})


def check_cadence(con, cfg):
    rows = con.execute("select snapshot_ts, prev_snapshot_ts, gap_minutes from silver.dim_snapshot "
                       "where prev_snapshot_ts is not null order by snapshot_ts").fetchall()
    gaps = [float(g) for *_, g in rows]
    long_ = [{"from": p.isoformat(), "to": t.isoformat(), "minutes": round(float(g), 1)}
             for t, p, g in rows if g > cfg["max_gap_minutes"]]
    short = [{"from": p.isoformat(), "to": t.isoformat(), "minutes": round(float(g), 1)}
             for t, p, g in rows if g < cfg["min_gap_minutes"]]
    regular = sorted(g for g in gaps if cfg["min_gap_minutes"] <= g <= cfg["max_gap_minutes"])
    median = regular[len(regular) // 2] if regular else None
    return _result("cadence_gap", cfg, not long_ and not short, len(long_) + len(short),
                   {"min_minutes": cfg["min_gap_minutes"], "max_minutes": cfg["max_gap_minutes"]},
                   {"intervals": len(gaps), "long_gaps": long_, "short_gaps": short,
                    "regular_intervals": len(regular),
                    "regular_minutes_min_median_max": [round(regular[0], 1), round(median, 1), round(regular[-1], 1)]
                    if regular else None})


def check_unknown_links(con, cfg):
    ids = [r[0] for r in con.execute(
        "select distinct o.link_id from silver.stg_speedband_obs o anti join silver.dim_link l using (link_id) "
        "order by 1").fetchall()]
    rows = con.execute("select count(*) from silver.stg_speedband_obs o anti join silver.dim_link l using (link_id)"
                       ).fetchone()[0]
    return _result("unknown_link_ids", cfg, len(ids) <= cfg["max_count"], len(ids), cfg["max_count"],
                   {"link_ids": ids[:20], "rows": rows})


def check_unobserved(con, cfg):
    n, total = con.execute(
        "select (select count(*) from silver.dim_link l anti join silver.stg_speedband_obs o using (link_id)), "
        "(select count(*) from silver.dim_link)").fetchone()
    return _result("unobserved_metadata_links", cfg, True, n, None, {"metadata_links": total})


def check_unknown_band(con, cfg):
    n, total, links = con.execute(
        "select count(*) filter (where is_band_unknown), count(*), "
        "count(distinct link_id) filter (where is_band_unknown) from silver.stg_speedband_obs").fetchone()
    share = n / total if total else 0.0
    return _result("unknown_band_share", cfg, share <= cfg["max_share"], round(share, 6), cfg["max_share"],
                   {"rows": n, "links": links, "of_rows": total})


def check_sampler_log(settings, con, cfg):
    log = Path(settings.source_dir) / "speedbands" / "sampler.log"
    files = sorted((Path(settings.source_dir) / "speedbands").glob("sb_*.csv.gz"))
    if not log.exists():
        return _result("sampler_log_consistency", cfg, False, None, None, {"note": "sampler.log not found"})
    lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
    fetched = [int(m.group(1)) for x in lines if (m := re.search(r"speed bands (\d+)", x))]
    other = [x.strip() for x in lines if x.strip() and not re.search(r"speed bands \d+", x)]
    has_ts = any(re.search(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}", x) for x in lines)
    return _result("sampler_log_consistency", cfg, len(fetched) == len(files) and not other,
                   len(fetched), len(files),
                   {"log_fetch_lines": len(fetched), "snapshot_files": len(files),
                    "row_counts_in_log": sorted(set(fetched)), "other_lines": other[:10],
                    "log_has_timestamps": has_ts})


def check_od_vs_tapin(con, cfg):
    rows = con.execute("""
        with t as (select year_month, day_type, sum(tap_in) as tap_in from silver.stg_stop_pv group by all),
             o as (select year_month, day_type, sum(trips) as trips from silver.stg_od group by all)
        select year_month, day_type, tap_in, trips from t join o using (year_month, day_type)
        order by year_month, day_type""").fetchall()
    out = [{"year_month": ym, "day_type": d, "tap_in": int(t), "od_trips": int(o),
            "relative_diff": round(abs(o - t) / t, 6) if t else None} for ym, d, t, o in rows]
    worst = max((r["relative_diff"] or 0 for r in out), default=None)
    return _result("od_vs_tapin", cfg, bool(out) and worst <= cfg["max_relative_diff"], worst,
                   cfg["max_relative_diff"], {"by_month_day_type": out})


def check_missing_stops(con, cfg):
    n_stops, taps, total = con.execute("""
        select count(distinct p.stop_code) filter (where s.stop_code is null),
               sum(p.tap_in) filter (where s.stop_code is null), sum(p.tap_in)
        from silver.stg_stop_pv p left join silver.dim_bus_stop s using (stop_code)""").fetchone()
    share = (taps or 0) / total if total else 0.0
    return _result("pv_stops_missing_from_master", cfg, share <= cfg["max_share_of_tap_ins"], round(share, 6),
                   cfg["max_share_of_tap_ins"], {"stops": n_stops, "tap_ins": int(taps or 0)})


def check_quarantine(settings, cfg):
    path = Path(settings.results_dir) / "batch_ingest.json"
    if not path.exists():
        return _result("quarantine_share", cfg, False, None, cfg["max_share"], {"note": "batch_ingest.json missing"})
    loads = json.loads(path.read_text(encoding="utf-8"))["loads"]
    read = sum(x["rows_read"] for x in loads)
    q = sum(x["rows_quarantined"] for x in loads)
    reasons, sums = {}, {}
    for x in loads:
        for k, v in (x.get("quarantine_reasons") or {}).items():
            reasons[k] = reasons.get(k, 0) + v
        for k, v in (x.get("quarantined_sums") or {}).items():
            sums[k] = sums.get(k, 0) + v
    share = q / read if read else 0.0
    return _result("quarantine_share", cfg, share <= cfg["max_share"], round(share, 8), cfg["max_share"],
                   {"rows_read": read, "rows_quarantined": q, "reasons": reasons, "volume_in_quarantined_rows": sums})


def run(settings, strict: bool = False) -> tuple[list[dict], bool]:
    cfg = yaml.safe_load(THRESHOLDS.read_text(encoding="utf-8"))
    con = duckdb_connect(settings, settings.warehouse, read_only=True)
    results = [
        check_snapshot_completeness(con, cfg["snapshot_completeness"]),
        check_stale(con, cfg["stale_snapshot"]),
        check_cadence(con, cfg["cadence_gap"]),
        check_unknown_links(con, cfg["unknown_link_ids"]),
        check_unobserved(con, cfg["unobserved_metadata_links"]),
        check_unknown_band(con, cfg["unknown_band_share"]),
        check_sampler_log(settings, con, cfg["sampler_log_consistency"]),
        check_od_vs_tapin(con, cfg["od_vs_tapin"]),
        check_missing_stops(con, cfg["pv_stops_missing_from_master"]),
        check_quarantine(settings, cfg["quarantine_share"]),
    ]
    con.close()
    failed = any(r["status"] == "FAIL" for r in results)
    out = Path(settings.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "quality_report.json").write_text(json.dumps({"checks": results, "failed": failed}, indent=2,
                                                        default=str), encoding="utf-8")
    (out / "quality_report.md").write_text(to_markdown(results), encoding="utf-8")
    return results, (failed and strict)


def to_markdown(results) -> str:
    lines = ["# Data-quality report", "", "Generated by `python -m lakehouse quality`. Thresholds: "
             "`config/quality_thresholds.yml`.", "",
             "| Check | Severity | Status | Value | Threshold |", "|---|---|---|---|---|"]
    for r in results:
        lines.append(f"| {r['check']} | {r['severity']} | {r['status']} | {r['value']} | {r['threshold']} |")
    lines.append("")
    for r in results:
        lines += [f"## {r['check']}", "", r["why"], "", "```json", json.dumps(r["details"], indent=2, default=str),
                  "```", ""]
    return "\n".join(lines)
