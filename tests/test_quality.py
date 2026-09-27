"""Boundary tests for every data-quality check, on small hand-built silver tables (no dbt run needed).

Each check is run once exactly at its threshold (must pass) and once just past it (must fail/warn), so a check that
can never fail, or whose comparison is off by one, is caught."""
import datetime as dt
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
import yaml

from lakehouse import quality

REPO = Path(__file__).resolve().parents[1]
T0 = dt.datetime(2026, 9, 26, 19, 0)


def cfg(**kw):
    return {"severity": "error", "why": "", **kw}


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("create schema silver")
    c.execute("create table silver.dim_link (link_id bigint)")
    c.execute("create table silver.stg_speedband_obs (snapshot_ts timestamp, link_id bigint, speed_band smallint, "
              "is_band_unknown boolean)")
    c.execute("create table silver.dim_snapshot (snapshot_ts timestamp, snapshot_seq bigint, "
              "prev_snapshot_ts timestamp, gap_minutes double)")
    c.execute("create table silver.stg_stop_pv (year_month varchar, day_type varchar, stop_code varchar, tap_in bigint)")
    c.execute("create table silver.stg_od (year_month varchar, day_type varchar, trips bigint)")
    c.execute("create table silver.dim_bus_stop (stop_code varchar)")
    yield c
    c.close()


def snapshots(con, bands_by_snapshot, links=None):
    """bands_by_snapshot: list of {link: band} per snapshot, 20 minutes apart."""
    links = links if links is not None else sorted({k for d in bands_by_snapshot for k in d})
    link_tbl = pa.table({"link_id": pa.array(links, pa.int64())})  # noqa: F841 (read by DuckDB below)
    con.execute("insert into silver.dim_link select * from link_tbl")
    prev = None
    for i, d in enumerate(bands_by_snapshot):
        ts = T0 + dt.timedelta(minutes=20 * i)
        con.execute("insert into silver.dim_snapshot values (?, ?, ?, ?)", [ts, i + 1, prev, 20.0 if prev else None])
        obs = pa.table({"snapshot_ts": pa.array([ts] * len(d), pa.timestamp("us")),  # noqa: F841
                        "link_id": pa.array(list(d), pa.int64()), "speed_band": pa.array(list(d.values()), pa.int16()),
                        "is_band_unknown": pa.array([not 1 <= b <= 8 for b in d.values()])})
        con.execute("insert into silver.stg_speedband_obs select * from obs")
        prev = ts


def test_completeness_at_and_below_threshold(con):
    snapshots(con, [{k: 3 for k in range(99)}], links=list(range(100)))
    assert quality.check_snapshot_completeness(con, cfg(min_ratio=0.99))["status"] == "PASS"
    assert quality.check_snapshot_completeness(con, cfg(min_ratio=0.991))["status"] == "FAIL"


def test_stale_at_and_below_threshold(con):
    a = {k: 3 for k in range(100)}
    b = {k: (4 if k < 5 else 3) for k in range(100)}  # 5% changed
    snapshots(con, [a, b])
    assert quality.check_stale(con, cfg(min_changed_share=0.05))["status"] == "PASS"
    r = quality.check_stale(con, cfg(min_changed_share=0.051))
    assert r["status"] == "FAIL" and len(r["details"]["stale"]) == 1


@pytest.mark.parametrize("gap,expect", [(30.0, "PASS"), (30.5, "FAIL"), (10.0, "PASS"), (9.9, "FAIL")])
def test_cadence_bounds(con, gap, expect):
    con.execute("insert into silver.dim_snapshot values (?, 1, null, null), (?, 2, ?, ?)",
                [T0, T0 + dt.timedelta(minutes=gap), T0, gap])
    r = quality.check_cadence(con, cfg(min_gap_minutes=10, max_gap_minutes=30))
    assert r["status"] == expect


@pytest.mark.parametrize("unknown,max_count,expect", [(0, 0, "PASS"), (1, 0, "FAIL"), (1, 1, "PASS"), (2, 1, "FAIL")])
def test_unknown_link_ids(con, unknown, max_count, expect):
    snapshots(con, [{**{k: 3 for k in range(10)}, **{1000 + i: 3 for i in range(unknown)}}], links=list(range(10)))
    r = quality.check_unknown_links(con, cfg(max_count=max_count))
    assert (r["status"], r["value"]) == (expect, unknown)


@pytest.mark.parametrize("n_zero,expect", [(1, "PASS"), (2, "FAIL")])
def test_unknown_band_share(con, n_zero, expect):
    snapshots(con, [{k: (0 if k < n_zero else 3) for k in range(1000)}])
    assert quality.check_unknown_band(con, cfg(max_share=0.001))["status"] == expect


@pytest.mark.parametrize("trips,expect", [(1001, "PASS"), (999, "PASS"), (1002, "FAIL"), (998, "FAIL")])
def test_od_vs_tapin(con, trips, expect):
    con.execute("insert into silver.stg_stop_pv values ('2026-08', 'WEEKDAY', '01012', 1000)")
    con.execute("insert into silver.stg_od values ('2026-08', 'WEEKDAY', ?)", [trips])
    assert quality.check_od_vs_tapin(con, cfg(max_relative_diff=0.001))["status"] == expect


def test_od_vs_tapin_fails_without_overlap(con):
    con.execute("insert into silver.stg_stop_pv values ('2026-08', 'WEEKDAY', '01012', 1000)")
    con.execute("insert into silver.stg_od values ('2026-07', 'WEEKDAY', 1000)")
    assert quality.check_od_vs_tapin(con, cfg(max_relative_diff=0.001))["status"] == "FAIL"


@pytest.mark.parametrize("missing_taps,expect", [(1, "PASS"), (2, "FAIL")])
def test_missing_stops(con, missing_taps, expect):
    con.execute("insert into silver.dim_bus_stop values ('01012')")
    con.execute("insert into silver.stg_stop_pv values ('2026-08', 'WEEKDAY', '01012', ?), "
                "('2026-08', 'WEEKDAY', '99999', ?)", [100 - missing_taps, missing_taps])
    r = quality.check_missing_stops(con, cfg(max_share_of_tap_ins=0.01))
    assert r["status"] == expect and r["details"]["stops"] == 1


@pytest.mark.parametrize("quarantined,expect", [(1, "PASS"), (2, "FAIL")])
def test_quarantine_share(tmp_path, quarantined, expect):
    (tmp_path / "batch_ingest.json").write_text(json.dumps({"loads": [
        {"dataset": "pv_stop", "rows_read": 9000, "rows_quarantined": quarantined,
         "quarantine_reasons": {"bad_hour": quarantined}}]}))
    (tmp_path / "speedband_files.json").write_text(json.dumps({"snapshots": [
        {"source_rows": 1000, "rows_quarantined": 0, "quarantine_reasons": {}}]}))
    s = type("S", (), {"results_dir": tmp_path})()
    r = quality.check_quarantine(s, cfg(max_share=0.0001))
    assert r["status"] == expect and r["details"]["rows_read"] == 10000


def test_sampler_log_mismatch_is_information_only(tmp_path, con):
    d = tmp_path / "speedbands"
    d.mkdir()
    for name in ("sb_20260926_1900.csv.gz", "sb_20260926_1920.csv.gz", "sb_20260926_1940.csv.gz"):
        (d / name).write_bytes(b"")
    (d / "sampler.log").write_text("speed bands 10\n" * 2)
    s = type("S", (), {"source_dir": tmp_path})()
    thresholds = yaml.safe_load(quality.THRESHOLDS.read_text(encoding="utf-8"))
    r = quality.check_sampler_log(s, con, thresholds["sampler_log_consistency"], until=dt.datetime(2026, 9, 26, 19, 20))
    assert r["status"] == "INFO" and (r["value"], r["threshold"]) == (2, 3)
    assert r["details"]["snapshot_files_up_to_cutoff"] == 2
    (d / "sampler.log").write_text("speed bands 10\n" * 3)
    assert quality.check_sampler_log(s, con, thresholds["sampler_log_consistency"])["status"] == "PASS"


def test_warn_severity_never_fails_the_gate(con):
    snapshots(con, [{k: 3 for k in range(99)}], links=list(range(100)))
    r = quality.check_snapshot_completeness(con, {**cfg(min_ratio=0.999), "severity": "warn"})
    assert r["status"] == "WARN"


def test_dbt_sampling_gap_matches_the_cadence_threshold():
    dbt_vars = yaml.safe_load((REPO / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))["vars"]
    thresholds = yaml.safe_load(quality.THRESHOLDS.read_text(encoding="utf-8"))
    assert dbt_vars["max_gap_minutes"] == thresholds["cadence_gap"]["max_gap_minutes"]
