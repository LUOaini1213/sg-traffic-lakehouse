"""End-to-end on the fixture: ingest -> profile -> dbt build (models + dbt tests) -> quality -> publish -> report.

Expected values are worked out by hand from tests/make_fixture.py.
"""
import datetime as dt
import hashlib
import json

import duckdb
import pytest

from lakehouse import quality
from lakehouse.storage import Lake

D = dt.datetime


@pytest.fixture(scope="module")
def run(pipeline_run):
    return pipeline_run[0]


@pytest.fixture(scope="module")
def con(run):
    c = duckdb.connect(str(run.warehouse), read_only=True)
    yield c
    c.close()


def q(con, sql):
    return con.execute(sql).fetchall()


def results(run, name):
    return json.loads((run.results_dir / name).read_text(encoding="utf-8"))


def test_dbt_build_ran_all_tests(run):
    rr = json.loads((run.warehouse.parent / "dbt_target" / "run_results.json").read_text(encoding="utf-8"))
    status = {}
    for r in rr["results"]:
        status[r["status"]] = status.get(r["status"], 0) + 1
    tests = [r for r in rr["results"] if r["unique_id"].startswith("test.")]
    assert len(tests) >= 60
    assert status.get("error", 0) == 0 and status.get("fail", 0) == 0
    warned = sorted(r["unique_id"].split(".")[2] for r in rr["results"] if r["status"] == "warn")
    # link 999 (not in the metadata) and stop 99999 (not in the stop list) are known fixture defects; the fixture
    # has a January 2027 file and the holiday seed has no 2027 list
    assert len(warned) == 4 and sum(w.startswith("relationships_") for w in warned) == 3
    assert "assert_calendar_years_have_holidays" in warned
    passed = {r["unique_id"].split(".")[2] for r in rr["results"] if r["status"] == "pass"}
    assert {"assert_gold_keeps_every_silver_row", "assert_episodes_stay_in_one_sampling_run"} <= passed


def test_congestion_episodes(con):
    got = q(con, "select link_id, start_ts, end_ts, n_snapshots, worst_band, span_minutes "
                 "from gold.gold_congestion_episodes order by link_id")
    assert got == [
        (101, D(2026, 9, 5, 8, 0), D(2026, 9, 5, 9, 0), 4, 1, 60.0),
        (105, D(2026, 9, 5, 11, 0), D(2026, 9, 5, 12, 0), 4, 1, 60.0),
        (111, D(2026, 9, 5, 8, 20), D(2026, 9, 5, 9, 0), 3, 2, 40.0),
    ]


def test_snapshot_dimension(con):
    got = q(con, "select snapshot_seq, sampling_run, gap_minutes, day_type from silver.dim_snapshot order by 1")
    assert [r[1] for r in got] == [1, 1, 1, 1, 1, 2, 2, 2, 2, 3]
    assert got[5][2] == 100.0 and got[9][2] == 2635.0
    assert got[9][3] == "WEEKDAY" and got[0][3] == "WEEKENDS/HOLIDAY"


def test_planted_bronze_duplicate_is_removed_in_silver_first_copy_wins(con, run):
    from conftest import PLANTED
    prof = results(run, "profile_bronze.json")
    assert {d["dataset"]: d["duplicate_keys"] for d in prof["datasets"]}["speedbands"] == 1
    got = q(con, "select speed_band, ingest_source from silver.stg_speedband_obs "
                 f"where link_id = {PLANTED['link_id']} and snapshot_ts = '{PLANTED['snapshot_ts']}'")
    assert got == [(2, "file")]
    assert q(con, "select count(*), count(distinct (snapshot_ts, link_id)) from silver.stg_speedband_obs") == [(120, 120)]


def test_calendar_is_derived_from_the_data(con):
    got = q(con, "select min(cal_date), max(cal_date), count(*) from silver.dim_calendar")
    assert got == [(dt.date(2026, 1, 1), dt.date(2027, 12, 31), 730)]
    assert q(con, "select day_type, n_days from silver.dim_month_day_counts where year_month = '2027-01' "
                  "order by 1") == [("WEEKDAY", 21), ("WEEKENDS/HOLIDAY", 10)]


def test_band_zero_is_null_in_silver(con):
    assert q(con, "select speed_band, is_band_unknown, speed_lower_kmh from silver.stg_speedband_obs "
                  "where link_id = 110 and snapshot_ts = '2026-09-05 08:00'") == [(None, True, None)]
    assert q(con, "select speed_upper_kmh from silver.stg_speedband_obs "
                  "where link_id = 107 and snapshot_ts = '2026-09-07 07:55'") == [(None,)]


def test_link_speed_hourly(con):
    got = q(con, "select day_type, hour_of_day, n_obs, mean_band, min_band, max_band, share_congested, "
                 "mean_speed_lower_kmh from gold.gold_link_speed_hourly where link_id = 101 order by 1, 2")
    assert got == [
        ("WEEKDAY", 7, 1, 5.0, 5, 5, 0.0, 40.0),
        ("WEEKENDS/HOLIDAY", 8, 3, 1.6667, 1, 2, 1.0, 6.67),
        ("WEEKENDS/HOLIDAY", 9, 2, 3.5, 2, 5, 0.5, 25.0),
        ("WEEKENDS/HOLIDAY", 11, 3, 6.0, 6, 6, 0.0, 50.0),
        ("WEEKENDS/HOLIDAY", 12, 1, 6.0, 6, 6, 0.0, 50.0),
    ]
    # band 0 is excluded from the statistics, not counted as band 0
    assert q(con, "select n_obs, min_band from gold.gold_link_speed_hourly "
                  "where link_id = 110 and hour_of_day = 8") == [(2, 3)]


def test_link_hour_features(con):
    got = q(con, "select obs_hour_ts, iso_dow, is_weekend_or_holiday, n_obs, mean_band, band_std, "
                 "prev_hour_mean_band, next_hour_mean_band from gold.gold_link_hour_features "
                 "where link_id = 101 order by 1")
    assert got == [
        (D(2026, 9, 5, 8), 6, True, 3, 1.6667, 0.5774, None, 3.5),
        (D(2026, 9, 5, 9), 6, True, 2, 3.5, 2.1213, 1.6667, None),
        (D(2026, 9, 5, 11), 6, True, 3, 6.0, 0.0, None, 6.0),
        (D(2026, 9, 5, 12), 6, True, 1, 6.0, None, 6.0, None),
        (D(2026, 9, 7, 7), 1, False, 1, 5.0, None, None, None),
    ]
    assert q(con, "select count(*) from gold.gold_link_hour_features where link_id = 999") == [(0,)]


def test_stop_demand_uses_day_counts(con):
    got = q(con, "select year_month, day_type, hour, stop_code, n_days, avg_daily_tap_in, avg_daily_tap_out "
                 "from gold.gold_stop_demand_hourly where stop_code = '01012' order by 1, 2, 3")
    assert got == [
        ("2026-07", "WEEKDAY", 8, "01012", 23, 210.0, 13.04),
        ("2026-07", "WEEKENDS/HOLIDAY", 8, "01012", 8, 100.0, 5.0),
        ("2026-08", "WEEKDAY", 7, "01012", 20, 100.0, 5.0),
        ("2026-08", "WEEKDAY", 8, "01012", 20, 200.0, 15.0),
        ("2026-08", "WEEKENDS/HOLIDAY", 8, "01012", 11, 100.0, 5.0),
        ("2027-01", "WEEKDAY", 8, "01012", 21, 100.0, 10.0),          # a 2027 file reaches gold
        ("2027-01", "WEEKENDS/HOLIDAY", 8, "01012", 10, 50.0, 5.0),
    ]


def test_top_od_flows_rank_and_ties(con):
    got = q(con, "select flow_rank, origin_stop_code, destination_stop_code, trips, avg_daily_trips "
                 "from gold.gold_top_od_flows where day_type = 'WEEKDAY' order by flow_rank")
    assert got == [(1, "01012", "01013", 3500, 175.0), (2, "01013", "01012", 1500, 75.0),
                   (3, "01012", "10009", 1000, 50.0), (4, "01013", "10009", 1000, 50.0),
                   (5, "10009", "01012", 600, 30.0), (6, "99999", "01012", 20, 1.0)]


def test_quality_findings(run):
    checks = {c["check"]: c for c in results(run, "quality_report.json")["checks"]}
    assert checks["snapshot_completeness"]["status"] == "FAIL"
    assert [x["snapshot_ts"] for x in checks["snapshot_completeness"]["details"]["incomplete"]] == \
        ["2026-09-05T11:20:00"]
    assert [x["snapshot_ts"] for x in checks["stale_snapshot"]["details"]["stale"]] == ["2026-09-05T12:00:00"]
    assert [x["minutes"] for x in checks["cadence_gap"]["details"]["long_gaps"]] == [100.0, 2635.0]
    assert checks["cadence_gap"]["details"]["short_gaps"] == []
    assert checks["unknown_link_ids"]["details"]["link_ids"] == [999]
    assert checks["unknown_band_share"]["details"]["rows"] == 1
    assert (checks["sampler_log_consistency"]["value"], checks["sampler_log_consistency"]["threshold"]) == (9, 10)
    assert checks["sampler_log_consistency"]["status"] == "INFO"
    assert checks["od_vs_tapin"]["status"] == "PASS" and checks["od_vs_tapin"]["value"] == 0.0
    assert checks["pv_stops_missing_from_master"]["details"] == {"stops": 1, "tap_ins": 20}
    qd = checks["quarantine_share"]["details"]
    assert qd["rows_quarantined"] == 11  # pv_stop 8, pv_od 1, speed bands 2
    assert qd["reasons"]["pv_stop: malformed_row"] == 2 and qd["reasons"]["speedbands: malformed_row"] == 1


def test_quality_thresholds_move_the_verdicts(run, tmp_path, monkeypatch):
    """The thresholds file really drives the checks: relax two of them and the verdicts change."""
    import yaml
    cfg = yaml.safe_load(quality.THRESHOLDS.read_text(encoding="utf-8"))
    cfg["snapshot_completeness"]["min_ratio"] = 0.9
    cfg["cadence_gap"]["max_gap_minutes"] = 3000
    p = tmp_path / "t.yml"
    p.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    monkeypatch.setattr(quality, "THRESHOLDS", p)
    s = run.__class__(**{**run.__dict__, "results_dir": tmp_path})
    res, _ = quality.run(s)
    checks = {c["check"]: c["status"] for c in res}
    assert checks["snapshot_completeness"] == "PASS" and checks["cadence_gap"] == "PASS"


def test_strict_quality_fails_on_error_checks(run, tmp_path):
    s = run.__class__(**{**run.__dict__, "results_dir": tmp_path})
    (tmp_path / "batch_ingest.json").write_text((run.results_dir / "batch_ingest.json").read_text())
    assert quality.run(s, strict=True)[1] is True
    assert quality.run(s, strict=False)[1] is False


def test_profile_reconciles(run):
    prof = results(run, "profile_bronze.json")
    assert prof["all_reconciled"] and len(prof["reconciliation"]) == 5  # 3 stop files, 1 OD file, links
    assert len(prof["speedband_reconciliation"]) == 10 and all(r["match"] for r in prof["speedband_reconciliation"])
    by = {d["dataset"]: d for d in prof["datasets"]}
    assert by["speedbands"]["rows"] == 121  # 120 + the planted duplicate
    assert by["pv_stop"]["rows"] == 10 and by["pv_od"]["rows"] == 9
    rec = {r["file"]: r for r in prof["reconciliation"]}["transport_node_bus_202608.csv"]
    assert (rec["source_rows"], rec["bronze_rows"], rec["quarantined_rows"]) == (14, 6, 8)


def test_publish_counts_and_gold_files(run):
    counts = results(run, "layer_counts.json")
    assert counts["bronze"]["speedbands"] == {"files": 31, "rows": 121}
    assert counts["bronze"]["_quarantine"]["rows"] == 11
    assert counts["gold"]["gold_congestion_episodes"] == 3
    manifest = results(run, "gold_manifest.json")
    lake = Lake(run)
    for t, m in manifest.items():
        assert lake.delta_stats(m["path"])["rows"] == m["rows"] == counts["gold"][t]
        assert hashlib.sha256("".join(lake.delta_file_hashes(m["path"])).encode()).hexdigest() == m["file_sha256"]


def test_report_written(run):
    page = (run.results_dir / "report.html").read_text(encoding="utf-8")
    assert "Congestion episodes by road category" in page and "data:image/png;base64," in page
