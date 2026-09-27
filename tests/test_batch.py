import json
import shutil

import pytest

from lakehouse import batch_ingest
from lakehouse.batch_ingest import PV_OD, PV_STOP, SchemaDriftError, ingest_csv, ingest_static
from lakehouse.storage import Lake, duckdb_connect

from conftest import FIXTURE


@pytest.fixture
def lake_con(settings):
    lake = Lake(settings)
    con = duckdb_connect(settings)
    yield lake, con
    con.close()


def test_pv_quarantine_reasons(lake_con):
    lake, con = lake_con
    r = ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    assert (r["rows_read"], r["rows_valid"], r["rows_quarantined"]) == (11, 6, 5)
    assert r["quarantine_reasons"] == {"bad_day_type": 1, "bad_hour": 1, "bad_pt_code": 1,
                                       "missing_or_uncastable_time_per_hour": 1, "year_month_mismatch": 1}
    assert r["valid_sums"] == {"TOTAL_TAP_IN_VOLUME": 8720, "TOTAL_TAP_OUT_VOLUME": 3665}
    assert r["quarantined_sums"] == {"TOTAL_TAP_IN_VOLUME": 25, "TOTAL_TAP_OUT_VOLUME": 25}
    q = con.execute(f"select count(*) from read_parquet('{lake.uri('bronze/_quarantine/pv_stop')}/**/*.parquet')"
                    ).fetchone()[0]
    assert q == 5


def test_stop_codes_keep_leading_zeros_and_types(lake_con):
    lake, con = lake_con
    ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    rows = con.execute(f"select PT_CODE, typeof(TIME_PER_HOUR), typeof(TOTAL_TAP_IN_VOLUME) from "
                       f"read_parquet('{lake.uri('bronze/pv_stop')}/**/*.parquet') order by 1").fetchall()
    assert rows[0] == ("01012", "TINYINT", "BIGINT")
    assert lake.list_files("bronze/pv_stop") == ["bronze/pv_stop/month=2026-08/part-0.parquet"]


def test_od_negative_trips_quarantined(lake_con):
    lake, con = lake_con
    r = ingest_csv(con, lake, PV_OD, FIXTURE / "origin_destination_bus_202608.csv")
    assert r["quarantine_reasons"] == {"negative_total_trips": 1}
    assert r["valid_sums"] == {"TOTAL_TRIPS": 8720}


def test_reload_replaces_partition(lake_con):
    lake, con = lake_con
    for _ in range(2):
        ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    n = con.execute(f"select count(*) from read_parquet('{lake.uri('bronze/pv_stop')}/**/*.parquet')").fetchone()[0]
    assert n == 6


def test_header_drift_stops_the_load(lake_con, tmp_path):
    lake, con = lake_con
    src = (FIXTURE / "transport_node_bus_202608.csv").read_text(encoding="utf-8")
    p = tmp_path / "transport_node_bus_202608.csv"
    p.write_text(src.replace("PT_CODE", "STOP_CODE", 1), encoding="utf-8")
    with pytest.raises(SchemaDriftError):
        ingest_csv(con, lake, PV_STOP, p)


def test_static_json_key_drift(lake_con, tmp_path):
    lake, con = lake_con
    rows = json.loads((FIXTURE / "BusStops_20260926.json").read_text(encoding="utf-8"))
    rows[0]["Lat"] = rows[0].pop("Latitude")
    p = tmp_path / "BusStops_20260926.json"
    p.write_text(json.dumps(rows), encoding="utf-8")
    with pytest.raises(SchemaDriftError):
        ingest_static(con, lake, "bus_stops", p, "20260926")


def test_run_ingests_latest_static_pull_only(settings, tmp_path):
    src = tmp_path / "src"
    shutil.copytree(FIXTURE, src)
    old = json.loads((src / "BusStops_20260926.json").read_text(encoding="utf-8"))[:2]
    (src / "BusStops_20260401.json").write_text(json.dumps(old), encoding="utf-8")
    loads = batch_ingest.run(settings.__class__(**{**settings.__dict__, "source_dir": src}), log=lambda *_: None)
    stops = [x for x in loads if x["dataset"] == "bus_stops"]
    assert [x["file"] for x in stops] == ["BusStops_20260926.json"] and stops[0]["rows_valid"] == 5
