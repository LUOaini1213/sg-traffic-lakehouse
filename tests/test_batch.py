import json
import shutil

import pyarrow as pa
import pytest

from lakehouse import batch_ingest, profiling
from lakehouse.batch_ingest import PV_OD, PV_STOP, SchemaDriftError, count_source, ingest_csv, ingest_links, ingest_static
from lakehouse.storage import Lake, duckdb_connect

from conftest import FIXTURE

PV_HEADER = "YEAR_MONTH,DAY_TYPE,TIME_PER_HOUR,PT_TYPE,PT_CODE,TOTAL_TAP_IN_VOLUME,TOTAL_TAP_OUT_VOLUME\n"


@pytest.fixture
def lake_con(settings):
    lake = Lake(settings)
    con = duckdb_connect(settings)
    yield lake, con
    con.close()


def rows_of(con, lake, rel, where=""):
    return con.execute(f"select count(*) from {lake.scan(rel)} {where}").fetchone()[0]


def test_pv_quarantine_reasons(lake_con):
    lake, con = lake_con
    r = ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    assert (r["rows_read"], r["rows_valid"], r["rows_quarantined"]) == (14, 6, 8)
    assert r["quarantine_reasons"] == {"bad_day_type": 1, "bad_hour": 1, "bad_pt_code": 1, "malformed_row": 2,
                                       "missing_or_uncastable_time_per_hour": 1,
                                       "missing_or_uncastable_total_tap_in_volume": 1, "year_month_mismatch": 1}
    assert r["valid_sums"] == {"TOTAL_TAP_IN_VOLUME": 8720, "TOTAL_TAP_OUT_VOLUME": 3665}
    assert r["quarantined_sums"] == {"TOTAL_TAP_IN_VOLUME": 25, "TOTAL_TAP_OUT_VOLUME": 30}
    assert rows_of(con, lake, "bronze/_quarantine/pv_stop") == 8
    raw = con.execute(f"select _raw_line from {lake.scan('bronze/_quarantine/pv_stop')} "
                      "where _reject = 'malformed_row' order by 1").fetchall()
    assert raw == [("2026-08,WEEKDAY,8,BUS,01012,5,5,x",), ("2026-08,WEEKDAY,8,BUS,01013",)]


def test_decimal_volume_is_rejected_not_rounded(lake_con, tmp_path):
    """TRY_CAST('5.5' AS BIGINT) is 6 in DuckDB; the integer-format rule quarantines the row instead."""
    lake, con = lake_con
    p = tmp_path / "transport_node_bus_202608.csv"
    p.write_text(PV_HEADER + "2026-08,WEEKDAY,8,BUS,01012,5.5,1\n2026-08,WEEKDAY,8,BUS,01013, 7 ,1\n")
    r = ingest_csv(con, lake, PV_STOP, p)
    assert r["quarantine_reasons"] == {"missing_or_uncastable_total_tap_in_volume": 1}
    assert r["valid_sums"]["TOTAL_TAP_IN_VOLUME"] == 7


def test_stop_codes_keep_leading_zeros_and_types(lake_con):
    lake, con = lake_con
    ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    rows = con.execute(f"select PT_CODE, typeof(TIME_PER_HOUR), typeof(TOTAL_TAP_IN_VOLUME), month from "
                       f"{lake.scan('bronze/pv_stop')} order by 1").fetchall()
    assert rows[0] == ("01012", "TINYINT", "BIGINT", "2026-08")


def test_od_negative_trips_quarantined(lake_con):
    lake, con = lake_con
    r = ingest_csv(con, lake, PV_OD, FIXTURE / "origin_destination_bus_202608.csv")
    assert r["quarantine_reasons"] == {"negative_total_trips": 1}
    assert r["valid_sums"] == {"TOTAL_TRIPS": 8720}


def test_reload_replaces_only_its_month(lake_con):
    lake, con = lake_con
    for _ in range(2):
        ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202607.csv")
    assert rows_of(con, lake, "bronze/pv_stop", "where month = '2026-08'") == 6
    assert rows_of(con, lake, "bronze/pv_stop", "where month = '2026-07'") == 2


def test_failed_reload_keeps_the_previous_month(lake_con, monkeypatch):
    """The reviewer's reproduction: the old load deleted the month's prefix, then ran COPY; a failed COPY left the
    month empty. Now the reload is one Delta commit, and a load that dies half-way commits nothing."""
    lake, con = lake_con
    ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    real = batch_ingest._reader

    def failing(con_, sql):
        r = real(con_, sql)
        first = r.read_next_batch()

        def gen():
            yield first
            raise OSError("disk full half-way through the load")
        return pa.RecordBatchReader.from_batches(r.schema, gen())

    monkeypatch.setattr(batch_ingest, "_reader", failing)
    with pytest.raises(Exception):
        ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    assert rows_of(con, lake, "bronze/pv_stop", "where month = '2026-08'") == 6


def test_reload_clears_stale_quarantine(lake_con, tmp_path):
    lake, con = lake_con
    p = tmp_path / "transport_node_bus_202608.csv"
    p.write_text(PV_HEADER + "2026-08,WEEKDAY,8,BUS,01012,5,1\n2026-08,SUNDAY,8,BUS,01013,7,1\n")
    ingest_csv(con, lake, PV_STOP, p)
    assert rows_of(con, lake, "bronze/_quarantine/pv_stop") == 1
    p.write_text(PV_HEADER + "2026-08,WEEKDAY,8,BUS,01012,5,1\n2026-08,WEEKDAY,8,BUS,01013,7,1\n")  # fixed upstream
    ingest_csv(con, lake, PV_STOP, p)
    assert rows_of(con, lake, "bronze/_quarantine/pv_stop") == 0
    assert rows_of(con, lake, "bronze/pv_stop") == 2


def test_header_drift_stops_the_load(lake_con, tmp_path):
    lake, con = lake_con
    src = (FIXTURE / "transport_node_bus_202608.csv").read_text(encoding="utf-8")
    p = tmp_path / "transport_node_bus_202608.csv"
    p.write_text(src.replace("PT_CODE", "STOP_CODE", 1), encoding="utf-8")
    with pytest.raises(SchemaDriftError):
        ingest_csv(con, lake, PV_STOP, p)


def test_links_bad_rows_are_quarantined(lake_con, tmp_path):
    lake, con = lake_con
    p = tmp_path / "speedband_links.csv"
    p.write_text("LinkID,RoadName,RoadCategory,StartLat,StartLon,EndLat,EndLon\n"
                 "1,A ROAD,1,1.3,103.8,1.31,103.8\n"
                 "2,B ROAD,x,1.3,103.8,1.31,103.8\n"        # category not a number
                 "3,C ROAD,1,1.3,103.8,1.31,103.8,extra\n"  # extra field
                 "4,D ROAD,2,1.3,103.8,1.31,103.8\n")
    r = ingest_links(con, lake, p)
    assert (r["rows_read"], r["rows_valid"], r["rows_quarantined"]) == (4, 2, 2)
    assert r["source"]["rows"] == 4
    assert rows_of(con, lake, "bronze/_quarantine/speedband_links") == 2


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


def test_source_count_is_independent_of_duckdb():
    c = count_source(FIXTURE / "transport_node_bus_202608.csv", list(PV_STOP.columns), PV_STOP.volume_columns)
    assert (c["rows"], c["well_formed_rows"]) == (14, 12)
    # valid 8720 + quarantined 25 (the '5.5' is not an integer and is not summed); 3665 + 30
    assert c["sums"] == {"TOTAL_TAP_IN_VOLUME": 8745, "TOTAL_TAP_OUT_VOLUME": 3695}
    assert c["sha256"] == batch_ingest.sha256_file(FIXTURE / "transport_node_bus_202608.csv")


def test_reconciliation_catches_rows_lost_after_the_load(lake_con):
    """Reconciliation compares the lake as it is now with the source counts; rows deleted from bronze after the
    load (or never written) must show up as a mismatch."""
    lake, con = lake_con
    load = ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    assert profiling.reconcile(con, lake, [load])[0]["match"] is True
    lake.delta("bronze/pv_stop").delete("PT_CODE = '10009'")
    rec = profiling.reconcile(con, lake, [load])[0]
    assert rec["match"] is False and rec["bronze_rows"] == 5 and rec["source_rows"] == 14


def test_reconciliation_catches_a_quarantine_that_was_not_written(lake_con, monkeypatch):
    lake, con = lake_con
    monkeypatch.setattr(batch_ingest, "_write_quarantine", lambda *a, **k: 0)
    load = ingest_csv(con, lake, PV_STOP, FIXTURE / "transport_node_bus_202608.csv")
    assert profiling.reconcile(con, lake, [load])[0]["match"] is False
