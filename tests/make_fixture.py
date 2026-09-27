"""Write the synthetic fixture under tests/fixtures/raw/datamall (same layout as the real DataMall pulls).

Everything here is invented; no real DataMall data is committed. The fixture plants the defects and edge cases
the tests check for:
  - 10 snapshots with a 100-minute sampling gap (09:20 -> 11:00) and a two-day gap before a Monday snapshot;
  - snapshot 12:00 is an exact copy of 11:40 (stale);
  - snapshot 11:20 lacks link 112 (incomplete); snapshot 08:40 carries link 999, absent from the metadata;
  - link 110 reports the undocumented band 0 at 08:00;
  - congestion episodes: link 101 08:00-09:00 (4 snapshots), link 111 08:20-09:00 (exactly 3), link 105
    11:00-12:00 (4); link 103 has 2 + 2 congested snapshots split by the gap (no episode); link 109 is always
    band 1 but is a small road (no episode);
  - passenger-volume files with one bad row per quarantine rule, a stop missing from the stop list, and OD totals
    that reconcile exactly with tap-ins; two OD flows tie on trips (tie broken by stop code).
Run: python tests/make_fixture.py
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "fixtures" / "raw" / "datamall"

SNAPSHOTS = [dt.datetime(2026, 9, 5, 8, 0), dt.datetime(2026, 9, 5, 8, 20), dt.datetime(2026, 9, 5, 8, 40),
             dt.datetime(2026, 9, 5, 9, 0), dt.datetime(2026, 9, 5, 9, 20), dt.datetime(2026, 9, 5, 11, 0),
             dt.datetime(2026, 9, 5, 11, 20), dt.datetime(2026, 9, 5, 11, 40), dt.datetime(2026, 9, 5, 12, 0),
             dt.datetime(2026, 9, 7, 7, 55)]

# link -> (road category, bands at the 10 snapshots; None = link missing from that snapshot)
LINKS = {
    101: (1, [2, 2, 1, 2, 5, 6, 6, 6, 6, 5]),
    102: (1, [5, 2, 2, 5, 5, 6, 6, 6, 6, 5]),
    103: (1, [5, 5, 5, 2, 2, 2, 1, 4, 4, 4]),
    104: (1, [6, 6, 6, 6, 6, 6, 6, 6, 6, 6]),
    105: (2, [4, 4, 4, 4, 4, 1, 1, 2, 2, 4]),
    106: (2, [5, 5, 5, 5, 5, 5, 5, 5, 5, 5]),
    107: (2, [7, 7, 7, 7, 7, 7, 7, 7, 7, 8]),
    108: (2, [3, 3, 3, 3, 3, 3, 3, 3, 3, 3]),
    109: (5, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
    110: (5, [0, 3, 3, 3, 3, 3, 3, 3, 3, 3]),
    111: (3, [4, 2, 2, 2, 4, 4, 4, 4, 4, 4]),
    112: (3, [5, 5, 5, 5, 5, 5, None, 5, 5, 5]),
}
UNKNOWN_LINK = (999, 2, 3)  # (link, snapshot index, band)


def band_bounds(b: int):
    if b == 0:
        return "", ""
    if b == 8:
        return 70, 999
    return (b - 1) * 10, b * 10 - 1


def write_snapshots():
    d = OUT / "speedbands"
    d.mkdir(parents=True, exist_ok=True)
    for i, ts in enumerate(SNAPSHOTS):
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["LinkID", "SpeedBand", "MinimumSpeed", "MaximumSpeed"])
        for link, (_, bands) in LINKS.items():
            if bands[i] is not None:
                w.writerow([link, bands[i], *band_bounds(bands[i])])
        if i == UNKNOWN_LINK[1]:
            w.writerow([UNKNOWN_LINK[0], UNKNOWN_LINK[2], *band_bounds(UNKNOWN_LINK[2])])
        # mtime=0 in the gzip header so regenerating the fixture gives identical bytes
        with open(d / f"sb_{ts:%Y%m%d_%H%M}.csv.gz", "wb") as raw, \
                gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as z:
            z.write(buf.getvalue().encode())
    (d / "sampler.log").write_text("speed bands 12\n" * (len(SNAPSHOTS) - 1), encoding="utf-8")


def write_links():
    with open(OUT / "speedband_links.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["LinkID", "RoadName", "RoadCategory", "StartLat", "StartLon", "EndLat", "EndLon"])
        for k, (cat, _) in LINKS.items():
            j = k - 100
            w.writerow([k, f"FIXTURE ROAD {j}", cat, round(1.30 + j * 0.001, 6), round(103.80 + j * 0.001, 6),
                        round(1.30 + j * 0.001 + 0.0005, 6), round(103.80 + j * 0.001, 6)])


PV_HEADER = ["YEAR_MONTH", "DAY_TYPE", "TIME_PER_HOUR", "PT_TYPE", "PT_CODE", "TOTAL_TAP_IN_VOLUME",
             "TOTAL_TAP_OUT_VOLUME"]
PV = {
    "202607": [
        ["2026-07", "WEEKDAY", 8, "BUS", "01012", 4830, 300],
        ["2026-07", "WEEKENDS/HOLIDAY", 8, "BUS", "01012", 800, 40],
    ],
    "202608": [
        ["2026-08", "WEEKDAY", 7, "BUS", "01012", 2000, 100],
        ["2026-08", "WEEKDAY", 8, "BUS", "01012", 4000, 300],
        ["2026-08", "WEEKDAY", 8, "BUS", "01013", 1000, 2500],
        ["2026-08", "WEEKENDS/HOLIDAY", 8, "BUS", "01012", 1100, 55],
        ["2026-08", "WEEKDAY", 9, "BUS", "10009", 600, 700],
        ["2026-08", "WEEKDAY", 9, "BUS", "99999", 20, 10],       # stop not in the stop list
        ["2026-08", "WEEKDAY", 24, "BUS", "01012", 5, 5],        # bad_hour
        ["2026-08", "SUNDAY", 8, "BUS", "01013", 5, 5],          # bad_day_type
        ["2026-08", "WEEKDAY", "x7", "BUS", "01012", 5, 5],      # uncastable hour
        ["2026-08", "WEEKDAY", 10, "BUS", "1012", 5, 5],         # bad stop code (lost leading zero)
        ["2026-07", "WEEKDAY", 10, "BUS", "01012", 5, 5],        # month does not match the file
    ],
}
OD_HEADER = ["YEAR_MONTH", "DAY_TYPE", "TIME_PER_HOUR", "PT_TYPE", "ORIGIN_PT_CODE", "DESTINATION_PT_CODE",
             "TOTAL_TRIPS"]
OD = {
    "202608": [
        ["2026-08", "WEEKDAY", 7, "BUS", "01012", "01013", 1500],
        ["2026-08", "WEEKDAY", 8, "BUS", "01012", "01013", 2000],
        ["2026-08", "WEEKDAY", 8, "BUS", "01013", "01012", 1500],
        ["2026-08", "WEEKDAY", 7, "BUS", "01012", "10009", 500],
        ["2026-08", "WEEKDAY", 8, "BUS", "01012", "10009", 500],
        ["2026-08", "WEEKDAY", 8, "BUS", "01013", "10009", 1000],
        ["2026-08", "WEEKDAY", 9, "BUS", "10009", "01012", 600],
        ["2026-08", "WEEKDAY", 9, "BUS", "99999", "01012", 20],
        ["2026-08", "WEEKENDS/HOLIDAY", 8, "BUS", "01012", "01013", 1100],
        ["2026-08", "WEEKDAY", 8, "BUS", "01012", "01013", -5],   # negative trips
    ],
}


def write_pv():
    for ym, rows in PV.items():
        with open(OUT / f"transport_node_bus_{ym}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(PV_HEADER)
            w.writerows(rows)
    for ym, rows in OD.items():
        with open(OUT / f"origin_destination_bus_{ym}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(OD_HEADER)
            w.writerows(rows)


def write_static():
    stops = [{"BusStopCode": c, "RoadName": "Fixture Rd", "Description": f"Fixture Stop {c}",
              "Latitude": 1.30 + i * 0.002, "Longitude": 103.80 + i * 0.002}
             for i, c in enumerate(["01012", "01013", "10009", "10011", "20009"])]
    services = [{"ServiceNo": s, "Operator": "FIX", "Direction": 1, "Category": "TRUNK", "OriginCode": "01012",
                 "DestinationCode": "10009", "AM_Peak_Freq": "08-10", "AM_Offpeak_Freq": "10-12",
                 "PM_Peak_Freq": "08-10", "PM_Offpeak_Freq": "10-12", "LoopDesc": ""} for s in ["1", "2"]]
    route = lambda s, q, c, km: {"ServiceNo": s, "Operator": "FIX", "Direction": 1, "StopSequence": q,  # noqa: E731
                                 "BusStopCode": c, "Distance": km, "WD_FirstBus": "0600", "WD_LastBus": "2300",
                                 "SAT_FirstBus": "0600", "SAT_LastBus": "2300", "SUN_FirstBus": "0600",
                                 "SUN_LastBus": "2300"}
    routes = [route("1", 1, "01012", 0.0), route("1", 2, "01013", 0.6), route("1", 3, "10009", 1.9),
              route("2", 1, "01012", 0.0)]
    for name, rows in [("BusStops", stops), ("BusServices", services), ("BusRoutes", routes)]:
        (OUT / f"{name}_20260926.json").write_text(json.dumps(rows, indent=0), encoding="utf-8")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    write_snapshots()
    write_links()
    write_pv()
    write_static()
    print("fixture written to", OUT)
