import datetime as dt
import gzip
import os
import time

import pyarrow as pa
import pytest

from lakehouse.messages import partition_for
from lakehouse.speedbands import (BronzeSpeedbandWriter, SchemaDriftError, dedupe, discover_snapshots,
                                  parse_snapshot_name, partition_path, read_snapshot, split_by_partition)
from lakehouse.storage import Lake

from conftest import FIXTURE

T0 = dt.datetime(2026, 9, 5, 8, 0)


def rows(ts, links, bands):
    return pa.table({
        "snapshot_ts": pa.array([ts] * len(links), pa.timestamp("s")),
        "link_id": pa.array(links, pa.int64()),
        "speed_band": pa.array(bands, pa.int16()),
        "min_speed": pa.array([None if b == 0 else (b - 1) * 10 for b in bands], pa.int16()),
        "max_speed": pa.array([None if b == 0 else b * 10 - 1 for b in bands], pa.int16()),
    })


def test_parse_snapshot_name():
    assert parse_snapshot_name("sb_20260926_1928.csv.gz") == dt.datetime(2026, 9, 26, 19, 28)
    for bad in ["sb_20260926_1928.csv", "sb_2026092_1928.csv.gz", "speedband_links.csv", "sb_20260926_19281.csv.gz"]:
        with pytest.raises(ValueError):
            parse_snapshot_name(bad)


def test_partition_path_uses_event_time():
    assert partition_path(dt.datetime(2026, 9, 26, 23, 58), 0) == \
        "bronze/speedbands/obs_date=2026-09-26/obs_hour=23/snapshot=20260926T2358-p0.parquet"
    assert partition_path(dt.datetime(2026, 9, 27, 0, 5), 2) == \
        "bronze/speedbands/obs_date=2026-09-27/obs_hour=00/snapshot=20260927T0005-p2.parquet"


def _write_gz(path, text, truncate=False):
    with gzip.open(path, "wt") as f:
        f.write(text)
    if truncate:
        data = path.read_bytes()
        path.write_bytes(data[: len(data) // 2])


def test_discover_skips_young_truncated_and_misnamed(tmp_path):
    body = "LinkID,SpeedBand,MinimumSpeed,MaximumSpeed\n" + "".join(f"{i},3,20,29\n" for i in range(2000))
    _write_gz(tmp_path / "sb_20260926_2010.csv.gz", body)
    _write_gz(tmp_path / "sb_20260926_1951.csv.gz", body)
    _write_gz(tmp_path / "sb_20260926_2028.csv.gz", body, truncate=True)
    _write_gz(tmp_path / "sb_20260926_2045.csv.gz", body)
    _write_gz(tmp_path / "sb_bad.csv.gz", body)
    now = time.time()
    old = now - 3600
    for p in tmp_path.iterdir():
        os.utime(p, (old, old))
    os.utime(tmp_path / "sb_20260926_2045.csv.gz", (now - 5, now - 5))  # still being written
    found, skipped = discover_snapshots(tmp_path, min_age_s=60, now=now)
    assert [p.name for _, p in found] == ["sb_20260926_1951.csv.gz", "sb_20260926_2010.csv.gz"]
    assert any("2028" in s and "truncated" in s for s in skipped)
    assert any("2045" in s and "younger" in s for s in skipped)
    assert any("sb_bad" in s for s in skipped)
    found, _ = discover_snapshots(tmp_path, min_age_s=60, now=now, until=dt.datetime(2026, 9, 26, 20, 0))
    assert [p.name for _, p in found] == ["sb_20260926_1951.csv.gz"]
    found, _ = discover_snapshots(tmp_path, min_age_s=60, now=now, since=dt.datetime(2026, 9, 26, 20, 0))
    assert [p.name for _, p in found] == ["sb_20260926_2010.csv.gz"]


def test_read_snapshot_types_and_band_zero():
    t = read_snapshot(FIXTURE / "speedbands" / "sb_20260905_0800.csv.gz")
    assert t.schema.field("link_id").type == pa.int64()
    assert t.num_rows == 12
    assert set(t["snapshot_ts"].to_pylist()) == {T0}
    zero = [r for r in t.to_pylist() if r["link_id"] == 110][0]
    assert zero["speed_band"] == 0 and zero["min_speed"] is None and zero["max_speed"] is None
    b8 = read_snapshot(FIXTURE / "speedbands" / "sb_20260907_0755.csv.gz").to_pylist()
    assert [r["max_speed"] for r in b8 if r["link_id"] == 107] == [999]


def test_read_snapshot_rejects_schema_drift(tmp_path):
    p = tmp_path / "sb_20260926_2010.csv.gz"
    _write_gz(p, "LinkID,SpeedBand,MinSpeed,MaximumSpeed\n1,3,20,29\n")
    with pytest.raises(SchemaDriftError):
        read_snapshot(p)


def test_dedupe_keeps_first_and_sorts():
    t = pa.concat_tables([rows(T0, [3, 1, 2], [4, 4, 4]), rows(T0, [1, 3], [7, 7])])
    d = dedupe(t)
    assert d["link_id"].to_pylist() == [1, 2, 3]
    assert d["speed_band"].to_pylist() == [4, 4, 4]


def test_dedupe_is_per_snapshot():
    t1 = T0 + dt.timedelta(minutes=20)
    d = dedupe(pa.concat_tables([rows(T0, [1, 2], [3, 3]), rows(t1, [1, 2], [5, 5])]))
    assert d.num_rows == 4


def test_upsert_is_idempotent(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake, clock=lambda: dt.datetime(2026, 9, 27, 12, 0))
    r1 = w.upsert(T0, rows(T0, [1, 2, 3], [2, 3, 4]), "stream", 0, 1)
    h1 = lake.sha256(r1.path)
    r2 = w.upsert(T0, rows(T0, [1, 2, 3], [2, 3, 4]), "file", 0, 1)
    assert (r1.rows_new, r2.rows_new, r2.rows_after, r2.written) == (3, 0, 3, False)
    assert lake.sha256(r1.path) == h1
    stored = lake.read_parquet(r1.path).to_pylist()
    assert {r["_source"] for r in stored} == {"stream"}


def test_upsert_merges_partial_redelivery(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake)
    w.upsert(T0, rows(T0, [1, 2], [2, 3]), "stream", 0, 1)
    r = w.upsert(T0, rows(T0, [2, 3, 4], [9, 4, 5]), "stream", 0, 1)
    assert (r.rows_existing, r.rows_new, r.rows_after) == (2, 2, 4)
    got = {x["link_id"]: x["speed_band"] for x in lake.read_parquet(r.path).to_pylist()}
    assert got == {1: 2, 2: 3, 3: 4, 4: 5}  # the first stored value of link 2 wins


def test_upsert_late_snapshot_goes_to_its_own_partition(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake)
    later, earlier = T0 + dt.timedelta(hours=3), T0
    w.upsert(later, rows(later, [1], [5]), "stream", 0, 1)
    w.upsert(earlier, rows(earlier, [1], [2]), "stream", 0, 1)  # arrives after a later snapshot
    files = lake.list_files("bronze/speedbands")
    assert files == [partition_path(earlier, 0), partition_path(later, 0)]
    assert lake.read_parquet(partition_path(earlier, 0))["speed_band"].to_pylist() == [2]


def test_upsert_rejects_rows_of_another_snapshot(settings):
    w = BronzeSpeedbandWriter(Lake(settings))
    with pytest.raises(ValueError):
        w.upsert(T0, rows(T0 + dt.timedelta(minutes=20), [1], [3]), "stream", 0, 1)


def test_split_by_partition_matches_producer_routing():
    t = rows(T0, [101, 102, 103, 104, 105, 999, 143787], [3] * 7)
    parts = split_by_partition(t, 3)
    for p, sub in parts.items():
        assert all(partition_for(x, 3) == p for x in sub["link_id"].to_pylist())
    assert sum(x.num_rows for x in parts.values()) == 7


def test_upsert_rejects_rows_of_another_partition(settings):
    w = BronzeSpeedbandWriter(Lake(settings))
    with pytest.raises(ValueError):
        w.upsert(T0, rows(T0, [1, 2], [3, 3]), "stream", 1, 3)  # link 2 belongs to partition 2
    assert w.upsert(T0, rows(T0, [1, 4], [3, 3]), "stream", 1, 3).rows_new == 2
