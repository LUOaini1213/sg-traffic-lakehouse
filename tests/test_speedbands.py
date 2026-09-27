import datetime as dt
import gzip
import os
import time

import pyarrow as pa
import pytest

from lakehouse.messages import partition_for
from lakehouse.speedbands import (QUARANTINE, TABLE, BronzeSpeedbandWriter, SchemaDriftError, dedupe,
                                  dedupe_counting, discover_snapshots, load_files, parse_snapshot,
                                  parse_snapshot_name, read_snapshot, split_by_partition, write_quarantine)
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


def stored(lake):
    t = lake.delta(TABLE)
    return [] if t is None else sorted(t.to_pyarrow_table().to_pylist(), key=lambda r: (r["snapshot_ts"], r["link_id"]))


def test_parse_snapshot_name():
    assert parse_snapshot_name("sb_20260926_1928.csv.gz") == dt.datetime(2026, 9, 26, 19, 28)
    for bad in ["sb_20260926_1928.csv", "sb_2026092_1928.csv.gz", "speedband_links.csv", "sb_20260926_19281.csv.gz"]:
        with pytest.raises(ValueError):
            parse_snapshot_name(bad)


def test_bronze_partitions_use_event_time(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake)
    late_evening, after_midnight = dt.datetime(2026, 9, 26, 23, 58), dt.datetime(2026, 9, 27, 0, 5)
    w.upsert(late_evening, rows(late_evening, [3], [4]), "stream", 0, 3)
    w.upsert(after_midnight, rows(after_midnight, [5], [4]), "stream", 2, 3)
    got = {(r["obs_date"], r["obs_hour"], r["part"]) for r in stored(lake)}
    assert got == {(dt.date(2026, 9, 26), 23, 0), (dt.date(2026, 9, 27), 0, 2)}


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


def test_bad_rows_are_quarantined_not_fatal(tmp_path):
    """One bad line used to stop the whole file. Now every rule sends its row to the rejects, and the rest load."""
    p = tmp_path / "sb_20260926_2010.csv.gz"
    _write_gz(p, "LinkID,SpeedBand,MinimumSpeed,MaximumSpeed\n"
                 "1,3,20,29\n"          # valid
                 "2,x,20,29\n"          # band not a number
                 "3,0,,\n"              # band 0 with empty bounds: valid, as DataMall publishes it
                 "4,3,20,29,extra\n"    # extra field
                 "5,3,20.5,29\n"        # decimal bound
                 "6,99999,1,2\n"        # band out of SMALLINT range
                 ",3,1,2\n"             # no link id
                 "7,2,10,19\n")         # valid
    ps = parse_snapshot(p)
    assert ps.table["link_id"].to_pylist() == [1, 3, 7]
    assert ps.source_rows == 8 == ps.table.num_rows + ps.rejects.num_rows
    assert ps.reasons == {"malformed_row": 1, "missing_or_uncastable_link_id": 1,
                          "missing_or_uncastable_speed_band": 2, "uncastable_min_speed": 1}
    malformed = [r for r in ps.rejects.to_pylist() if r["_reject"] == "malformed_row"]
    assert malformed[0]["_raw_line"] == "4,3,20,29,extra"


def test_fixture_snapshot_with_bad_lines():
    ps = parse_snapshot(FIXTURE / "speedbands" / "sb_20260905_0900.csv.gz")
    assert (ps.source_rows, ps.table.num_rows, ps.rejects.num_rows) == (14, 12, 2)


def test_quarantine_is_replaced_per_snapshot(settings, tmp_path):
    lake = Lake(settings)
    p = tmp_path / "sb_20260926_2010.csv.gz"
    _write_gz(p, "LinkID,SpeedBand,MinimumSpeed,MaximumSpeed\n1,3,20,29\n2,x,20,29\n")
    write_quarantine(lake, parse_snapshot(p))
    assert lake.delta_stats(QUARANTINE)["rows"] == 1
    _write_gz(p, "LinkID,SpeedBand,MinimumSpeed,MaximumSpeed\n1,3,20,29\n2,3,20,29\n")  # fixed upstream
    write_quarantine(lake, parse_snapshot(p))
    assert lake.delta_stats(QUARANTINE)["rows"] == 0


def test_dedupe_keeps_first_and_sorts():
    t = pa.concat_tables([rows(T0, [3, 1, 2], [4, 4, 4]), rows(T0, [1, 3], [7, 7])])
    d = dedupe(t)
    assert d["link_id"].to_pylist() == [1, 2, 3]
    assert d["speed_band"].to_pylist() == [4, 4, 4]


def test_dedupe_counts_only_repeats_with_a_different_payload():
    t = pa.concat_tables([rows(T0, [1, 2, 3], [4, 4, 0]), rows(T0, [1, 2, 3], [4, 7, 0])])
    d, conflicts = dedupe_counting(t)
    assert d.num_rows == 3 and conflicts == 1  # link 2 differs; link 1 and link 3 (NULL bounds) repeat exactly


def test_dedupe_is_per_snapshot():
    t1 = T0 + dt.timedelta(minutes=20)
    d = dedupe(pa.concat_tables([rows(T0, [1, 2], [3, 3]), rows(t1, [1, 2], [5, 5])]))
    assert d.num_rows == 4


def test_upsert_is_idempotent(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake, clock=lambda: dt.datetime(2026, 9, 27, 4, 0))
    r1 = w.upsert(T0, rows(T0, [1, 2, 3], [2, 3, 4]), "stream", 0, 1)
    v1 = lake.delta_stats(TABLE)["version"]
    r2 = w.upsert(T0, rows(T0, [1, 2, 3], [2, 3, 4]), "file", 0, 1)
    assert (r1.rows_new, r2.rows_new, r2.rows_after, r2.written, r2.collisions_payload_differs) == (3, 0, 3, False, 0)
    assert lake.delta_stats(TABLE)["version"] == v1  # nothing committed
    assert {r["_source"] for r in stored(lake)} == {"stream"}


def test_upsert_merges_partial_redelivery_first_writer_wins(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake)
    w.upsert(T0, rows(T0, [1, 2], [2, 3]), "stream", 0, 1)
    r = w.upsert(T0, rows(T0, [2, 3, 4], [9, 4, 5]), "stream", 0, 1)
    assert (r.rows_existing, r.rows_new, r.rows_after, r.collisions_payload_differs) == (2, 2, 4, 1)
    assert {x["link_id"]: x["speed_band"] for x in stored(lake)} == {1: 2, 2: 3, 3: 4, 4: 5}


def test_upsert_late_snapshot_goes_to_its_own_partition(settings):
    lake = Lake(settings)
    w = BronzeSpeedbandWriter(lake)
    later, earlier = T0 + dt.timedelta(hours=3), T0
    w.upsert(later, rows(later, [1], [5]), "stream", 0, 1, late=False)
    w.upsert(earlier, rows(earlier, [1], [2]), "stream", 0, 1, late=True)  # arrives after a later snapshot
    got = [(r["snapshot_ts"], r["obs_hour"], r["speed_band"], r["_late"]) for r in stored(lake)]
    assert got == [(earlier, 8, 2, True), (later, 11, 5, False)]


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


@pytest.mark.parametrize("first", ["consumer", "backfill"])
def test_concurrent_writers_lose_nothing(settings, first):
    """The reviewer's reproduction: the file backfill (4 rows) and a consumer flush (1 row) write the same slice.
    One writer reads the slice, the other commits, then the first commits on its stale read. With read-merge-write
    of one Parquet object the last writer overwrote the other (bronze kept 1 of 4 rows). With a Delta MERGE the
    stale commit is rejected, retried on the new version, and bronze holds all 4 rows once each."""
    lake = Lake(settings)
    consumer, backfill = BronzeSpeedbandWriter(lake), BronzeSpeedbandWriter(lake)
    stream_rows, file_rows = rows(T0, [1], [2]), rows(T0, [1, 4, 7, 10], [2, 3, 4, 5])
    slow, fast = (consumer, backfill) if first == "consumer" else (backfill, consumer)
    slow_rows, fast_rows = (stream_rows, file_rows) if first == "consumer" else (file_rows, stream_rows)
    fired = []

    def interleave(ts, part):
        if not fired:
            fired.append(1)
            fast.upsert(T0, fast_rows, "file" if fast is backfill else "stream", 1, 3)

    slow.before_commit = interleave
    res = slow.upsert(T0, slow_rows, "stream" if slow is consumer else "file", 1, 3)
    got = stored(lake)
    assert fired and res.retries == 1
    assert sorted(r["link_id"] for r in got) == [1, 4, 7, 10]
    assert res.rows_after == 4


def test_backfill_twice_changes_nothing_and_counts_repeats(settings):
    lake = Lake(settings)
    first = load_files(settings, lake, log=lambda *_: None)
    v = lake.delta_stats(TABLE)["version"]
    again = load_files(settings, lake, log=lambda *_: None)
    assert sum(x["rows_new_in_bronze"] for x in first["snapshots"]) == 120
    assert sum(x["rows_new_in_bronze"] for x in again["snapshots"]) == 0
    assert lake.delta_stats(TABLE)["version"] == v
    by = {x["file"]: x for x in first["snapshots"]}
    assert by["sb_20260905_0920.csv.gz"]["duplicate_rows_in_file"] == 1
    assert by["sb_20260905_0920.csv.gz"]["collisions_payload_differs"] == 1
    assert by["sb_20260905_0900.csv.gz"]["rows_quarantined"] == 2
    assert {r["speed_band"] for r in stored(lake) if r["link_id"] == 104 and r["snapshot_ts"].hour == 9
            and r["snapshot_ts"].minute == 20} == {6}
