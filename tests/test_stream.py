"""Producer and consumer logic without a broker: messages are routed to simulated partitions and interleaved."""
import datetime as dt
import random

import pyarrow as pa

from lakehouse import messages as m
from lakehouse.consumer import SnapshotAssembler
from lakehouse.producer import plan_messages, replay_order
from lakehouse.speedbands import BronzeSpeedbandWriter, discover_snapshots, partition_path, read_snapshot
from lakehouse.storage import Lake

from conftest import FIXTURE

T0 = dt.datetime(2026, 9, 5, 8, 0)
P = 3


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def table(ts, links, band=3):
    return pa.table({"snapshot_ts": pa.array([ts] * len(links), pa.timestamp("s")),
                     "link_id": pa.array(links, pa.int64()),
                     "speed_band": pa.array([band] * len(links), pa.int16()),
                     "min_speed": pa.array([20] * len(links), pa.int16()),
                     "max_speed": pa.array([29] * len(links), pa.int16())})


def feed(asm, msgs, create_ms=0):
    for p, key, value in msgs:
        d = m.decode(key, value)
        if d["kind"] == "eos":
            asm.add_eos(d["ts"], p, d["expected"])
        else:
            asm.add_record(d["ts"], d["link"], d["band"], d["min"], d["max"], create_ms, p)


def test_codec_roundtrip_with_nulls():
    d = m.decode(b"7", m.encode_record(T0.isoformat(), 7, 0, None, None))
    assert d == {"ts": T0, "link": 7, "band": 0, "min": None, "max": None, "kind": "rec"}
    e = m.decode(m.EOS_KEY, m.encode_eos(T0.isoformat(), 5, 12))
    assert e["kind"] == "eos" and e["expected"] == 5 and e["ts"] == T0


def test_plan_messages_partitions_and_eos():
    msgs = list(plan_messages(T0, table(T0, list(range(1, 11))), P))
    recs = [x for x in msgs if x[1] != m.EOS_KEY]
    eos = [x for x in msgs if x[1] == m.EOS_KEY]
    assert all(p == int(k) % P for p, k, _ in recs)
    assert sorted(p for p, _, _ in eos) == [0, 1, 2]
    assert {p: m.decode(k, v)["expected"] for p, k, v in eos} == {0: 3, 1: 4, 2: 3}
    assert msgs[-3:] == eos  # markers come after every record


def test_plan_messages_duplicates_do_not_change_expected_counts():
    msgs = list(plan_messages(T0, table(T0, list(range(1, 11))), P, dup_rate=1.0, rng=random.Random(1)))
    assert sum(1 for x in msgs if x[1] != m.EOS_KEY) == 20
    eos = [m.decode(k, v)["expected"] for _, k, v in msgs if k == m.EOS_KEY]
    assert sum(eos) == 10


def test_replay_order_holds_back_late_snapshots():
    snaps = [(T0 + dt.timedelta(minutes=20 * i), f"f{i}") for i in range(10)]
    assert replay_order(list(reversed(snaps))) == snaps
    order = replay_order(snaps, late=2)
    assert sorted(order) == snaps
    assert len(order) == 10
    head, tail = order[:8], order[8:]
    assert head == sorted(head) and tail == sorted(tail)
    assert all(t[0] < head[-1][0] for t in tail)  # the held-back snapshots really arrive out of order


def test_assembler_flushes_each_partition_on_its_marker():
    asm = SnapshotAssembler(clock=Clock())
    msgs = list(plan_messages(T0, table(T0, [1, 2, 3, 4, 5]), P))
    feed(asm, [x for x in msgs if x[1] != m.EOS_KEY])
    assert asm.ready() == []
    feed(asm, [x for x in msgs if x[1] == m.EOS_KEY and x[0] == 1])
    assert asm.ready() == [((T0, 1), "eos")]
    t, info = asm.pop((T0, 1), "eos")
    assert t["link_id"].to_pylist() == [1, 4] and info.complete and info.partition == 1
    feed(asm, [x for x in msgs if x[1] == m.EOS_KEY and x[0] != 1])
    assert asm.ready() == [((T0, 0), "eos"), ((T0, 2), "eos")]
    assert [asm.pop(k, r)[0]["link_id"].to_pylist() for k, r in asm.ready()] == [[3], [2, 5]]
    assert asm.pending == {}


def test_assembler_drops_duplicates():
    asm = SnapshotAssembler(clock=Clock())
    feed(asm, plan_messages(T0, table(T0, list(range(1, 21))), P, dup_rate=0.5, rng=random.Random(3)))
    popped = [asm.pop(k, r) for k, r in asm.ready()]
    assert sum(t.num_rows for t, _ in popped) == 20
    assert sum(i.duplicates for _, i in popped) > 0 and all(i.complete for _, i in popped)


def test_assembler_flags_missing_records_as_incomplete():
    asm = SnapshotAssembler(clock=Clock())
    msgs = [x for x in plan_messages(T0, table(T0, [1, 2, 3, 4, 5, 6]), P) if x[1] != b"4"]  # lose one record
    feed(asm, msgs)
    infos = {k[1]: asm.pop(k, r)[1] for k, r in asm.ready()}
    assert not infos[1].complete and infos[1].rows == 1
    assert infos[0].complete and infos[2].complete


def test_assembler_late_and_out_of_order():
    asm = SnapshotAssembler(allowed_lateness=dt.timedelta(minutes=30), clock=Clock())

    def one(ts):
        feed(asm, plan_messages(ts, table(ts, [3]), P))  # link 3 -> partition 0
        return asm.pop((ts, 0), "eos")[1]

    t2 = T0 + dt.timedelta(hours=2)
    info = one(t2)
    assert not info.out_of_order and not info.late
    info = one(t2 - dt.timedelta(minutes=20))  # out of order but within the allowed lateness
    assert info.out_of_order and not info.late
    info = one(T0)  # two hours behind the newest event
    assert info.out_of_order and info.late
    assert asm.watermark() == t2 - dt.timedelta(minutes=30)


def test_watermark_waits_for_every_owned_partition():
    """A single consumer may read many snapshots from one partition before the first record of another arrives;
    that must not make the other partition's snapshots look late."""
    asm = SnapshotAssembler(allowed_lateness=dt.timedelta(minutes=30), clock=Clock())
    asm.owned = {0, 1}
    t2 = T0 + dt.timedelta(hours=2)
    for ts in (T0, T0 + dt.timedelta(hours=1), t2):
        feed(asm, [x for x in plan_messages(ts, table(ts, [3]), P) if x[0] == 0])  # partition 0 only
    assert asm.watermark() is None
    feed(asm, plan_messages(T0, table(T0, [4]), P))  # partition 1 starts at the oldest snapshot
    assert not asm.pending[(T0, 1)].late
    assert asm.watermark() == T0 - dt.timedelta(minutes=30)


def test_assembler_times_out_without_markers():
    clock = Clock()
    asm = SnapshotAssembler(flush_timeout_s=30, clock=clock)
    feed(asm, [x for x in plan_messages(T0, table(T0, [3, 6]), P) if x[1] != m.EOS_KEY])
    clock.t += 29
    assert asm.ready() == []
    clock.t += 2
    assert asm.ready() == [((T0, 0), "timeout")]
    _, info = asm.pop((T0, 0), "timeout")
    assert not info.complete and info.reason == "timeout"


def test_stream_to_bronze_matches_files_with_duplicates_and_late_data(settings):
    """Every fixture snapshot, 30% duplicated records, 2 late snapshots, partitions interleaved at random and
    delivered twice: bronze must equal the source files exactly, one object per snapshot and partition."""
    snaps, _ = discover_snapshots(FIXTURE / "speedbands", min_age_s=0)
    lake = Lake(settings)
    writer = BronzeSpeedbandWriter(lake)
    rng = random.Random(42)
    for _delivery in range(2):
        asm = SnapshotAssembler(clock=Clock())
        queues = {p: [] for p in range(P)}
        for ts, path in replay_order(snaps, late=2):
            for msg in plan_messages(ts, read_snapshot(path, ts), P, dup_rate=0.3, rng=rng):
                queues[msg[0]].append(msg)
        while any(queues.values()):  # interleave partitions, keeping order within each partition
            p = rng.choice([k for k, v in queues.items() if v])
            feed(asm, [queues[p].pop(0)])
            for key, reason in asm.ready():
                t, info = asm.pop(key, reason)
                writer.upsert(info.ts, t, "stream", info.partition, P)
    files = lake.list_files("bronze/speedbands")
    assert files == sorted(partition_path(ts, p) for ts, _ in snaps for p in range(P))
    for ts, path in snaps:
        got = pa.concat_tables(lake.read_parquet(partition_path(ts, p)) for p in range(P)).sort_by("link_id")
        cols = ["link_id", "speed_band", "min_speed", "max_speed"]
        want = read_snapshot(path, ts).sort_by("link_id").select(cols)
        assert got.select(cols).equals(want), ts


def test_file_backfill_and_stream_write_the_same_objects(settings):
    """Backfill from files after the stream: nothing new, no object rewritten."""
    from lakehouse.speedbands import load_files
    lake = Lake(settings)
    first = load_files(settings, lake, log=lambda *_: None)
    hashes = {f: lake.sha256(f) for f in lake.list_files("bronze/speedbands")}
    again = load_files(settings, lake, log=lambda *_: None)
    assert sum(x["rows_new_in_bronze"] for x in first["snapshots"]) == 120
    assert sum(x["rows_new_in_bronze"] for x in again["snapshots"]) == 0
    assert {f: lake.sha256(f) for f in lake.list_files("bronze/speedbands")} == hashes
    assert len(hashes) == 30


def test_live_pages_to_table_matches_file_schema():
    from lakehouse.live import pages_to_table
    t = pages_to_table([{"LinkID": "103000000", "SpeedBand": 3, "MinimumSpeed": "20", "MaximumSpeed": "29"},
                        {"LinkID": "103000001", "SpeedBand": 0, "MinimumSpeed": "", "MaximumSpeed": None}], T0)
    ref = read_snapshot(FIXTURE / "speedbands" / "sb_20260905_0800.csv.gz")
    assert t.schema == ref.schema
    assert t.to_pylist()[1] == {"snapshot_ts": T0, "link_id": 103000001, "speed_band": 0, "min_speed": None,
                                "max_speed": None}
