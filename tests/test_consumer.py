"""The consumer loop against an in-memory broker (tests/fakekafka.py): crashes, rebalances and bad messages."""
import dataclasses
import datetime as dt
import json
import random
import time

import pyarrow as pa
import pytest

import fakekafka
from lakehouse import messages as m
from lakehouse.consumer import DLQ_TABLE, BronzeConsumer
from lakehouse.producer import plan_messages, replay_order
from lakehouse.speedbands import TABLE, dedupe, discover_snapshots, read_snapshot
from lakehouse.storage import Lake

from conftest import FIXTURE

P = 3
T0 = dt.datetime(2026, 9, 5, 8, 0)
quiet = lambda *_: None  # noqa: E731


class Crash(Exception):
    pass


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def table(ts, links, band=3):
    return pa.table({"snapshot_ts": pa.array([ts] * len(links), pa.timestamp("s")),
                     "link_id": pa.array(links, pa.int64()),
                     "speed_band": pa.array([band] * len(links), pa.int16()),
                     "min_speed": pa.array([(band - 1) * 10] * len(links), pa.int16()),
                     "max_speed": pa.array([band * 10 - 1] * len(links), pa.int16())})


@pytest.fixture
def s(settings):
    return dataclasses.replace(settings, topic="t")


def fixture_broker(dup_rate=0.3, late=2, seed=5):
    snaps, _ = discover_snapshots(FIXTURE / "speedbands", min_age_s=0)
    b = fakekafka.Broker("t", P)
    rng = random.Random(seed)
    for ts, path in replay_order(snaps, late):
        b.produce_all(plan_messages(ts, read_snapshot(path, ts), P, dup_rate, rng))
    return b, snaps


def drain(bc):
    """Step until the topic is exhausted and nothing is buffered."""
    bc.subscribe()
    for _ in range(100_000):
        if bc.step(0) == 0 and not bc.asm.pending:
            return bc.summary()
    raise AssertionError("consumer did not drain")


def bronze(lake):
    return lake.delta(TABLE).to_pyarrow_table().to_pylist()


def assert_bronze_equals_files(lake, snaps):
    got = bronze(lake)
    keys = [(r["snapshot_ts"], r["link_id"]) for r in got]
    assert len(keys) == len(set(keys)), "duplicate keys in bronze"
    want = {}
    for ts, path in snaps:
        for r in dedupe(read_snapshot(path, ts)).to_pylist():
            want[(ts, r["link_id"])] = (r["speed_band"], r["min_speed"], r["max_speed"])
    assert {(r["snapshot_ts"], r["link_id"]): (r["speed_band"], r["min_speed"], r["max_speed"]) for r in got} == want


@pytest.mark.parametrize("crash_at", [1, 2, 4, 9, 17, 29])
def test_crash_between_write_and_commit_loses_and_duplicates_nothing(s, crash_at):
    """The consumer dies right after its n-th bronze write, before it can commit offsets (as with kill -9). A new
    consumer in the same group starts from the committed offsets: the records written but not committed come
    again and the MERGE inserts nothing for them; records that were only buffered come again and are written."""
    broker, snaps = fixture_broker()
    lake = Lake(s)

    def boom(n):
        if n == crash_at:
            raise Crash

    first = BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g"), log=quiet, on_write=boom)
    with pytest.raises(Crash):
        drain(first)
    written_before = sum(f["rows_new_in_bronze"] for f in first.flushes)
    second = BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g"), log=quiet)
    res = drain(second)
    assert_bronze_equals_files(lake, snaps)
    assert written_before + res["rows_new_in_bronze"] == 120
    # the slice written just before the crash was delivered again and found already stored
    last = first.flushes[-1]
    again = [f for f in second.flushes if (f["snapshot_ts"], f["partition"]) == (last["snapshot_ts"], last["partition"])]
    assert again and again[0]["rows"] > 0 and again[0]["rows_new_in_bronze"] == 0
    assert broker.committed["g"] == {p: len(broker.logs[p]) for p in range(P)}


def test_no_crash_run_writes_everything_once_and_persists_late_flags(s):
    broker, snaps = fixture_broker(dup_rate=0.0)  # no injected copies, so the collision count below is exact
    lake = Lake(s)
    res = drain(BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g"), log=quiet))
    assert_bronze_equals_files(lake, snaps)
    assert res["rows_new_in_bronze"] == 120 and res["bronze_commits"] == 30 and res["flushes_incomplete"] == 0
    late = {r["snapshot_ts"] for r in bronze(lake) if r["_late"]}
    assert late == {T0 + dt.timedelta(minutes=40), T0 + dt.timedelta(hours=3, minutes=20)}
    assert {r["_late"] for r in bronze(lake)} == {True, False}
    # the fixture repeats link 104 with a different band inside one snapshot: counted, first copy kept
    assert res["collisions_payload_differs"] == 1 and res["duplicates_dropped_in_consumer"] == 1


def test_watermark_uses_the_assigned_partitions(s):
    """A consumer owns partitions 0 and 1 and reads all of partition 0 (three hourly snapshots) before the first
    record of partition 1. Partition 1's oldest snapshot is not late: the watermark waits for every assigned
    partition. (If the assignment were ignored, the watermark would follow partition 0 alone and flag it.)"""
    broker = fakekafka.Broker("t", P)
    for h in range(3):
        ts = T0 + dt.timedelta(hours=h)
        broker.produce_all(plan_messages(ts, table(ts, [3, 4]), P))  # link 3 -> partition 0, link 4 -> partition 1
    lake = Lake(s)
    bc = BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g", partitions=[0, 1], sequential=True), log=quiet)
    drain(bc)
    flags = {(f["snapshot_ts"], f["partition"]): f["late"] for f in bc.flushes}
    assert flags[(T0.isoformat(), 1)] is False
    assert bc.asm.owned == {0, 1}
    assert {r["_late"] for r in bronze(lake) if r["link_id"] == 4} == {False}


def test_revoked_partition_buffers_are_dropped_not_flushed(s):
    """Partition 2 is revoked while its snapshot is half-read. Its buffer must be dropped (the next owner re-reads
    from the last commit); flushing it on the timeout would write an incomplete slice."""
    broker = fakekafka.Broker("t", P)
    msgs = list(plan_messages(T0, table(T0, [3, 4, 5, 6, 7, 8]), P))
    broker.produce_all([x for x in msgs if not (x[0] == 2 and x[1] == m.EOS_KEY)])  # partition 2: no marker yet
    clock = Clock()
    client = fakekafka.Consumer(broker, "g", chunk=100)
    bc = BronzeConsumer(s, Lake(s), client, log=quiet, clock=clock, flush_timeout_s=30)
    bc.subscribe()
    bc.step(0)
    assert set(bc.asm.pending) == {(T0, 2)}
    client.revoke([2])
    assert bc.asm.pending == {} and bc.dropped_on_revoke == 2
    clock.t += 60
    bc.step(0)
    assert [f["partition"] for f in bc.flushes] == [0, 1]
    assert {r["part"] for r in bronze(Lake(s))} == {0, 1}
    assert 2 not in broker.committed["g"]  # never committed past the dropped records


def test_bad_messages_go_to_the_dead_letter_table(s):
    """Malformed JSON, a key on the wrong partition, a wrong type, an out-of-range value and a bad marker used to
    crash the consumer, and crash it again on every restart. Now they are dead-lettered once each."""
    broker = fakekafka.Broker("t", P)
    ts = T0.isoformat()
    broker.produce(0, b"3", b"{not json")
    broker.produce(0, b"4", m.encode_record(ts, 4, 3, 20, 29))                    # link 4 belongs to partition 1
    broker.produce(1, b"7", json.dumps({"ts": ts, "link": 7, "band": "x", "min": 1, "max": 2}).encode())
    broker.produce(2, m.EOS_KEY, json.dumps({"ts": ts, "eos": True, "expected": "a"}).encode())
    broker.produce(2, b"8", m.encode_record(ts, 8, 99999, 20, 29))                # band outside SMALLINT
    broker.produce(1, b"10", json.dumps({"ts": "yesterday", "link": 10, "band": 1}).encode())
    broker.produce_all(plan_messages(T0, table(T0, [3, 4, 5]), P))
    lake = Lake(s)
    res = drain(BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g"), log=quiet))
    dlq = lake.delta(DLQ_TABLE).to_pyarrow_table().to_pylist()
    assert sorted(r["reason"] for r in dlq) == ["bad_band", "bad_band", "bad_eos", "bad_ts", "malformed_json",
                                                "wrong_partition"]
    assert res["dead_lettered"] == 6 and res["rows_new_in_bronze"] == 3
    assert broker.committed["g"] == {p: len(broker.logs[p]) for p in range(P)}
    # a second group reads everything again: no new dead letters, no new rows
    res2 = drain(BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g2"), log=quiet))
    assert res2["dead_lettered"] == 0 and len(lake.delta(DLQ_TABLE).to_pyarrow_table()) == 6
    assert len(bronze(lake)) == 3


def test_latency_is_measured_from_the_end_of_snapshot_marker(s):
    """Records produced long ago, marker produced 0.5 s ago: the reported latency is marker -> bronze write."""
    broker = fakekafka.Broker("t", P)
    now_ms = time.time() * 1000
    for p, k, v in plan_messages(T0, table(T0, [3, 4, 5]), P):
        broker.produce(p, k, v, ts_ms=int(now_ms - 500) if k == m.EOS_KEY else int(now_ms - 3_600_000))
    bc = BronzeConsumer(s, Lake(s), fakekafka.Consumer(broker, "g"), log=quiet)
    res = drain(bc)
    assert res["eos_to_write_ms"]["n"] == 3
    assert 500 <= res["eos_to_write_ms"]["p50"] < 60_000


def test_a_different_payload_for_a_stored_key_is_counted(s):
    broker = fakekafka.Broker("t", P)
    lake = Lake(s)
    from lakehouse.speedbands import BronzeSpeedbandWriter
    BronzeSpeedbandWriter(lake).upsert(T0, table(T0, [3], band=2), "file", 0, P)
    broker.produce_all(plan_messages(T0, table(T0, [3, 6], band=5), P))
    res = drain(BronzeConsumer(s, lake, fakekafka.Consumer(broker, "g"), log=quiet))
    assert res["collisions_payload_differs"] == 1 and res["rows_new_in_bronze"] == 1
    assert {r["link_id"]: r["speed_band"] for r in bronze(lake)} == {3: 2, 6: 5}


def test_nothing_to_commit_after_a_rebalance_is_not_fatal(s):
    """Seen on Redpanda: two consumers went idle and left, their partitions moved to the third, which had nothing
    new to commit for them; librdkafka then raises 'No offset stored' (_NO_OFFSET). That must not stop the consumer.
    Any other commit error still must."""
    from confluent_kafka import KafkaError, KafkaException

    class NoOffsetOnce(fakekafka.Consumer):
        error = KafkaError._NO_OFFSET

        def commit(self, asynchronous=False):
            if self.error is not None:
                err, self.error = self.error, None
                raise KafkaException(KafkaError(err))
            super().commit(asynchronous)

    broker = fakekafka.Broker("t", P)
    broker.produce_all(plan_messages(T0, table(T0, [3, 4, 5]), P))
    res = drain(BronzeConsumer(s, Lake(s), NoOffsetOnce(broker, "g"), log=quiet))
    assert res["rows_new_in_bronze"] == 3

    class OtherError(NoOffsetOnce):
        error = KafkaError._TRANSPORT

    broker.produce_all(plan_messages(T0 + dt.timedelta(hours=1), table(T0, [3]), P))
    with pytest.raises(KafkaException):
        drain(BronzeConsumer(s, Lake(s), OtherError(broker, "g2"), log=quiet))
