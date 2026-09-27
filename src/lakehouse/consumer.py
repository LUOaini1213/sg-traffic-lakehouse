"""Kafka -> bronze consumer for speed-band records.

Records are grouped by (snapshot event time, Kafka partition) in a SnapshotAssembler. A group is flushed to bronze
(one Delta MERGE, see speedbands.py) when the partition's end-of-snapshot (EOS) marker arrives (Kafka keeps order
within a partition, so every record sent before the marker has arrived), or, if the marker never comes, after
`flush_timeout_s` without new records (the flush is then marked incomplete). Every slice belongs to one partition,
and a partition has one owner in the group, so consumers in one group never write the same slice.

Delivery. Offsets are committed only when nothing is buffered: no pending snapshot slice and no unwritten
dead letter. A consumer that dies after a write but before the commit gets its records again on restart (or its
partitions go to another member); the MERGE finds the rows already stored and inserts nothing. This is at-least-once
delivery into an idempotent sink; tests/test_consumer.py kills the consumer at every write and checks the result.

Rebalancing. on_assign records the owned partitions (the watermark waits for all of them). on_revoke drops the
buffers of the revoked partitions: their offsets were not committed past those records, so the next owner reads
them again. Flushing a revoked partition's partial buffer would write an incomplete slice.

Bad messages. A message that is not valid JSON, lacks a field, has a value of the wrong type or range, or is keyed
to a link that does not belong to the partition it arrived on is written to the dead-letter table
bronze/_dead_letter/speedband_stream (MERGE on topic, partition, offset, so redelivery does not repeat it) instead of
stopping the consumer. Before this, one bad message crashed the consumer again on every restart.

Late data: each partition's newest event time is tracked; the watermark is the smallest of these over the owned
partitions minus `allowed_lateness`. A snapshot that first appears behind the watermark is flagged late (the flag is
stored in bronze as _late) but still written to its own event-time partition. Nothing is dropped.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
from deltalake.exceptions import CommitFailedError

from . import messages as m
from .speedbands import BronzeSpeedbandWriter, utcnow

DLQ_TABLE = "bronze/_dead_letter/speedband_stream"
DLQ_SCHEMA = pa.schema([
    ("topic", pa.string()), ("kafka_partition", pa.int32()), ("kafka_offset", pa.int64()),
    ("key", pa.string()), ("value", pa.string()), ("reason", pa.string()), ("dead_lettered_at", pa.timestamp("us")),
])
INT16 = (-32768, 32767)


@dataclass
class Pending:
    ts: dt.datetime
    partition: int
    late: bool
    out_of_order: bool
    rows: dict = field(default_factory=dict)          # link_id -> (band, min, max)
    expected: int | None = None                       # from the EOS marker
    eos_create_ms: float | None = None                # producer timestamp of the EOS marker
    duplicates: int = 0
    conflicts: int = 0                                # duplicates whose payload differs from the first copy
    last_update: float = 0.0


@dataclass
class FlushInfo:
    ts: dt.datetime
    partition: int
    rows: int
    complete: bool
    late: bool
    out_of_order: bool
    duplicates: int
    conflicts: int
    reason: str
    eos_create_ms: float | None


class SnapshotAssembler:
    def __init__(self, allowed_lateness: dt.timedelta = dt.timedelta(minutes=30), flush_timeout_s: float = 30.0,
                 clock=time.monotonic):
        self.allowed_lateness = allowed_lateness
        self.flush_timeout_s = flush_timeout_s
        self.clock = clock
        self.pending: dict[tuple[dt.datetime, int], Pending] = {}
        self.max_event_ts: dict[int, dt.datetime] = {}   # newest event time seen, per partition
        self.owned: set[int] = set()                      # partitions assigned to this consumer (empty: all seen)

    def watermark(self) -> dt.datetime | None:
        """Event time only advances in order within a partition, so the watermark follows the slowest partition this
        consumer owns, and is undefined until every owned partition has delivered something."""
        parts = self.owned if self.owned else set(self.max_event_ts)
        if not parts or not parts <= set(self.max_event_ts):
            return None
        return min(self.max_event_ts[p] for p in parts) - self.allowed_lateness

    def _get(self, ts: dt.datetime, partition: int) -> Pending:
        p = self.pending.get((ts, partition))
        if p is None:
            wm = self.watermark()
            newest = self.max_event_ts.get(partition)
            p = Pending(ts=ts, partition=partition, late=wm is not None and ts < wm,
                        out_of_order=newest is not None and ts < newest)
            self.pending[(ts, partition)] = p
            if newest is None or ts > newest:
                self.max_event_ts[partition] = ts
        p.last_update = self.clock()
        return p

    def add_record(self, ts, link_id, band, min_speed, max_speed, partition: int) -> None:
        p = self._get(ts, partition)
        value = (band, min_speed, max_speed)
        if link_id in p.rows:
            p.duplicates += 1
            p.conflicts += p.rows[link_id] != value
            return
        p.rows[link_id] = value

    def add_eos(self, ts, partition: int, expected: int, create_ms: float | None = None) -> None:
        p = self._get(ts, partition)
        p.expected = expected
        p.eos_create_ms = create_ms

    def drop_partitions(self, partitions: set[int]) -> int:
        """Forget everything buffered for these partitions (they were revoked). Returns the records dropped."""
        keys = [k for k in self.pending if k[1] in partitions]
        n = sum(len(self.pending[k].rows) for k in keys)
        for k in keys:
            del self.pending[k]
        for p in partitions:
            self.max_event_ts.pop(p, None)
        return n

    def ready(self) -> list[tuple[tuple[dt.datetime, int], str]]:
        now = self.clock()
        out = []
        for key, p in self.pending.items():
            if p.expected is not None:
                out.append((key, "eos"))
            elif now - p.last_update >= self.flush_timeout_s:
                out.append((key, "timeout"))
        return sorted(out)

    def pop(self, key: tuple[dt.datetime, int], reason: str) -> tuple[pa.Table, FlushInfo]:
        p = self.pending.pop(key)
        links = sorted(p.rows)
        band, mn, mx = (zip(*(p.rows[k] for k in links)) if links else ((), (), ()))
        table = pa.table({
            "snapshot_ts": pa.array([p.ts] * len(links), pa.timestamp("us")),
            "link_id": pa.array(links, pa.int64()),
            "speed_band": pa.array(list(band), pa.int16()),
            "min_speed": pa.array(list(mn), pa.int16()),
            "max_speed": pa.array(list(mx), pa.int16()),
        })
        complete = p.expected is not None and len(links) >= p.expected
        info = FlushInfo(p.ts, p.partition, len(links), complete, p.late, p.out_of_order, p.duplicates, p.conflicts,
                         reason, p.eos_create_ms)
        return table, info


# -- message validation -----------------------------------------------------------------------------------------------
class BadMessage(ValueError):
    pass


def _opt_int16(d: dict, name: str):
    v = d.get(name, "missing")
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, int) or not INT16[0] <= v <= INT16[1]:
        raise BadMessage(f"bad_{name}")
    return v


def decode_message(key: bytes | None, value: bytes | None, partition: int, partitions: int, ts_cache: dict) -> tuple:
    """('eos', ts, expected) or ('rec', ts, link, band, min, max). Raises BadMessage with a reason code."""
    try:
        d = json.loads(value)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise BadMessage("malformed_json") from None
    if not isinstance(d, dict):
        raise BadMessage("not_a_json_object")
    raw_ts = d.get("ts")
    ts = ts_cache.get(raw_ts) if isinstance(raw_ts, str) else None
    if ts is None:
        try:
            ts = ts_cache[raw_ts] = dt.datetime.fromisoformat(raw_ts)
        except (TypeError, ValueError):
            raise BadMessage("bad_ts") from None
        if ts.tzinfo is not None:
            raise BadMessage("bad_ts")
    if key == m.EOS_KEY:
        exp = d.get("expected")
        if isinstance(exp, bool) or not isinstance(exp, int) or exp < 0:
            raise BadMessage("bad_eos")
        return ("eos", ts, exp)
    link = d.get("link")
    if isinstance(link, bool) or not isinstance(link, int) or link <= 0:
        raise BadMessage("bad_link")
    band = _opt_int16(d, "band")
    if band is None:
        raise BadMessage("bad_band")
    mn, mx = _opt_int16(d, "min"), _opt_int16(d, "max")
    if key != str(link).encode():
        raise BadMessage("key_does_not_match_link")
    if m.partition_for(link, partitions) != partition:
        raise BadMessage("wrong_partition")
    return ("rec", ts, link, band, mn, mx)


class DeadLetterSink:
    def __init__(self, lake, topic: str):
        self.lake = lake
        self.topic = topic
        self.buffer: list[dict] = []
        self.written = 0

    def add(self, msg, reason: str) -> None:
        dec = lambda b: None if b is None else b.decode("utf-8", errors="replace")  # noqa: E731
        self.buffer.append({"topic": self.topic, "kafka_partition": msg.partition(), "kafka_offset": msg.offset(),
                            "key": dec(msg.key()), "value": dec(msg.value()), "reason": reason,
                            "dead_lettered_at": utcnow()})

    def flush(self) -> int:
        """Write the buffered dead letters (idempotent on topic, partition, offset). Returns rows inserted."""
        if not self.buffer:
            return 0
        rows = pa.Table.from_pylist(self.buffer, schema=DLQ_SCHEMA)
        table = self.lake.ensure_delta(DLQ_TABLE, DLQ_SCHEMA)
        for attempt in range(20):
            try:
                table.update_incremental()
                res = (table.merge(rows, predicate="t.topic = s.topic and t.kafka_partition = s.kafka_partition "
                                                   "and t.kafka_offset = s.kafka_offset",
                                   source_alias="s", target_alias="t").when_not_matched_insert_all().execute())
                break
            except CommitFailedError:
                time.sleep(0.05 * (attempt + 1))
        else:
            raise RuntimeError("could not write dead letters")
        self.buffer = []
        n = int(res["num_target_rows_inserted"])
        self.written += n
        return n


# -- the consumer loop ------------------------------------------------------------------------------------------------
def _no_offset(e: Exception) -> bool:
    """librdkafka's 'Local: No offset stored' (nothing consumed since the last commit on this assignment)."""
    try:
        from confluent_kafka import KafkaError, KafkaException
    except ImportError:
        return False
    return isinstance(e, KafkaException) and e.args and e.args[0].code() == KafkaError._NO_OFFSET



class BronzeConsumer:
    """One member of the consumer group. `client` is a confluent_kafka.Consumer or a test double with the same
    methods (subscribe, consume, commit, close)."""

    def __init__(self, settings, lake, client, *, flush_timeout_s: float = 30.0, allowed_lateness_min: float = 30.0,
                 log=print, clock=time.monotonic, ready_file: str | None = None, on_write=None):
        self.settings = settings
        self.client = client
        self.log = log
        self.asm = SnapshotAssembler(dt.timedelta(minutes=allowed_lateness_min), flush_timeout_s, clock)
        self.writer = BronzeSpeedbandWriter(lake)
        self.dlq = DeadLetterSink(lake, settings.topic)
        self.ready_file = ready_file
        self.on_write = on_write or (lambda n: None)   # runs after each bronze write, before any commit
        self.assigned_at = None
        self.uncommitted = False
        self.ts_cache: dict = {}
        self.flushes: list[dict] = []
        self.latencies: list[float] = []
        self.consumed = self.eos_seen = self.writes = self.commits = 0
        self.dropped_on_revoke = self.revocations = 0
        self.t_first = self.t_last_write = None

    # rebalance callbacks
    def on_assign(self, _client, partitions):
        self.asm.owned = {p.partition for p in partitions}
        self.assigned_at = time.monotonic()
        if self.ready_file:  # lets a benchmark start producing only once the partitions are owned
            with open(self.ready_file, "w") as f:
                f.write(",".join(str(p.partition) for p in partitions))

    def on_revoke(self, _client, partitions):
        gone = {p.partition for p in partitions}
        self.dropped_on_revoke += self.asm.drop_partitions(gone)
        self.asm.owned -= gone
        self.revocations += 1

    def subscribe(self):
        self.client.subscribe([self.settings.topic], on_assign=self.on_assign, on_revoke=self.on_revoke)

    def handle(self, msg) -> None:
        if msg.error():
            from confluent_kafka import KafkaError
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            raise RuntimeError(msg.error())
        if self.t_first is None:
            self.t_first = time.time()
        self.uncommitted = True
        try:
            d = decode_message(msg.key(), msg.value(), msg.partition(), self.settings.partitions, self.ts_cache)
        except BadMessage as e:
            self.dlq.add(msg, str(e))
            return
        if d[0] == "eos":
            self.eos_seen += 1
            self.asm.add_eos(d[1], msg.partition(), d[2], msg.timestamp()[1])
        else:
            self.consumed += 1
            self.asm.add_record(d[1], d[2], d[3], d[4], d[5], msg.partition())

    def flush_ready(self) -> None:
        for key, reason in self.asm.ready():
            table, info = self.asm.pop(key, reason)
            res = self.writer.upsert(info.ts, table, "stream", info.partition, self.settings.partitions,
                                     late=info.late)
            self.t_last_write = time.time()
            lat = (self.t_last_write * 1000.0 - info.eos_create_ms) if info.eos_create_ms else None
            if lat is not None:
                self.latencies.append(lat)
            self.flushes.append({
                "snapshot_ts": info.ts.isoformat(), "partition": info.partition, "rows": info.rows,
                "complete": info.complete, "late": info.late, "out_of_order": info.out_of_order,
                "duplicates_dropped": info.duplicates, "flush": reason, "rows_new_in_bronze": res.rows_new,
                "rows_in_bronze": res.rows_after, "committed_to_bronze": res.written, "merge_retries": res.retries,
                "collisions_payload_differs": info.conflicts + res.collisions_payload_differs,
                "eos_to_write_ms": None if lat is None else round(lat, 1),
            })
            self.log(json.dumps(self.flushes[-1]))
            self.writes += 1
            self.on_write(self.writes)

    def maybe_commit(self) -> None:
        if self.uncommitted and not self.asm.pending and not self.dlq.buffer:
            try:
                self.client.commit(asynchronous=False)
                self.commits += 1
            except Exception as e:  # after a rebalance there may be nothing to commit for the new assignment
                if not _no_offset(e):
                    raise
            self.uncommitted = False

    def step(self, timeout: float = 0.5) -> int:
        batch = self.client.consume(num_messages=20000, timeout=timeout)
        for msg in batch:
            self.handle(msg)
        self.dlq.flush()
        self.flush_ready()
        self.maybe_commit()
        return len(batch)

    def run(self, idle_exit_s: float = 20.0, max_seconds: float = 3600.0) -> dict:
        """Consume until the topic has been idle for idle_exit_s with nothing buffered."""
        self.subscribe()
        t_start = time.time()
        last_msg = time.monotonic()
        try:
            while time.time() - t_start < max_seconds:
                if self.step(0.5):
                    last_msg = time.monotonic()
                idle_since = max(last_msg, self.assigned_at or last_msg)
                if self.assigned_at and not self.asm.pending and time.monotonic() - idle_since > idle_exit_s:
                    break
        finally:
            self.client.close()
        return self.summary()

    def summary(self) -> dict:
        lat = np.asarray(self.latencies, dtype=np.float64)
        active = (self.t_last_write - self.t_first) if (self.t_first and self.t_last_write) else None
        pct = lambda q: round(float(np.percentile(lat, q)), 1) if len(lat) else None  # noqa: E731
        fl = self.flushes
        return {
            "records_consumed": self.consumed,
            "eos_markers": self.eos_seen,
            "dead_lettered": self.dlq.written,
            "flushes": len(fl),
            "bronze_commits": sum(f["committed_to_bronze"] for f in fl),
            "offset_commits": self.commits,
            "snapshots_seen": len({f["snapshot_ts"] for f in fl}),
            "snapshots_late": len({f["snapshot_ts"] for f in fl if f["late"]}),
            "snapshots_out_of_order": len({f["snapshot_ts"] for f in fl if f["out_of_order"]}),
            "flushes_incomplete": sum(not f["complete"] for f in fl),
            "duplicates_dropped_in_consumer": sum(f["duplicates_dropped"] for f in fl),
            "collisions_payload_differs": sum(f["collisions_payload_differs"] for f in fl),
            "rows_new_in_bronze": sum(f["rows_new_in_bronze"] for f in fl),
            "merge_retries": sum(f["merge_retries"] for f in fl),
            "revocations": self.revocations,
            "records_dropped_on_revoke": self.dropped_on_revoke,
            "t_first_message": self.t_first,
            "t_last_write": self.t_last_write,
            "active_seconds": round(active, 2) if active else None,
            "records_per_s": round(self.consumed / active) if active else None,
            "eos_to_write_ms": {"p50": pct(50), "p95": pct(95), "max": pct(100), "n": int(len(lat))},
            "flush_log": fl,
        }


def run(settings, lake, group_id: str = "sgtl-bronze-writer", idle_exit_s: float = 20.0,
        flush_timeout_s: float = 30.0, allowed_lateness_min: float = 30.0, max_seconds: float = 3600.0,
        log=print, ready_file: str | None = None, latency_file: str | None = None,
        crash_after_writes: int = 0) -> dict:
    """The consumer against a real broker. crash_after_writes > 0 makes the process exit abruptly (os._exit, no
    commit, no clean leave) right after that many bronze writes: used to measure redelivery after a crash."""
    from confluent_kafka import Consumer

    from .producer import ensure_topic
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.partitions)
    client = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "fetch.min.bytes": 1_000_000,
        "fetch.wait.max.ms": 100,
        "session.timeout.ms": 10000,   # a crashed member leaves the group after 10 s instead of 45 s
    })
    lake.ensure_bucket()

    def crash(n):
        if crash_after_writes and n >= crash_after_writes:
            log(json.dumps({"crash": f"exiting after {n} bronze writes, before committing offsets"}))
            os._exit(70)

    bc = BronzeConsumer(settings, lake, client, flush_timeout_s=flush_timeout_s,
                        allowed_lateness_min=allowed_lateness_min, log=log, ready_file=ready_file, on_write=crash)
    res = bc.run(idle_exit_s, max_seconds)
    if latency_file:
        np.save(latency_file, np.asarray(bc.latencies, dtype=np.float64))
    return res
