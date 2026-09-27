"""Kafka -> bronze consumer for speed-band records.

Records are grouped by (snapshot event time, Kafka partition) in a SnapshotAssembler. A group is flushed to its own
bronze object when the partition's end-of-snapshot (EOS) marker arrives (Kafka keeps order within a partition, so
every record sent before the marker has arrived), or, if the marker never comes, after `flush_timeout_s` without
new records (the flush is then marked incomplete). Because every object belongs to one partition, several consumer
processes in one group can run in parallel without two of them writing the same object.

Offsets are committed only when nothing is buffered, so a crash replays records that may already be in bronze; the
merge-on-write upsert makes that harmless (at-least-once delivery plus an idempotent sink).

Late data: each partition's newest event time is tracked; the watermark is the smallest of these minus
`allowed_lateness` (event time only advances in order within a partition). A snapshot that first appears behind the
watermark is flagged late but still written to its own event-time partition; the next dbt
build picks it up. Nothing is dropped.
"""
from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa

from . import messages as m
from .speedbands import BronzeSpeedbandWriter


@dataclass
class Pending:
    ts: dt.datetime
    partition: int
    late: bool
    out_of_order: bool
    rows: dict = field(default_factory=dict)          # link_id -> (band, min, max)
    create_ms: list = field(default_factory=list)     # producer timestamp of each distinct record
    expected: int | None = None                       # from the EOS marker
    duplicates: int = 0
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
    reason: str
    create_ms: np.ndarray


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

    def add_record(self, ts, link_id, band, min_speed, max_speed, create_ms: int, partition: int) -> None:
        p = self._get(ts, partition)
        if link_id in p.rows:
            p.duplicates += 1
            return
        p.rows[link_id] = (band, min_speed, max_speed)
        p.create_ms.append(create_ms)

    def add_eos(self, ts, partition: int, expected: int) -> None:
        self._get(ts, partition).expected = expected

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
            "snapshot_ts": pa.array([p.ts] * len(links), pa.timestamp("s")),
            "link_id": pa.array(links, pa.int64()),
            "speed_band": pa.array(list(band), pa.int16()),
            "min_speed": pa.array(list(mn), pa.int16()),
            "max_speed": pa.array(list(mx), pa.int16()),
        })
        complete = p.expected is not None and len(links) >= p.expected
        info = FlushInfo(p.ts, p.partition, len(links), complete, p.late, p.out_of_order, p.duplicates, reason,
                         np.asarray(p.create_ms, dtype=np.float64))
        return table, info


def run(settings, lake, group_id: str = "sgtl-bronze-writer", idle_exit_s: float = 20.0,
        flush_timeout_s: float = 30.0, allowed_lateness_min: float = 30.0, max_seconds: float = 3600.0,
        log=print, ready_file: str | None = None, latency_file: str | None = None) -> dict:
    """Consume until the topic has been idle for idle_exit_s with nothing buffered. Returns run metrics."""
    from confluent_kafka import Consumer, KafkaError

    from .producer import ensure_topic
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.partitions)
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "fetch.min.bytes": 1_000_000,
        "fetch.wait.max.ms": 100,
    })

    state = {"assigned_at": None}

    def on_assign(_c, partitions):
        asm.owned = {p.partition for p in partitions}
        state["assigned_at"] = time.monotonic()
        if ready_file:  # lets a benchmark start producing only once the partitions are owned
            with open(ready_file, "w") as f:
                f.write(",".join(str(p.partition) for p in partitions))

    asm = SnapshotAssembler(dt.timedelta(minutes=allowed_lateness_min), flush_timeout_s)
    consumer.subscribe([settings.topic], on_assign=on_assign)
    writer = BronzeSpeedbandWriter(lake)
    lake.ensure_bucket()

    latencies, flushes, ts_cache = [], [], {}
    consumed = eos_seen = 0
    t_start = time.time()
    t_first = t_last_write = None
    last_msg = time.monotonic()
    uncommitted = False
    try:
        while time.time() - t_start < max_seconds:
            batch = consumer.consume(num_messages=20000, timeout=0.5)
            for msg in batch:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(msg.error())
                d = json.loads(msg.value())
                ts = ts_cache.get(d["ts"])
                if ts is None:
                    ts = ts_cache[d["ts"]] = dt.datetime.fromisoformat(d["ts"])
                if t_first is None:
                    t_first = time.time()
                uncommitted = True
                if msg.key() == m.EOS_KEY:
                    eos_seen += 1
                    asm.add_eos(ts, msg.partition(), d["expected"])
                else:
                    consumed += 1
                    asm.add_record(ts, d["link"], d["band"], d["min"], d["max"], msg.timestamp()[1],
                                   msg.partition())
            if batch:
                last_msg = time.monotonic()
            for key, reason in asm.ready():
                table, info = asm.pop(key, reason)
                res = writer.upsert(info.ts, table, "stream", info.partition, settings.partitions)
                t_last_write = time.time()
                lat = t_last_write * 1000.0 - info.create_ms
                latencies.append(lat)
                flushes.append({
                    "snapshot_ts": info.ts.isoformat(), "partition": info.partition, "rows": info.rows,
                    "complete": info.complete, "late": info.late, "out_of_order": info.out_of_order,
                    "duplicates_dropped": info.duplicates, "flush": reason, "rows_new_in_bronze": res.rows_new,
                    "rows_in_bronze": res.rows_after, "object_written": res.written,
                    "latency_ms_p50": round(float(np.percentile(lat, 50)), 1) if len(lat) else None,
                    "latency_ms_max": round(float(lat.max()), 1) if len(lat) else None,
                })
                log(json.dumps(flushes[-1]))
            if uncommitted and not asm.pending:
                consumer.commit(asynchronous=False)
                uncommitted = False
            idle_since = max(last_msg, state["assigned_at"] or last_msg)
            if state["assigned_at"] and not asm.pending and time.monotonic() - idle_since > idle_exit_s:
                break
    finally:
        consumer.close()

    lat = np.concatenate(latencies) if latencies else np.array([])
    if latency_file:
        np.save(latency_file, lat)
    active = (t_last_write - t_first) if (t_first and t_last_write) else None
    pct = lambda q: round(float(np.percentile(lat, q)), 1) if len(lat) else None  # noqa: E731
    return {
        "records_consumed": consumed,
        "eos_markers": eos_seen,
        "objects_flushed": len(flushes),
        "snapshots_seen": len({f["snapshot_ts"] for f in flushes}),
        "snapshots_late": len({f["snapshot_ts"] for f in flushes if f["late"]}),
        "snapshots_out_of_order": len({f["snapshot_ts"] for f in flushes if f["out_of_order"]}),
        "flushes_incomplete": sum(not f["complete"] for f in flushes),
        "duplicates_dropped_in_consumer": sum(f["duplicates_dropped"] for f in flushes),
        "rows_new_in_bronze": sum(f["rows_new_in_bronze"] for f in flushes),
        "t_first_message": t_first,
        "t_last_write": t_last_write,
        "active_seconds": round(active, 2) if active else None,
        "records_per_s": round(consumed / active) if active else None,
        "latency_ms": {"p50": pct(50), "p95": pct(95), "p99": pct(99), "max": pct(100), "n": int(len(lat))},
        "flushes": flushes,
    }
