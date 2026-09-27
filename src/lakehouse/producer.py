"""Replay recorded speed-band snapshots to Redpanda/Kafka in event-time order, optionally faster than real time.

Test switches (both off by default):
  --dup-rate  re-send a fraction of records, as an at-least-once producer would after a timeout;
  --late N    hold back N snapshots and send them after all the others (late, out-of-order arrival).
"""
from __future__ import annotations

import datetime as dt
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from . import messages as m
from .speedbands import read_snapshot


def replay_order(snapshots: list, late: int = 0) -> list:
    """Event-time order, except that `late` snapshots spread evenly through the list are moved to the end."""
    snaps = sorted(snapshots, key=lambda x: x[0])
    if late <= 0 or len(snaps) < 2:
        return snaps
    late = min(late, len(snaps) - 1)
    step = len(snaps) / (late + 1)
    held = {int(round(step * (i + 1))) - 1 for i in range(late)}
    return [s for i, s in enumerate(snaps) if i not in held] + [s for i, s in enumerate(snaps) if i in held]


def plan_messages(ts: dt.datetime, table, partitions: int, dup_rate: float = 0.0, rng: random.Random | None = None):
    """Yield (partition, key, value) for one snapshot: its records, then one EOS marker per partition."""
    ts_iso = ts.isoformat()
    seen = [set() for _ in range(partitions)]
    cols = [table[c].to_pylist() for c in ("link_id", "speed_band", "min_speed", "max_speed")]
    for link, band, mn, mx in zip(*cols):
        p = m.partition_for(link, partitions)
        seen[p].add(link)
        msg = (p, str(link).encode(), m.encode_record(ts_iso, link, band, mn, mx))
        yield msg
        if dup_rate and rng.random() < dup_rate:
            yield msg
    total = sum(len(s) for s in seen)
    for p in range(partitions):
        yield p, m.EOS_KEY, m.encode_eos(ts_iso, len(seen[p]), total)


@dataclass
class ProducerStats:
    snapshots: int = 0
    messages: int = 0
    bytes: int = 0
    duplicates_injected: int = 0
    delivery_errors: int = 0
    send_seconds: float = 0.0
    paced_sleep_seconds: float = 0.0
    order: list = field(default_factory=list)

    def summary(self) -> dict:
        d = asdict(self)
        d["msgs_per_s"] = round(self.messages / self.send_seconds) if self.send_seconds else None
        d["mb_per_s"] = round(self.bytes / 1e6 / self.send_seconds, 2) if self.send_seconds else None
        return d


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic]
    try:
        fut.result()
    except Exception as e:  # created concurrently by another process
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise


def replay(settings, snapshots: list[tuple[dt.datetime, Path]], speedup: float = 0.0, max_sleep: float = 10.0,
           dup_rate: float = 0.0, late: int = 0, seed: int = 7) -> ProducerStats:
    from confluent_kafka import Producer
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.partitions)
    prod = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,  # broker drops producer-retry duplicates; re-runs are handled by the sink
        "acks": "all",
        "linger.ms": 20,
        "batch.size": 1_000_000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 1_000_000,
    })
    rng = random.Random(seed)
    stats = ProducerStats()

    def on_delivery(err, _msg):
        if err is not None:
            stats.delivery_errors += 1
    prev_ts = None
    for ts, path in replay_order(snapshots, late):
        if speedup > 0 and prev_ts is not None and ts > prev_ts:
            pause = min(max_sleep, (ts - prev_ts).total_seconds() / speedup)
            time.sleep(pause)
            stats.paced_sleep_seconds += pause
        prev_ts = ts if prev_ts is None else max(prev_ts, ts)
        table = read_snapshot(path, ts)
        t0 = time.perf_counter()
        sent = 0
        for p, key, value in plan_messages(ts, table, settings.partitions, dup_rate, rng):
            while True:
                try:
                    prod.produce(settings.topic, value=value, key=key, partition=p, on_delivery=on_delivery)
                    break
                except BufferError:
                    prod.poll(0.05)
            sent += 1
            stats.bytes += len(value) + len(key)
        if prod.flush(120) != 0:
            raise RuntimeError("producer could not deliver all messages within 120 s")
        stats.send_seconds += time.perf_counter() - t0
        stats.messages += sent
        stats.snapshots += 1
        stats.duplicates_injected += sent - table.num_rows - settings.partitions
        stats.order.append(ts.isoformat())
    if stats.delivery_errors:
        raise RuntimeError(f"{stats.delivery_errors} messages were not acknowledged by the broker")
    return stats
