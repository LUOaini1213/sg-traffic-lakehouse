"""Wire format for the speed-band topic.

Each link observation is one JSON message keyed by link_id and sent to partition link_id % P, so all observations of
a link keep their order. After the last record of a snapshot, the producer sends one end-of-snapshot (EOS) marker to
every partition, carrying the number of distinct links it sent to that partition. Because Kafka preserves order
within a partition, the consumer knows a snapshot is complete once it holds an EOS from every partition.
"""
from __future__ import annotations

import datetime as dt
import json

EOS_KEY = b"__eos__"


def _n(v) -> str:
    return "null" if v is None else str(int(v))


def encode_record(ts_iso: str, link_id: int, band, min_speed, max_speed) -> bytes:
    return (f'{{"ts":"{ts_iso}","link":{int(link_id)},"band":{_n(band)},'
            f'"min":{_n(min_speed)},"max":{_n(max_speed)}}}').encode()


def encode_eos(ts_iso: str, expected: int, snapshot_rows: int) -> bytes:
    return json.dumps({"ts": ts_iso, "eos": True, "expected": expected, "snapshot_rows": snapshot_rows}).encode()


def decode(key: bytes | None, value: bytes) -> dict:
    d = json.loads(value)
    d["kind"] = "eos" if key == EOS_KEY else "rec"
    d["ts"] = dt.datetime.fromisoformat(d["ts"])
    return d


def partition_for(link_id: int, partitions: int) -> int:
    return int(link_id) % partitions
