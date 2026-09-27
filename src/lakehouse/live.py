"""Optional live mode: fetch Traffic Speed Bands from LTA DataMall and publish them to the same topic as the replay.

The DataMall AccountKey is read from the LTA_KEY environment variable at run time and is never written anywhere.
Not exercised in this repository's measurements (they use recorded snapshots); `pages_to_table` is unit-tested.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

import pyarrow as pa

URL = "https://datamall2.mytransport.sg/ltaodataservice/v4/TrafficSpeedBands"
SGT = ZoneInfo("Asia/Singapore")


def fetch_pages(key: str, page_size: int = 500, timeout: float = 60.0) -> list[dict]:
    rows, skip = [], 0
    while True:
        req = urllib.request.Request(f"{URL}?{urllib.parse.urlencode({'$skip': skip})}",
                                     headers={"AccountKey": key, "accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            page = json.loads(r.read()).get("value", [])
        rows += page
        if len(page) < page_size:
            return rows
        skip += page_size
        time.sleep(0.2)


def _int(v):
    return None if v in (None, "") else int(v)


def pages_to_table(rows: list[dict], ts: dt.datetime) -> pa.Table:
    return pa.table({
        "snapshot_ts": pa.array([ts] * len(rows), pa.timestamp("us")),
        "link_id": pa.array([int(r["LinkID"]) for r in rows], pa.int64()),
        "speed_band": pa.array([_int(r.get("SpeedBand")) for r in rows], pa.int16()),
        "min_speed": pa.array([_int(r.get("MinimumSpeed")) for r in rows], pa.int16()),
        "max_speed": pa.array([_int(r.get("MaximumSpeed")) for r in rows], pa.int16()),
    })


def fetch_start(now: dt.datetime | None = None) -> dt.datetime:
    """Event time of a live snapshot: the fetch start in Singapore time, naive and truncated to the minute, like the
    recorded file names. Explicit about the zone, so the machine's own time zone does not matter."""
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.astimezone(SGT).replace(tzinfo=None, second=0, microsecond=0)


def run(settings, every_s: float, count: int) -> None:
    from confluent_kafka import Producer

    from .producer import ensure_topic, plan_messages
    key = os.environ.get("LTA_KEY")
    if not key:
        raise SystemExit("set LTA_KEY in the environment to use live mode")
    ensure_topic(settings.kafka_bootstrap, settings.topic, settings.partitions)
    prod = Producer({"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True, "acks": "all",
                     "linger.ms": 20, "compression.type": "lz4"})
    for i in range(count):
        ts = fetch_start()
        table = pages_to_table(fetch_pages(key), ts)
        for p, k, v in plan_messages(ts, table, settings.partitions):
            while True:
                try:
                    prod.produce(settings.topic, value=v, key=k, partition=p)
                    break
                except BufferError:
                    prod.poll(0.05)
        prod.flush(120)
        print(f"{ts:%Y-%m-%d %H:%M} published {table.num_rows} links")
        if i + 1 < count:
            time.sleep(every_s)
