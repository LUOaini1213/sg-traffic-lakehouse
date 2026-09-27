"""Streaming measurements against the running Redpanda + MinIO stack. Writes results/stream_benchmark.json.

Three runs, each on its own new topic. For each run, CONSUMERS consumer processes join one group (one per topic
partition) and the producer starts only after every partition is assigned, so the numbers exclude group start-up.
  1. throughput - all snapshots as fast as possible, with 1% duplicate records and 2 snapshots held back until the
                  end (late, out of order), into the main lake. Producer and consumer rates.
  2. latency    - the same snapshots paced in event time (PACED_SPEEDUP), into a separate empty bucket.
                  Produce -> durable-in-bronze latency per record.
  3. rerun      - the same snapshots again into the main lake. Bronze objects must be unchanged (same SHA-256).
"""
from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

from .producer import ensure_topic, replay
from .speedbands import discover_snapshots
from .storage import Lake

PACED_SPEEDUP = 120.0   # 18 minutes of event time -> 9 s
PACED_MAX_SLEEP = 10.0  # the overnight gap would otherwise last minutes
CONSUMERS = 3


def bronze_manifest(lake: Lake) -> dict:
    return {f: lake.sha256(f) for f in lake.list_files("bronze/speedbands")}


def _wait_for_assignment(readies, procs, partitions, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if any(p.poll() is not None for p in procs):
            raise RuntimeError("a consumer exited before it was assigned partitions")
        if all(r.exists() for r in readies):
            parts = [[int(x) for x in r.read_text().split(",") if x] for r in readies]
            flat = [p for ps in parts for p in ps]
            if sorted(flat) == list(range(partitions)) and all(parts):
                time.sleep(1.0)
                if [[int(x) for x in r.read_text().split(",") if x] for r in readies] == parts:
                    return parts
        time.sleep(0.2)
    raise RuntimeError("consumers did not settle on a partition assignment")


def _run(settings, snaps, name, lake_root, speedup, dup_rate, late, max_sleep=PACED_MAX_SLEEP) -> dict:
    stamp = time.strftime("%Y%m%d%H%M%S")
    s = dataclasses.replace(settings, topic=f"sg.speedbands.{name}-{stamp}", lake_root=lake_root)
    ensure_topic(s.kafka_bootstrap, s.topic, s.partitions)
    env = dict(os.environ, KAFKA_TOPIC=s.topic, LAKE_ROOT=lake_root)
    res_dir = Path(settings.results_dir)
    procs, readies, outs, lats = [], [], [], []
    for i in range(CONSUMERS):
        ready, lat = res_dir / f".ready-{name}-{i}", res_dir / f".latency-{name}-{i}.npy"
        ready.unlink(missing_ok=True)
        out = f"stream_{name}_consumer{i}.json"
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "lakehouse", "consume", "--group", f"bench-{name}-{stamp}", "--idle-exit", "20",
             "--ready-file", str(ready), "--latency-file", str(lat), "--out", out],
            env=env, stdout=subprocess.DEVNULL))
        readies.append(ready), outs.append(res_dir / out), lats.append(lat)
    assignment = _wait_for_assignment(readies, procs, s.partitions)
    stats = replay(s, snaps, speedup=speedup, max_sleep=max_sleep, dup_rate=dup_rate, late=late)
    for p in procs:
        if p.wait(timeout=3600) != 0:
            raise RuntimeError(f"a consumer for {name} exited with {p.returncode}")
    cons = [json.loads(o.read_text(encoding="utf-8")) for o in outs]
    lat = np.concatenate([np.load(f) for f in lats])
    for f in [*readies, *lats, *outs]:
        f.unlink(missing_ok=True)
    flushes = [f for c in cons for f in c["flushes"]]
    t0 = min(c["t_first_message"] for c in cons if c["t_first_message"])
    t1 = max(c["t_last_write"] for c in cons if c["t_last_write"])
    records = sum(c["records_consumed"] for c in cons)
    pct = lambda q: round(float(np.percentile(lat, q)), 1)  # noqa: E731
    return {
        "topic": s.topic, "lake_root": lake_root, "consumers": CONSUMERS, "assignment": assignment,
        "producer": stats.summary(),
        "consumer": {
            "records_consumed": records,
            "per_consumer_records": [c["records_consumed"] for c in cons],
            "objects_flushed": len(flushes),
            "flushes_incomplete": sum(not f["complete"] for f in flushes),
            "snapshots_late": sorted({f["snapshot_ts"] for f in flushes if f["late"]}),
            "duplicates_dropped": sum(c["duplicates_dropped_in_consumer"] for c in cons),
            "rows_new_in_bronze": sum(c["rows_new_in_bronze"] for c in cons),
            "objects_written": sum(f["object_written"] for f in flushes),
            "active_seconds": round(t1 - t0, 2),
            "records_per_s": round(records / (t1 - t0)),
            "latency_ms": {"p50": pct(50), "p95": pct(95), "p99": pct(99), "max": pct(100), "n": int(len(lat))},
        },
        "flushes": flushes,
    }


def run(settings, until=None, fresh: bool = True) -> dict:
    snaps, skipped = discover_snapshots(Path(settings.source_dir) / "speedbands", until=until,
                                        min_age_s=settings.snapshot_min_age_s)
    lake = Lake(settings)
    lake.ensure_bucket()
    if fresh:
        lake.delete_prefix("bronze/speedbands")
    res = {"snapshots": len(snaps), "first": snaps[0][0].isoformat(), "last": snaps[-1][0].isoformat(),
           "skipped": skipped, "fresh_bronze": fresh}
    res["throughput"] = _run(settings, snaps, "throughput", settings.lake_root, 0.0, 0.01, 2)
    paced_root = "s3://sgtl-stream-paced" if settings.is_s3 else settings.lake_root + "-paced"
    Lake(dataclasses.replace(settings, lake_root=paced_root)).delete_prefix("bronze")
    res["latency"] = _run(settings, snaps, "latency", paced_root, PACED_SPEEDUP, 0.0, 0)
    res["latency"].update(speedup=PACED_SPEEDUP, max_sleep_s=PACED_MAX_SLEEP)
    before = bronze_manifest(lake)
    res["rerun"] = _run(settings, snaps, "rerun", settings.lake_root, 0.0, 0.0, 0)
    after = bronze_manifest(lake)
    res["rerun"].update(bronze_objects=len(after), bronze_unchanged=before == after)
    res["bronze_rows_main"] = sum(lake.num_rows(f) for f in after)
    out = Path(settings.results_dir) / "stream_benchmark.json"
    out.write_text(json.dumps(res, indent=2), encoding="utf-8")
    return res
