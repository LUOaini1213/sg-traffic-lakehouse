"""Streaming measurements against the running Redpanda + MinIO stack. Writes results/stream_benchmark.json.

Four runs, each on its own new topic and consumer group. For each run, CONSUMERS consumer processes join one group
(one per topic partition) and the producer starts only after every partition is assigned, so the numbers exclude
group start-up.
  1. throughput - all snapshots as fast as possible, with 1% duplicate records and 2 snapshots held back until the
                  end (late, out of order), into the main lake. Producer and consumer rates.
  2. latency    - the same snapshots paced in event time (PACED_SPEEDUP), into a separate empty bucket. Latency is
                  from the producer's timestamp on a partition's end-of-snapshot marker to the end of the Delta
                  commit that writes that slice to bronze.
  3. rerun      - the same snapshots again into the main lake, on a new topic and a new group. This tests that the
                  sink is idempotent (the Delta table must not get a single new version); it is not a crash test.
  4. crash      - into a separate empty bucket. Every consumer exits abruptly (os._exit, no commit, no clean leave)
                  right after its CRASH_AFTER_WRITES-th bronze write. Three new consumers then join the same group
                  and resume from the committed offsets, while the file backfill writes the same snapshots into
                  the same table at the same time. Bronze must end with every row exactly once.
"""
from __future__ import annotations

import dataclasses
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

from .producer import ensure_topic, replay
from .speedbands import TABLE, discover_snapshots
from .storage import Lake, duckdb_connect

PACED_SPEEDUP = 120.0   # 18 minutes of event time -> 9 s
PACED_MAX_SLEEP = 10.0  # the overnight gap would otherwise last minutes
CONSUMERS = 3
CRASH_AFTER_WRITES = 5


def _wait_for_assignment(readies, procs, partitions, timeout=180):
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


def _start_consumers(s, env, res_dir, tag, group, crash_after=0):
    procs, readies, outs, lats = [], [], [], []
    for i in range(CONSUMERS):
        ready, lat = res_dir / f".ready-{tag}-{i}", res_dir / f".latency-{tag}-{i}.npy"
        ready.unlink(missing_ok=True)
        out = f".stream_{tag}_consumer{i}.json"
        (res_dir / out).unlink(missing_ok=True)
        cmd = [sys.executable, "-m", "lakehouse", "consume", "--group", group, "--idle-exit", "20",
               "--ready-file", str(ready), "--latency-file", str(lat), "--out", out]
        if crash_after:
            cmd += ["--crash-after-writes", str(crash_after)]
        procs.append(subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL))
        readies.append(ready), outs.append(res_dir / out), lats.append(lat)
    return procs, readies, outs, lats


def _collect(outs, lats, readies):
    cons = [json.loads(o.read_text(encoding="utf-8")) for o in outs if o.exists()]
    lat = np.concatenate([np.load(f) for f in lats if f.exists()] or [np.array([])])
    for f in [*readies, *lats, *outs]:
        f.unlink(missing_ok=True)
    return cons, lat


def _consumer_summary(cons, lat) -> dict:
    flushes = [f for c in cons for f in c["flush_log"]]
    t0 = min((c["t_first_message"] for c in cons if c["t_first_message"]), default=None)
    t1 = max((c["t_last_write"] for c in cons if c["t_last_write"]), default=None)
    records = sum(c["records_consumed"] for c in cons)
    pct = lambda q: round(float(np.percentile(lat, q)), 1) if len(lat) else None  # noqa: E731
    return {
        "records_consumed": records,
        "per_consumer_records": [c["records_consumed"] for c in cons],
        "flushes": len(flushes),
        "flushes_incomplete": sum(not f["complete"] for f in flushes),
        "snapshots_late": sorted({f["snapshot_ts"] for f in flushes if f["late"]}),
        "duplicates_dropped": sum(c["duplicates_dropped_in_consumer"] for c in cons),
        "collisions_payload_differs": sum(c["collisions_payload_differs"] for c in cons),
        "rows_new_in_bronze": sum(c["rows_new_in_bronze"] for c in cons),
        "bronze_commits": sum(f["committed_to_bronze"] for f in flushes),
        "merge_retries": sum(c["merge_retries"] for c in cons),
        "dead_lettered": sum(c["dead_lettered"] for c in cons),
        "slices_redelivered_already_stored": sum(f["rows"] > 0 and f["rows_new_in_bronze"] == 0 for f in flushes),
        "active_seconds": round(t1 - t0, 2) if t0 and t1 else None,
        "records_per_s": round(records / (t1 - t0)) if t0 and t1 and t1 > t0 else None,
        "eos_to_write_ms": {"p50": pct(50), "p95": pct(95), "p99": pct(99), "max": pct(100), "n": int(len(lat))},
    }, flushes


def _run(settings, snaps, name, lake_root, speedup, dup_rate, late, max_sleep=PACED_MAX_SLEEP) -> dict:
    stamp = time.strftime("%Y%m%d%H%M%S")
    s = dataclasses.replace(settings, topic=f"sg.speedbands.{name}-{stamp}", lake_root=lake_root)
    ensure_topic(s.kafka_bootstrap, s.topic, s.partitions)
    env = dict(os.environ, KAFKA_TOPIC=s.topic, LAKE_ROOT=lake_root)
    res_dir = Path(settings.results_dir)
    procs, readies, outs, lats = _start_consumers(s, env, res_dir, name, f"bench-{name}-{stamp}")
    assignment = _wait_for_assignment(readies, procs, s.partitions)
    stats = replay(s, snaps, speedup=speedup, max_sleep=max_sleep, dup_rate=dup_rate, late=late)
    for p in procs:
        if p.wait(timeout=3600) != 0:
            raise RuntimeError(f"a consumer for {name} exited with {p.returncode}")
    cons, lat = _collect(outs, lats, readies)
    summary, flushes = _consumer_summary(cons, lat)
    return {"topic": s.topic, "group": f"bench-{name}-{stamp}", "lake_root": lake_root, "consumers": CONSUMERS,
            "assignment": assignment, "producer": stats.summary(), "consumer": summary, "flushes": flushes}


def _bronze_check(settings, lake_root) -> dict:
    s = dataclasses.replace(settings, lake_root=lake_root)
    lake = Lake(s)
    con = duckdb_connect(s)
    n, keys = con.execute(f"SELECT count(*), count(DISTINCT (snapshot_ts, link_id)) FROM {lake.scan(TABLE)}").fetchone()
    con.close()
    return {"rows": n, "distinct_keys": keys, "duplicate_rows": n - keys, **lake.delta_stats(TABLE)}


def _crash_run(settings, snaps, until) -> dict:
    """Consumers crash after a few writes; replacements resume while the file backfill runs concurrently."""
    stamp = time.strftime("%Y%m%d%H%M%S")
    root = "s3://sgtl-stream-crash" if settings.is_s3 else settings.lake_root + "-crash"
    Lake(dataclasses.replace(settings, lake_root=root)).delete_prefix("bronze")
    s = dataclasses.replace(settings, topic=f"sg.speedbands.crash-{stamp}", lake_root=root)
    ensure_topic(s.kafka_bootstrap, s.topic, s.partitions)
    env = dict(os.environ, KAFKA_TOPIC=s.topic, LAKE_ROOT=root)
    res_dir = Path(settings.results_dir)
    group = f"bench-crash-{stamp}"
    procs, readies, outs, lats = _start_consumers(s, env, res_dir, "crash1", group, crash_after=CRASH_AFTER_WRITES)
    _wait_for_assignment(readies, procs, s.partitions)
    stats = replay(s, snaps)
    codes = [p.wait(timeout=600) for p in procs]
    for f in [*readies, *lats, *outs]:  # a crashed process writes no summary
        f.unlink(missing_ok=True)
    first = _bronze_check(settings, root)
    # restart the group and run the file backfill against the same table at the same time
    procs, readies, outs, lats = _start_consumers(s, env, res_dir, "crash2", group)
    with tempfile.TemporaryDirectory() as tmp:
        bf_env = dict(env, RESULTS_DIR=tmp)
        backfill = subprocess.Popen([sys.executable, "-m", "lakehouse", "ingest-speedbands",
                                     *(["--until", until.strftime("%Y%m%d%H%M")] if until else [])],
                                    env=bf_env, stdout=subprocess.DEVNULL)
        if backfill.wait(timeout=3600) != 0:
            raise RuntimeError("the concurrent file backfill failed")
        files = json.loads((Path(tmp) / "speedband_files.json").read_text(encoding="utf-8"))["snapshots"]
    for p in procs:
        if p.wait(timeout=3600) != 0:
            raise RuntimeError(f"a restarted consumer exited with {p.returncode}")
    cons, lat = _collect(outs, lats, readies)
    summary, _ = _consumer_summary(cons, lat)
    return {"topic": s.topic, "group": group, "lake_root": root, "crash_after_writes": CRASH_AFTER_WRITES,
            "producer": stats.summary(), "first_exit_codes": codes, "bronze_after_crash": first,
            "restarted_consumers": summary,
            "backfill": {"rows_new_in_bronze": sum(f["rows_new_in_bronze"] for f in files),
                         "merge_retries": sum(f["merge_retries"] for f in files)},
            "bronze_final": _bronze_check(settings, root)}


def run(settings, until=None, fresh: bool = True) -> dict:
    snaps, skipped = discover_snapshots(Path(settings.source_dir) / "speedbands", until=until,
                                        min_age_s=settings.snapshot_min_age_s)
    lake = Lake(settings)
    lake.ensure_bucket()
    if fresh:
        lake.delete_prefix("bronze/speedbands")
        lake.delete_prefix("bronze/_dead_letter")
    res = {"snapshots": len(snaps), "first": snaps[0][0].isoformat(), "last": snaps[-1][0].isoformat(),
           "skipped": skipped, "fresh_bronze": fresh}
    res["throughput"] = _run(settings, snaps, "throughput", settings.lake_root, 0.0, 0.01, 2)
    paced_root = "s3://sgtl-stream-paced" if settings.is_s3 else settings.lake_root + "-paced"
    Lake(dataclasses.replace(settings, lake_root=paced_root)).delete_prefix("bronze")
    res["latency"] = _run(settings, snaps, "latency", paced_root, PACED_SPEEDUP, 0.0, 0)
    res["latency"].update(speedup=PACED_SPEEDUP, max_sleep_s=PACED_MAX_SLEEP)
    before = lake.delta_stats(TABLE)
    res["rerun"] = _run(settings, snaps, "rerun", settings.lake_root, 0.0, 0.0, 0)
    after = lake.delta_stats(TABLE)
    res["rerun"].update(bronze_before=before, bronze_after=after, bronze_unchanged=before == after)
    res["crash"] = _crash_run(settings, snaps, until)
    res["bronze_main"] = _bronze_check(settings, settings.lake_root)
    out = Path(settings.results_dir) / "stream_benchmark.json"
    out.write_text(json.dumps(res, indent=2), encoding="utf-8")
    return res
