"""Measurements of the Spark Structured Streaming sink (spark_stream.py) against Redpanda. Writes
results/spark_stream.json.

Every run uses a new topic, a new checkpoint directory and a new bronze table in the same local lake, and is checked
against a reference: the bronze table that the file backfill (`ingest-speedbands`) wrote from the same snapshot
files. The check counts keys lost (in the reference, not in the Spark table), keys added (the other way round), keys
stored more than once, and keys whose payload (band, min, max) differs.

  1. crash   - unpaced replay. The stream exits abruptly (os._exit) right after its CRASH_AFTER_BATCHES-th Delta
               commit, before Spark records the batch as done. A new stream process restarts from the same
               checkpoint.
  2. sigkill - unpaced replay. The whole stream process group (Python driver and JVM) gets SIGKILL from outside once
               KILL_AFTER_BATCHES batches are logged, at whatever point the next batch has reached. Restart as above.
  3. latency - replay paced in event time (PACED_SPEEDUP, as in stream_bench.py), no faults. Latency per EOS marker.
"""
from __future__ import annotations

import dataclasses
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np

from .producer import ensure_topic, replay
from .speedbands import TABLE as REF_TABLE
from .speedbands import discover_snapshots
from .storage import duckdb_connect

PACED_SPEEDUP = 120.0
PACED_MAX_SLEEP = 10.0
CRASH_AFTER_BATCHES = 3
KILL_AFTER_BATCHES = 4


def _stream_cmd(table, checkpoint, log, *extra):
    return [sys.executable, "-m", "lakehouse", "spark-stream", "--table", table, "--checkpoint", checkpoint,
            "--log", log, "--idle-exit", "25", *extra]


def _log_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def _wait_ready(ready: Path, proc, timeout=300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if ready.exists():
            return
        if proc.poll() is not None:
            raise RuntimeError(f"stream exited with {proc.returncode} before it started")
        time.sleep(0.5)
    raise RuntimeError("stream did not start")


def check(settings, table: str) -> dict:
    """The Spark bronze table against the file-backfill bronze table of the same lake."""
    con = duckdb_connect(settings)
    spk = f"delta_scan('{settings.lake_root}/{table}')"
    ref = f"delta_scan('{settings.lake_root}/{REF_TABLE}')"
    try:
        rows, keys = con.execute(f"select count(*), count(distinct (snapshot_ts, link_id)) from {spk}").fetchone()
        span = con.execute(f"select min(snapshot_ts), max(snapshot_ts) from {spk}").fetchone()
        ref_where = f"where snapshot_ts between '{span[0]}' and '{span[1]}'" if span[0] else "where false"
        ref_rows = con.execute(f"select count(*) from {ref} {ref_where}").fetchone()[0]
        lost = con.execute(f"select count(*) from (select snapshot_ts, link_id from {ref} {ref_where} "
                           f"except select snapshot_ts, link_id from {spk})").fetchone()[0]
        added = con.execute(f"select count(*) from (select snapshot_ts, link_id from {spk} "
                            f"except select snapshot_ts, link_id from {ref})").fetchone()[0]
        differs = con.execute(
            f"select count(*) from {spk} s join {ref} r using (snapshot_ts, link_id) "
            "where s.speed_band is distinct from r.speed_band or s.min_speed is distinct from r.min_speed "
            "or s.max_speed is distinct from r.max_speed").fetchone()[0]
    finally:
        con.close()
    return {"rows": rows, "distinct_keys": keys, "duplicate_rows": rows - keys, "reference_rows": ref_rows,
            "keys_lost": lost, "keys_added": added, "payload_differs": differs}


def _summarise_log(lines: list[dict]) -> dict:
    written = [x for x in lines if "bronze" in x]
    lat = np.array([e["latency_ms"] for x in lines for e in x.get("eos", [])], dtype=float)
    recs = sum(x["counts"].get("rec", 0) for x in lines)
    t0 = min((x["t_start"] for x in lines), default=None)
    t1 = max((x["t_commit"] for x in lines), default=None)
    pct = lambda q: round(float(np.percentile(lat, q)) / 1000, 2) if len(lat) else None  # noqa: E731
    return {"batches": len(lines), "batches_with_bronze_commit": len(written),
            "records": recs, "rows_inserted": sum(x["bronze"]["inserted"] for x in written),
            "batches_inserting_nothing": sum(x["bronze"]["inserted"] == 0 for x in written),
            "dead_lettered": sum(v for x in lines for k, v in x["counts"].items() if k.endswith("_bad")),
            "active_seconds": round(t1 - t0, 1) if t0 else None,
            "records_per_s": round(recs / (t1 - t0)) if t0 and t1 > t0 else None,
            "eos_to_commit_s": {"p50": pct(50), "p95": pct(95), "max": pct(100), "n": int(len(lat))},
            "batch_seconds_median": round(float(np.median([x["seconds"] for x in written])), 2) if written else None}


def _prepare(settings, name: str):
    stamp = time.strftime("%Y%m%d%H%M%S")
    s = dataclasses.replace(settings, topic=f"sg.speedbands.spark-{name}-{stamp}")
    ensure_topic(s.kafka_bootstrap, s.topic, s.partitions)
    work = Path(settings.lake_root) / "_spark_stream" / name
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    table = f"bronze/speedbands_spark_{name}"
    shutil.rmtree(Path(settings.lake_root) / table, ignore_errors=True)
    shutil.rmtree(Path(settings.lake_root) / "bronze" / "_dead_letter" / "speedband_spark", ignore_errors=True)
    env = dict(os.environ, KAFKA_TOPIC=s.topic, LAKE_ROOT=settings.lake_root)
    return s, work, table, env


def _fault_run(settings, snaps, name: str, mode: str) -> dict:
    s, work, table, env = _prepare(settings, name)
    ckpt, log, ready = str(work / "checkpoint"), work / "batches.jsonl", work / "ready"
    extra = ["--crash-after-batches", str(CRASH_AFTER_BATCHES)] if mode == "exit" else []
    p1 = subprocess.Popen(_stream_cmd(table, ckpt, str(log), "--ready-file", str(ready), *extra), env=env,
                          start_new_session=True, stdout=subprocess.DEVNULL, stderr=open(work / "stream1.err", "w"))
    _wait_ready(ready, p1)
    prod = {}
    th = threading.Thread(target=lambda: prod.update(stats=replay(s, snaps).summary()))
    th.start()
    killed_at = None
    if mode == "sigkill":
        while len(_log_lines(log)) < KILL_AFTER_BATCHES and p1.poll() is None:
            time.sleep(0.2)
        time.sleep(1.5)  # land somewhere inside the next batch
        killed_at = len(_log_lines(log))
        os.killpg(p1.pid, signal.SIGKILL)
    code1 = p1.wait(timeout=1800)
    th.join()
    first_lines = _log_lines(log)
    after_crash = check(settings, table)
    ready.unlink(missing_ok=True)
    t_restart = time.time()
    p2 = subprocess.Popen(_stream_cmd(table, ckpt, str(log), "--ready-file", str(ready)), env=env,
                          start_new_session=True, stdout=subprocess.DEVNULL, stderr=open(work / "stream2.err", "w"))
    code2 = p2.wait(timeout=3600)
    restart_lines = _log_lines(log)[len(first_lines):]
    return {"topic": s.topic, "table": table, "mode": mode,
            "crash_after_batches": CRASH_AFTER_BATCHES if mode == "exit" else None,
            "killed_after_logged_batches": killed_at, "first_exit_code": code1, "restart_exit_code": code2,
            "producer": {k: v for k, v in prod["stats"].items() if k != "order"},
            "first_process": _summarise_log(first_lines), "bronze_after_crash": after_crash,
            "restart": _summarise_log(restart_lines),
            "restart_first_batch_inserted": restart_lines[0]["bronze"]["inserted"] if restart_lines
            and "bronze" in restart_lines[0] else None,
            "restart_wall_seconds": round(time.time() - t_restart, 1),
            "bronze_final": check(settings, table)}


def _latency_run(settings, snaps) -> dict:
    s, work, table, env = _prepare(settings, "latency")
    ckpt, log, ready = str(work / "checkpoint"), work / "batches.jsonl", work / "ready"
    p = subprocess.Popen(_stream_cmd(table, ckpt, str(log), "--ready-file", str(ready)), env=env,
                         start_new_session=True, stdout=subprocess.DEVNULL, stderr=open(work / "stream.err", "w"))
    _wait_ready(ready, p)
    time.sleep(10)  # let the query plan its first (empty) batch, so start-up is not counted as latency
    stats = replay(s, snaps, speedup=PACED_SPEEDUP, max_sleep=PACED_MAX_SLEEP)
    code = p.wait(timeout=3600)
    return {"topic": s.topic, "table": table, "speedup": PACED_SPEEDUP, "max_sleep_s": PACED_MAX_SLEEP,
            "exit_code": code, "producer": {k: v for k, v in stats.summary().items() if k != "order"},
            "stream": _summarise_log(_log_lines(log)), "bronze_final": check(settings, table)}


def run(settings, until=None, runs=("crash", "sigkill", "latency")) -> dict:
    snaps, skipped = discover_snapshots(Path(settings.source_dir) / "speedbands", until=until,
                                        min_age_s=settings.snapshot_min_age_s)
    import pyspark
    res = {"snapshots": len(snaps), "first": snaps[0][0].isoformat(), "last": snaps[-1][0].isoformat(),
           "spark": pyspark.__version__, "partitions": settings.partitions, "max_offsets_per_trigger": 500_000}
    out = Path(settings.results_dir) / "spark_stream.json"
    if out.exists():
        res = json.loads(out.read_text(encoding="utf-8")) | res
    for r in runs:
        print(f"== spark stream run: {r}", flush=True)
        if r == "crash":
            res["crash"] = _fault_run(settings, snaps, "crash", "exit")
        elif r == "sigkill":
            res["sigkill"] = _fault_run(settings, snaps, "sigkill", "sigkill")
        elif r == "latency":
            res["latency"] = _latency_run(settings, snaps)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    return res
