"""Command-line entry point: python -m lakehouse <command>.

The batch pipeline without Airflow (used by CI and by the reproducibility check):
    python -m lakehouse pipeline [--until YYYYmmddHHMM]
runs ingest-batch -> ingest-speedbands -> profile -> dbt build -> quality -> publish -> report.
The Airflow DAG (airflow/dags/sg_traffic_lakehouse.py) runs the same commands as separate tasks.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from . import config
from .config import REPO

DBT_DIR = REPO / "dbt"


def _ts(s: str | None) -> dt.datetime | None:
    return dt.datetime.strptime(s, "%Y%m%d%H%M") if s else None


def _cutoff(a) -> dt.datetime | None:
    """Snapshot cutoff as naive SGT: --until YYYYmmddHHMM (SGT), or --cutoff-utc <ISO timestamp with zone> (what the
    Airflow DAG passes: the run's data_interval_end)."""
    iso = getattr(a, "cutoff_utc", None)
    if iso:
        from zoneinfo import ZoneInfo
        t = dt.datetime.fromisoformat(iso)
        if t.tzinfo is None:
            raise SystemExit("--cutoff-utc needs a time zone, e.g. 2026-09-27T11:23:00+00:00")
        cut = t.astimezone(ZoneInfo("Asia/Singapore")).replace(tzinfo=None)
        print(f"snapshot cutoff: {cut:%Y%m%d%H%M} SGT (from {iso})", flush=True)
        return cut
    return _ts(getattr(a, "until", None))


def _write(settings, name: str, obj) -> None:
    out = Path(settings.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / name).write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def dbt_env(settings, *, until: dt.datetime | None = None) -> dict:
    env = dict(os.environ)
    env.update({
        "LAKE_ROOT": settings.lake_root,
        "DUCKDB_PATH": str(settings.warehouse),
        "DUCKDB_MEMORY": settings.duckdb_memory,
        "DBT_PROFILES_DIR": str(DBT_DIR),
        "DBT_TARGET_PATH": str(Path(settings.warehouse).parent / "dbt_target"),
        "DBT_LOG_PATH": str(Path(settings.warehouse).parent / "dbt_logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        # Explicitly clear this on unbounded runs; an inherited or previous cutoff must not leak in.
        "SNAPSHOT_CUTOFF_SGT": until.strftime("%Y-%m-%d %H:%M:%S") if until else "",
        "S3_ENDPOINT": settings.s3_endpoint,
        "AWS_ACCESS_KEY_ID": settings.s3_key,
        "AWS_SECRET_ACCESS_KEY": settings.s3_secret,
    })
    return env


def run_dbt(settings, args: list[str], *, until: dt.datetime | None = None) -> int:
    Path(settings.warehouse).parent.mkdir(parents=True, exist_ok=True)
    exe = Path(sys.executable).parent / "dbt"
    cmd = [str(exe) if exe.exists() else "dbt", *args, "--project-dir", str(DBT_DIR)]
    return subprocess.run(cmd, env=dbt_env(settings, until=until), cwd=DBT_DIR).returncode


def cmd_ingest_batch(settings, a):
    from . import batch_ingest
    loads = batch_ingest.run(settings)
    _write(settings, "batch_ingest.json", {"loads": loads})


def cmd_ingest_speedbands(settings, a):
    from .speedbands import load_files
    from .storage import Lake
    res = load_files(settings, Lake(settings), until=_cutoff(a), since=_ts(a.since))
    _write(settings, "speedband_files.json", res)


def cmd_profile(settings, a):
    from . import profiling
    rep = profiling.run(settings)
    if not rep["all_reconciled"]:
        sys.exit("bronze does not reconcile with the source files; see profile_bronze.md")


def cmd_dbt(settings, a):
    rc = run_dbt(settings, a.dbt_args or ["build"], until=_cutoff(a))
    if rc:
        sys.exit(rc)


def cmd_quality(settings, a):
    from . import quality
    results, fail = quality.run(settings, strict=a.strict, until=_cutoff(a))
    for r in results:
        print(f"{r['status']:5} {r['check']}: {r['value']} (threshold {r['threshold']})")
    if fail:
        sys.exit("an error-severity quality check failed")


def cmd_publish(settings, a):
    from . import publish
    res = publish.run(settings)
    print(json.dumps(res["layers"], indent=2))
    print(json.dumps({"release_id": res["publication"]["release_id"],
                      "ledger_version": res["publication"]["ledger_version"],
                      "receipt_warnings": res["receipt_warnings"]}))


def cmd_report(settings, a):
    from . import report
    print(report.run(settings))


def cmd_publication(settings, a):
    from .publication import read_snapshot
    from .storage import Lake
    snapshot = read_snapshot(Lake(settings), a.version)
    if a.verify:
        snapshot.verify()
    print(json.dumps({"ledger_version": snapshot.ledger_version, "verified": a.verify,
                      **snapshot.manifest}, indent=2))


def cmd_produce(settings, a):
    from .producer import replay
    from .speedbands import discover_snapshots
    snaps, skipped = discover_snapshots(Path(settings.source_dir) / "speedbands", until=_ts(a.until),
                                        since=_ts(a.since), min_age_s=settings.snapshot_min_age_s)
    if a.limit:
        snaps = snaps[: a.limit]
    stats = replay(settings, snaps, speedup=a.speedup, max_sleep=a.max_sleep, dup_rate=a.dup_rate, late=a.late)
    summary = stats.summary() | {"skipped": skipped, "speedup": a.speedup, "dup_rate": a.dup_rate, "late": a.late}
    print(json.dumps({k: v for k, v in summary.items() if k != "order"}, indent=2))
    if a.out:
        _write(settings, a.out, summary)


def cmd_consume(settings, a):
    from . import consumer
    from .storage import Lake
    res = consumer.run(settings, Lake(settings), group_id=a.group, idle_exit_s=a.idle_exit,
                       flush_timeout_s=a.flush_timeout, allowed_lateness_min=a.allowed_lateness,
                       max_seconds=a.max_seconds, ready_file=a.ready_file,
                       latency_file=a.latency_file, crash_after_writes=a.crash_after_writes)
    print(json.dumps({k: v for k, v in res.items() if k != "flush_log"}, indent=2))
    if a.out:
        _write(settings, a.out, res)


def run_pipeline(settings, until: str | None = None, strict: bool = False, after_step=None) -> dict:
    """Batch pipeline without Airflow. Returns step timings. after_step(name) runs after each step (tests use it
    to plant a defect between two steps)."""
    ns = argparse.Namespace(until=until, since=None, dbt_args=["build"], strict=strict)
    timings = {}
    for name, fn in [("ingest-batch", cmd_ingest_batch), ("ingest-speedbands", cmd_ingest_speedbands),
                     ("profile", cmd_profile), ("dbt-build", cmd_dbt), ("quality", cmd_quality),
                     ("publish", cmd_publish), ("report", cmd_report)]:
        t0 = time.perf_counter()
        print(f"== {name}", flush=True)
        fn(settings, ns)
        timings[name] = round(time.perf_counter() - t0, 2)
        if after_step:
            after_step(name)
    _write(settings, "pipeline_timings.json", timings)
    return timings


def cmd_pipeline(settings, a):
    print(json.dumps(run_pipeline(settings, a.until, a.strict), indent=2))


def cmd_repro(settings, a):
    from . import repro
    res = repro.run(settings, a.until, a.workdir)
    print(json.dumps({k: v for k, v in res.items() if k != "tables"}, indent=2))
    if not res["identical_content"]:
        sys.exit("gold outputs differ between the two runs")


def cmd_summarize(settings, a):
    from . import summarize
    summarize.run(settings, update_readme=a.update_readme, check=a.check)


def cmd_stream_bench(settings, a):
    from . import stream_bench
    res = stream_bench.run(settings, _ts(a.until), fresh=not a.keep_bronze)
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "flushes"} if isinstance(v, dict) else v
                      for k, v in res.items()}, indent=2))


def cmd_live(settings, a):
    from . import live
    live.run(settings, a.every, a.count)


def main(argv=None):
    p = argparse.ArgumentParser(prog="lakehouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ingest-batch", help="monthly PV/OD CSVs, bus network JSON and link metadata -> bronze")
    s = sub.add_parser("ingest-speedbands", help="backfill bronze speed bands from snapshot files (no Kafka)")
    s.add_argument("--until"), s.add_argument("--since")
    s.add_argument("--cutoff-utc", help="cutoff as an ISO timestamp with zone (Airflow's data_interval_end)")
    sub.add_parser("profile", help="profile bronze and reconcile it with the source files")
    s = sub.add_parser("dbt", help="run dbt with the lake settings, e.g. `dbt build` or `dbt docs generate`")
    s.add_argument("--until", help="snapshot cutoff in SGT; place before the dbt command")
    s.add_argument("--cutoff-utc", help="cutoff with time zone (Airflow's data_interval_end); place before build")
    s.add_argument("dbt_args", nargs=argparse.REMAINDER)
    s = sub.add_parser("quality", help="custom data-quality checks")
    s.add_argument("--strict", action="store_true", help="exit non-zero if an error-severity check fails")
    s.add_argument("--until", help="the snapshot cutoff the pipeline used (reported by sampler_log_consistency)")
    s.add_argument("--cutoff-utc", help="cutoff as an ISO timestamp with zone (Airflow's data_interval_end)")
    sub.add_parser("publish", help="export gold to the lake and record row counts and hashes")
    sub.add_parser("report", help="copy the active publication's committed HTML report")
    s = sub.add_parser("publication", help="inspect a coherent gold cohort, independently of local receipts")
    s.add_argument("--version", type=int, help="historical publication ledger version")
    s.add_argument("--verify", action="store_true", help="verify stored rows, files and report against the manifest")
    s = sub.add_parser("pipeline", help="the whole batch pipeline without Airflow")
    s.add_argument("--until"), s.add_argument("--strict", action="store_true")
    s = sub.add_parser("produce", help="replay snapshot files to Kafka")
    s.add_argument("--speedup", type=float, default=0.0, help="event-time speed-up; 0 = as fast as possible")
    s.add_argument("--max-sleep", type=float, default=10.0, help="cap on one pause between snapshots, seconds")
    s.add_argument("--dup-rate", type=float, default=0.0)
    s.add_argument("--late", type=int, default=0)
    s.add_argument("--limit", type=int, default=0)
    s.add_argument("--until"), s.add_argument("--since"), s.add_argument("--out")
    s = sub.add_parser("consume", help="Kafka -> bronze consumer")
    s.add_argument("--group", default="sgtl-bronze-writer")
    s.add_argument("--idle-exit", type=float, default=20.0)
    s.add_argument("--flush-timeout", type=float, default=30.0)
    s.add_argument("--allowed-lateness", type=float, default=30.0, help="minutes of event time")
    s.add_argument("--max-seconds", type=float, default=3600.0)
    s.add_argument("--ready-file", help="written once partitions are assigned")
    s.add_argument("--latency-file", help="save end-of-snapshot-to-write latencies (.npy) for the benchmark")
    s.add_argument("--crash-after-writes", type=int, default=0,
                   help="exit abruptly after this many bronze writes, before committing offsets (crash test)")
    s.add_argument("--out")
    s = sub.add_parser("stream-bench", help="throughput, latency and re-run measurements on the Docker stack")
    s.add_argument("--until"), s.add_argument("--keep-bronze", action="store_true")
    s = sub.add_parser("repro", help="run the batch pipeline twice from scratch and compare gold hashes")
    s.add_argument("--until"), s.add_argument("--workdir", default="/root/lakehouse-data/repro")
    s = sub.add_parser("summarize", help="write results/SUMMARY.md from the results files")
    s.add_argument("--update-readme", action="store_true")
    s.add_argument("--check", action="store_true", help="fail if README.md disagrees with the results files")
    s = sub.add_parser("live", help="fetch live snapshots from DataMall (needs LTA_KEY) and publish them to Kafka")
    s.add_argument("--every", type=float, default=900.0), s.add_argument("--count", type=int, default=1)
    a = p.parse_args(argv)
    settings = config.load()
    {"ingest-batch": cmd_ingest_batch, "ingest-speedbands": cmd_ingest_speedbands, "profile": cmd_profile,
     "dbt": cmd_dbt, "quality": cmd_quality, "publish": cmd_publish, "report": cmd_report, "publication": cmd_publication,
     "pipeline": cmd_pipeline, "produce": cmd_produce, "consume": cmd_consume, "repro": cmd_repro,
     "summarize": cmd_summarize, "live": cmd_live, "stream-bench": cmd_stream_bench}[a.cmd](settings, a)


if __name__ == "__main__":
    main()
