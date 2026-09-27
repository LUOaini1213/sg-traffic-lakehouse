"""Reproducibility check: run the whole batch pipeline twice from empty storage and compare the gold outputs.

Each run gets its own lake (a new bucket on MinIO, or a new directory), its own DuckDB warehouse and its own results
folder. Inputs are pinned by --until. The result, per gold table, is whether the Parquet bytes and the row content
(key-ordered CSV) hash the same. Written to <results>/reproducibility.json.
"""
from __future__ import annotations

import dataclasses
import json
import shutil
import time
from pathlib import Path

from .storage import Lake


def run(settings, until: str | None, workdir: str) -> dict:
    from .cli import run_pipeline
    runs = {}
    for tag in ("a", "b"):
        w = Path(workdir) / tag
        shutil.rmtree(w, ignore_errors=True)
        root = f"s3://sgtl-repro-{tag}" if settings.is_s3 else str(w / "lake")
        s = dataclasses.replace(settings, lake_root=root, warehouse=w / "warehouse.duckdb", results_dir=w / "results")
        lake = Lake(s)
        for prefix in ("bronze", "gold"):
            lake.delete_prefix(prefix)
        t0 = time.perf_counter()
        timings = run_pipeline(s, until)
        runs[tag] = {"lake_root": root, "seconds": round(time.perf_counter() - t0, 1), "timings": timings,
                     "manifest": json.loads((w / "results" / "gold_manifest.json").read_text(encoding="utf-8"))}
    a, b = runs["a"]["manifest"], runs["b"]["manifest"]
    tables = {t: {"rows": a[t]["rows"],
                  "file_sha256_equal": a[t]["file_sha256"] == b[t]["file_sha256"],
                  "content_sha256_equal": a[t]["content_sha256"] == b[t]["content_sha256"],
                  "content_sha256": a[t]["content_sha256"]} for t in a}
    res = {"until": until, "identical_files": all(v["file_sha256_equal"] for v in tables.values()),
           "identical_content": all(v["content_sha256_equal"] for v in tables.values()),
           "run_seconds": {k: v["seconds"] for k, v in runs.items()}, "tables": tables}
    out = Path(settings.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reproducibility.json").write_text(json.dumps(res, indent=2), encoding="utf-8")
    return res
