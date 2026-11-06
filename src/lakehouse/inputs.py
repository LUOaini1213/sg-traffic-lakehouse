"""Compare a DataMall folder (SOURCE_DIR) with the input files that the committed measurements used.

results/batch_ingest.json and results/speedband_files.json record the SHA-256 of every input file. This command
hashes the files of the same names under SOURCE_DIR (snapshots under SOURCE_DIR/speedbands) and reports, per file,
`match`, `differs` or `missing`. It exits 1 unless every file matches, so a new download can be told apart from the
files behind the numbers in the README before any pipeline run.

    SOURCE_DIR=../sg-bus-network-monitor/data/raw/datamall python -m lakehouse inputs
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


def expected(results_dir: Path) -> list[dict]:
    """Files recorded in the results: path relative to SOURCE_DIR, SHA-256 and source rows."""
    out = []
    loads = results_dir / "batch_ingest.json"
    if loads.exists():
        for x in json.loads(loads.read_text(encoding="utf-8"))["loads"]:
            out.append({"path": x["file"], "sha256": x["sha256"], "rows": x["source"]["rows"]})
    snaps = results_dir / "speedband_files.json"
    if snaps.exists():
        for x in json.loads(snaps.read_text(encoding="utf-8"))["snapshots"]:
            out.append({"path": f"speedbands/{x['file']}", "sha256": x["sha256"], "rows": x["source_rows"]})
    return out


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def compare(source_dir: Path, results_dir: Path) -> list[dict]:
    rows = []
    for e in expected(results_dir):
        p = Path(source_dir) / e["path"]
        status = "missing" if not p.is_file() else ("match" if _sha256(p) == e["sha256"] else "differs")
        rows.append({**e, "status": status})
    return rows


def run(source_dir: Path, results_dir: Path) -> int:
    rows = compare(source_dir, results_dir)
    if not rows:
        print(f"no input files are recorded in {results_dir}")
        return 1
    for r in rows:
        print(f"{r['status']:8} {r['path']}  ({r['rows']:,} rows in the measured file)")
    counts = {s: sum(r["status"] == s for r in rows) for s in ("match", "differs", "missing")}
    print(f"{counts['match']}/{len(rows)} files match the measured inputs; {counts['differs']} differ, "
          f"{counts['missing']} missing (SOURCE_DIR={source_dir})")
    return 0 if counts["match"] == len(rows) else 1
