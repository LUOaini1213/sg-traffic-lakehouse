"""`python -m lakehouse inputs`: a DataMall folder is compared file by file with the recorded SHA-256s."""
import hashlib
import json

from conftest import FIXTURE
from lakehouse import config, inputs
from lakehouse.config import REPO


def _results_for(folder, tmp_path):
    """A results folder that records the fixture's own files, as a pipeline run on them would."""
    sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()  # noqa: E731
    res = tmp_path / "results"
    res.mkdir()
    loads = [{"file": p.name, "sha256": sha(p), "source": {"rows": 1}} for p in sorted(folder.glob("*.csv"))]
    snaps = [{"file": p.name, "sha256": sha(p), "source_rows": 1}
             for p in sorted((folder / "speedbands").glob("sb_*.csv.gz"))]
    (res / "batch_ingest.json").write_text(json.dumps({"loads": loads}), encoding="utf-8")
    (res / "speedband_files.json").write_text(json.dumps({"snapshots": snaps}), encoding="utf-8")
    return res


def test_identical_folder_matches(tmp_path):
    res = _results_for(FIXTURE, tmp_path)
    rows = inputs.compare(FIXTURE, res)
    assert len(rows) > 10 and {r["status"] for r in rows} == {"match"}
    assert inputs.run(FIXTURE, res) == 0


def test_changed_and_missing_files_are_reported(tmp_path):
    res = _results_for(FIXTURE, tmp_path)
    loads = json.loads((res / "batch_ingest.json").read_text(encoding="utf-8"))
    loads["loads"][0]["sha256"] = "0" * 64
    loads["loads"].append({"file": "transport_node_bus_209901.csv", "sha256": "1" * 64, "source": {"rows": 1}})
    (res / "batch_ingest.json").write_text(json.dumps(loads), encoding="utf-8")
    status = {r["path"]: r["status"] for r in inputs.compare(FIXTURE, res)}
    assert status[loads["loads"][0]["file"]] == "differs"
    assert status["transport_node_bus_209901.csv"] == "missing"
    assert inputs.run(FIXTURE, res) == 1


def test_committed_results_record_every_real_input():
    rows = inputs.expected(REPO / "results")
    assert len(rows) == 35 and all(len(r["sha256"]) == 64 for r in rows)
    assert sum(r["path"].startswith("speedbands/") for r in rows) == 25


def test_relative_paths_in_env_are_relative_to_the_repository(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DUCKDB_PATH", ".lake/stack/warehouse.duckdb")
    monkeypatch.setenv("SOURCE_DIR", "../elsewhere/datamall")
    s = config.load()
    assert s.warehouse == REPO / ".lake" / "stack" / "warehouse.duckdb"
    assert s.source_dir == REPO / ".." / "elsewhere" / "datamall"
