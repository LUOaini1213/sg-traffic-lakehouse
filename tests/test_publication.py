"""Whole-cohort publication under real Delta files, failures and concurrent readers."""
import dataclasses
import hashlib
import json
import os
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa
import pytest

from lakehouse import config, publish
from lakehouse.publication import LEDGER, TABLES, activate, read_snapshot
from lakehouse.storage import Lake
from conftest import local_settings


@pytest.fixture
def settings(tmp_path):
    configured = os.environ.get("PUBLICATION_TEST_ROOT")
    local = local_settings(tmp_path)
    if not configured:
        return local
    # Each MinIO case owns a fresh prefix, without touching pipeline gold.
    if not configured.startswith("s3://"):
        raise ValueError("PUBLICATION_TEST_ROOT must be an isolated S3 test prefix")
    remote = config.load()
    return dataclasses.replace(local, lake_root=configured.rstrip("/") + "/" + uuid.uuid4().hex,
                               s3_endpoint=remote.s3_endpoint, s3_key=remote.s3_key, s3_secret=remote.s3_secret)


def stage(settings, lake, marker):
    connection = duckdb.connect()
    try:
        connection.execute("CREATE SCHEMA gold")
        for table, keys in publish.GOLD.items():
            columns = ", ".join(f"0 AS {key}" for key in keys)
            connection.execute(f"CREATE TABLE gold.{table} AS SELECT {columns}, {marker} AS publication_marker")
        release_id = uuid.uuid4().hex
        tables = publish.export_gold(settings, connection, lake, release_id)
        return {"schema_version": 1, "release_id": release_id, "tables": tables,
                "layers": {"gold": {table: entry["rows"] for table, entry in tables.items()}}}
    finally:
        connection.close()


def markers(snapshot):
    return [snapshot.delta(table).to_pyarrow_table()["publication_marker"].to_pylist() for table in TABLES]


def test_interrupted_third_table_never_changes_active_cohort(settings, monkeypatch):
    lake = Lake(settings)
    old = activate(lake, stage(settings, lake, 1))
    writer = lake.write_delta
    def fail_third(rel, *args, **kwargs):
        if rel.endswith("/" + TABLES[2]):
            raise OSError("Controlled third-table write failure")
        return writer(rel, *args, **kwargs)
    monkeypatch.setattr(lake, "write_delta", fail_third)
    with pytest.raises(OSError, match="third-table"):
        stage(settings, lake, 2)
    fresh = read_snapshot(Lake(settings))
    assert fresh.release_id == old.release_id
    assert fresh.ledger_version == old.ledger_version
    assert markers(fresh) == [[1]] * 5


def test_first_failed_publication_exposes_no_partial_gold(settings, monkeypatch):
    lake = Lake(settings)
    writer = lake.write_delta
    def fail_last(rel, *args, **kwargs):
        if rel.endswith("/" + TABLES[-1]):
            raise OSError("Controlled last-table failure")
        return writer(rel, *args, **kwargs)
    monkeypatch.setattr(lake, "write_delta", fail_last)
    with pytest.raises(OSError):
        stage(settings, lake, 1)
    assert lake.delta(LEDGER) is None
    with pytest.raises(ValueError, match="No committed"):
        lake.scan("gold/" + TABLES[0])


def test_reader_keeps_all_old_tables_until_explicit_refresh(settings):
    writer = Lake(settings)
    first = activate(writer, stage(settings, writer, 1))
    reader = Lake(settings)
    captured = reader.gold_snapshot()
    assert reader.delta("gold/" + TABLES[0]).to_pyarrow_table()["publication_marker"].to_pylist() == [1]
    second = activate(writer, stage(settings, writer, 2))
    assert second.release_id != first.release_id
    assert markers(captured) == [[1]] * 5
    assert markers(reader.gold_snapshot()) == [[1]] * 5
    assert markers(reader.refresh_gold_snapshot()) == [[2]] * 5
    assert markers(captured) == [[1]] * 5


def test_release_paths_and_manifest_copies_cannot_modify_a_published_reader(settings):
    lake = Lake(settings)
    snapshot = activate(lake, stage(settings, lake, 1))
    external_copy = snapshot.tables
    external_copy[TABLES[0]]["path"] = "gold/releases/invalid/private"
    assert markers(snapshot) == [[1]] * 5
    with pytest.raises(ValueError, match="immutable"):
        lake.write_delta(snapshot.tables[TABLES[0]]["path"], pa.table({"publication_marker": [2]}), mode="overwrite")
    with pytest.raises(ValueError, match="complete publication"):
        lake.write_delta("gold/" + TABLES[0], pa.table({"publication_marker": [2]}), mode="overwrite")


def test_foreign_writer_cannot_silently_replace_a_release_version(settings):
    from deltalake import write_deltalake
    lake = Lake(settings)
    snapshot = activate(lake, stage(settings, lake, 1))
    table = TABLES[0]
    # A writer bypassing this API has changed the release; readers detect it.
    original = snapshot.delta(table).to_pyarrow_table()
    write_deltalake(lake.uri(snapshot.tables[table]["path"]), original, mode="overwrite",
                   storage_options=lake.storage_options)
    with pytest.raises(ValueError, match="modified"):
        snapshot.delta(table)


@pytest.mark.parametrize("damage", ["missing_table", "foreign_path", "wrong_version", "boolean_rows", "wrong_hash", "wrong_content"])
def test_invalid_or_unverified_candidate_cannot_replace_active_publication(settings, damage):
    lake = Lake(settings)
    old = activate(lake, stage(settings, lake, 1))
    candidate = stage(settings, lake, 2)
    item = candidate["tables"][TABLES[0]]
    if damage == "missing_table":
        candidate["tables"].pop(TABLES[-1])
    elif damage == "foreign_path":
        item["path"] = old.tables[TABLES[0]]["path"]
    elif damage == "wrong_version":
        item["delta_version"] = 1
    elif damage == "boolean_rows":
        item["rows"] = True
    elif damage == "wrong_hash":
        item["file_sha256"] = "0" * 64
    else:
        item["content_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        activate(lake, candidate)
    assert read_snapshot(lake).release_id == old.release_id
    assert markers(read_snapshot(lake)) == [[1]] * 5


def test_activation_failure_before_commit_keeps_previous_publication(settings, monkeypatch):
    lake = Lake(settings)
    old = activate(lake, stage(settings, lake, 1))
    candidate = stage(settings, lake, 2)
    writer = lake.write_delta
    def unavailable(rel, *args, **kwargs):
        if rel == LEDGER:
            raise OSError("Controlled ledger outage")
        return writer(rel, *args, **kwargs)
    monkeypatch.setattr(lake, "write_delta", unavailable)
    with pytest.raises(OSError, match="ledger outage"):
        activate(lake, candidate)
    assert read_snapshot(lake).release_id == old.release_id


def test_lost_acknowledgment_after_commit_is_confirmed_instead_of_false_rollback(settings, monkeypatch):
    lake = Lake(settings)
    activate(lake, stage(settings, lake, 1))
    candidate = stage(settings, lake, 2)
    writer = lake.write_delta
    def lost_ack(rel, *args, **kwargs):
        writer(rel, *args, **kwargs)
        if rel == LEDGER:
            raise OSError("Controlled post-commit acknowledgment loss")
    monkeypatch.setattr(lake, "write_delta", lost_ack)
    receipt = activate(lake, candidate)
    assert receipt.release_id == candidate["release_id"]
    assert receipt.ledger_version == 1
    assert markers(receipt) == [[2]] * 5


def test_receipt_uses_real_versions_after_another_publisher_commits(settings, monkeypatch):
    lake = Lake(settings)
    activate(lake, stage(settings, lake, 1))
    candidate = stage(settings, lake, 2)
    following = stage(settings, lake, 3)
    reader = lake.delta
    advanced = False
    def advance_after_receipt_snapshot_opens(rel):
        nonlocal advanced
        opened = reader(rel)
        if rel == LEDGER and not advanced:
            advanced = True
            # Load A's actual v1 snapshot, then commit B's actual v2 before A
            # confirms its receipt. No stored history or version is fabricated.
            activate(Lake(settings), following)
        return opened
    monkeypatch.setattr(lake, "delta", advance_after_receipt_snapshot_opens)
    receipt = activate(lake, candidate)
    assert receipt.release_id == candidate["release_id"] and receipt.ledger_version == 1
    assert markers(receipt) == [[2]] * 5
    fresh = read_snapshot(Lake(settings))
    assert fresh.release_id == following["release_id"] and fresh.ledger_version == 2
    assert markers(fresh) == [[3]] * 5


def test_concurrent_publishers_expose_one_complete_candidate(settings):
    lake = Lake(settings)
    activate(lake, stage(settings, lake, 1))
    candidates = [stage(settings, lake, marker) for marker in (2, 3)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(activate, Lake(settings), candidate) for candidate in candidates]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except Exception as error:
                # A genuine Delta conflict may reject a publisher; it cannot
                # make a mixed manifest visible or delete the previous release.
                assert "conflict" in str(error).lower() or "concurrent" in str(error).lower()
    assert outcomes
    fresh = read_snapshot(Lake(settings))
    assert fresh.release_id in {candidate["release_id"] for candidate in candidates}
    values = markers(fresh)
    assert values in ([[2]] * 5, [[3]] * 5)


@pytest.mark.parametrize("rows", [[], [{"manifest_json": "{}", "manifest_sha256": "0" * 64}],
                                [{"manifest_json": "{}", "manifest_sha256": hashlib.sha256(b"{}").hexdigest()}]])
def test_corrupt_ledger_never_falls_back_to_latest_individual_tables(settings, rows):
    lake = Lake(settings)
    activate(lake, stage(settings, lake, 1))
    table = pa.Table.from_pylist(rows, schema=pa.schema([("manifest_json", pa.string()), ("manifest_sha256", pa.string())]))
    lake.write_delta(LEDGER, table, mode="overwrite")
    with pytest.raises(ValueError):
        read_snapshot(lake)


def test_valid_manifest_with_corrupt_ledger_digest_is_rejected(settings):
    lake = Lake(settings)
    snapshot = activate(lake, stage(settings, lake, 1))
    lake.write_delta(LEDGER, pa.table({"manifest_json": [snapshot._manifest_json],
                                     "manifest_sha256": ["0" * 64]}), mode="overwrite")
    with pytest.raises(ValueError, match="fingerprint"):
        read_snapshot(lake)


@pytest.fixture
def publication_source(pipeline_run, tmp_path, settings):
    # Copy the real fixture warehouse, leaving the shared end-to-end fixture and
    # its historical results intact. Subsequent tests mutate only this copy.
    source = pipeline_run[0]
    warehouse = tmp_path / "warehouse.duckdb"
    shutil.copy2(source.warehouse, warehouse)
    return dataclasses.replace(settings, warehouse=warehouse)


def test_report_is_captured_with_gold_and_survives_changed_warehouse(publication_source):
    from lakehouse import report
    settings = publication_source
    first = publish.run(settings)
    reader = Lake(settings).gold_snapshot()
    old_page = reader.read_report()
    assert reader.release_id.encode() in old_page
    assert first["publication"]["report"]["sha256"] == hashlib.sha256(old_page).hexdigest()
    con = duckdb.connect(str(settings.warehouse))
    try:
        con.execute("UPDATE gold.gold_top_od_flows SET avg_daily_trips=9876.54 "
                    "WHERE day_type='WEEKDAY' AND flow_rank=1")
    finally:
        con.close()
    # Rendering after the warehouse changes still returns the committed report.
    assert open(report.run(settings), "rb").read() == old_page
    second = publish.run(settings)
    fresh = read_snapshot(Lake(settings))
    assert second["publication"]["release_id"] != reader.release_id
    assert b"9876.54" in fresh.read_report() and b"9876.54" not in old_page
    assert reader.read_report() == old_page
    fresh.verify()


def test_failed_report_preparation_does_not_activate_staged_tables(publication_source, monkeypatch):
    from lakehouse import report
    settings = publication_source
    publish.run(settings)
    old = read_snapshot(Lake(settings))
    def failed_render(*args, **kwargs):
        raise OSError("Controlled report rendering failure")
    monkeypatch.setattr(report, "prepare", failed_render)
    with pytest.raises(OSError, match="rendering failure"):
        publish.run(settings)
    fresh = read_snapshot(Lake(settings))
    assert fresh.release_id == old.release_id and fresh.ledger_version == old.ledger_version
    assert fresh.read_report() == old.read_report()


def test_local_receipt_failure_after_commit_returns_the_real_publication(publication_source, monkeypatch):
    settings = publication_source
    writer = publish._atomic_json
    def unavailable(path, value):
        if path.name == "gold_manifest.json":
            raise OSError("Controlled local receipt outage")
        return writer(path, value)
    monkeypatch.setattr(publish, "_atomic_json", unavailable)
    result = publish.run(settings)
    assert result["receipt_warnings"] == ["Publication committed; local gold_manifest.json receipt could not be replaced"]
    receipt = read_snapshot(Lake(settings))
    assert receipt.release_id == result["publication"]["release_id"]
    assert receipt.ledger_version == result["publication"]["ledger_version"]
    receipt.verify()


def test_changed_report_bytes_are_detected(publication_source):
    settings = publication_source
    publish.run(settings)
    lake = Lake(settings)
    receipt = read_snapshot(lake)
    with lake.fs.open_output_stream(lake.path(receipt.manifest["report"]["path"])) as output:
        output.write(b"Changed report")
    with pytest.raises(ValueError, match="report fingerprint"):
        receipt.read_report()
    with pytest.raises(ValueError, match="report fingerprint"):
        receipt.verify()
