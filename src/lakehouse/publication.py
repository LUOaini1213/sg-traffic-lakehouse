"""One committed publication selects five immutable gold tables together.

The Delta publication table is the authority, rather than five independently
overwritten paths or a pair of local JSON reports. Capture one GoldSnapshot
per reader and keep it for all related queries. Older snapshots remain valid.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

import pyarrow as pa
import duckdb

TABLES = ("gold_link_speed_hourly", "gold_congestion_episodes", "gold_stop_demand_hourly",
          "gold_top_od_flows", "gold_link_hour_features")
LEDGER = "gold/_publication"
RELEASE_ID = re.compile(r"[0-9a-f]{32}")
HASH = re.compile(r"[0-9a-f]{64}")


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def release_path(release_id: str, table: str) -> str:
    if not isinstance(release_id, str) or not RELEASE_ID.fullmatch(release_id) or table not in TABLES:
        raise ValueError("Invalid gold publication identity")
    return f"gold/releases/{release_id}/{table}"


def validate_manifest(manifest: dict) -> None:
    if not isinstance(manifest, dict) or type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 1:
        raise ValueError("Unsupported gold publication manifest")
    release_id = manifest.get("release_id")
    tables = manifest.get("tables")
    if not isinstance(tables, dict) or set(tables) != set(TABLES):
        raise ValueError("A publication must contain exactly the five gold tables")
    for table, item in tables.items():
        if not isinstance(item, dict) or item.get("path") != release_path(release_id, table):
            raise ValueError("Publication table must belong to its immutable release")
        for key in ("rows", "files", "delta_version"):
            if type(item.get(key)) is not int or item[key] < 0:
                raise ValueError("Invalid publication table statistics")
        if item["delta_version"] != 0:
            raise ValueError("A release table may have only its initial commit")
        if any(not isinstance(item.get(key), str) or not HASH.fullmatch(item[key])
               for key in ("file_sha256", "content_sha256")):
            raise ValueError("Invalid publication table fingerprint")
    if "layers" in manifest and not isinstance(manifest["layers"], dict):
        raise ValueError("Invalid publication layer counts")
    if "report" in manifest:
        report = manifest["report"]
        if not isinstance(report, dict) or report.get("path") != f"gold/releases/{release_id}/report.html":
            raise ValueError("Report must belong to the same gold publication")
        if type(report.get("bytes")) is not int or report["bytes"] < 1 or not HASH.fullmatch(str(report.get("sha256", ""))):
            raise ValueError("Invalid publication report fingerprint")


@dataclass(frozen=True)
class GoldSnapshot:
    lake: object
    ledger_version: int
    _manifest_json: str

    @property
    def manifest(self) -> dict:
        # A caller cannot mutate the captured reader's paths through this copy.
        return json.loads(self._manifest_json)

    @property
    def release_id(self) -> str:
        return self.manifest["release_id"]

    @property
    def tables(self) -> dict:
        return self.manifest["tables"]

    def delta(self, table: str):
        if table not in TABLES:
            raise ValueError("Unknown gold table")
        item = self.tables[table]
        delta = self.lake.delta(item["path"])
        if delta is None or delta.version() != item["delta_version"]:
            raise ValueError("Published release table is missing or has been modified")
        stats = self.lake.delta_stats(item["path"])
        if stats["rows"] != item["rows"] or stats["files"] != item["files"]:
            raise ValueError("Published release statistics do not match its manifest")
        return delta

    def scan(self, table: str) -> str:
        self.delta(table)
        return self.lake.scan(self.tables[table]["path"])

    def read_report(self) -> bytes:
        artifact = self.manifest.get("report")
        if artifact is None:
            raise ValueError("This data-only publication has no report")
        with self.lake.fs.open_input_stream(self.lake.path(artifact["path"])) as source:
            data = source.read()
        if len(data) != artifact["bytes"] or hashlib.sha256(data).hexdigest() != artifact["sha256"]:
            raise ValueError("Publication report fingerprint does not match")
        return data

    def verify(self) -> None:
        from .publish import GOLD, content_sha256
        for table, item in self.tables.items():
            delta = self.delta(table)
            hashes = self.lake.delta_file_hashes(item["path"])
            digest = hashlib.sha256("".join(hashes).encode()).hexdigest()
            if digest != item["file_sha256"]:
                raise ValueError("Published release file fingerprint does not match")
            # Recompute the content hash from the stored data, independently of
            # the source DuckDB query used by the exporter. Ordering is explicit
            # across all Parquet files, not their incidental file-name order.
            con = duckdb.connect()
            try:
                con.execute("SET memory_limit=?", [self.lake.settings.duckdb_memory])
                con.register("stored_gold", delta.to_pyarrow_dataset())
                if content_sha256(con, "stored_gold", GOLD[table]) != item["content_sha256"]:
                    raise ValueError("Published release content fingerprint does not match")
            finally:
                con.close()
        if "report" in self.manifest:
            self.read_report()


def read_snapshot(lake, version: int | None = None) -> GoldSnapshot:
    delta = lake.delta(LEDGER)
    if delta is None:
        raise ValueError("No committed gold publication; run publish before reading gold")
    if version is not None:
        from deltalake import DeltaTable
        if type(version) is not int or version < 0:
            raise ValueError("Invalid gold publication ledger version")
        delta = DeltaTable(lake.uri(LEDGER), version=version, storage_options=lake.storage_options)
    rows = delta.to_pyarrow_table().to_pylist()
    if len(rows) != 1 or set(rows[0]) != {"manifest_json", "manifest_sha256"}:
        raise ValueError("Corrupt gold publication ledger")
    text, digest = rows[0]["manifest_json"], rows[0]["manifest_sha256"]
    if not isinstance(text, str) or hashlib.sha256(text.encode()).hexdigest() != digest:
        raise ValueError("Corrupt gold publication fingerprint")
    manifest = json.loads(text)
    validate_manifest(manifest)
    if text != canonical(manifest):
        raise ValueError("Noncanonical gold publication manifest")
    return GoldSnapshot(lake, delta.version(), text)


def activate(lake, manifest: dict) -> GoldSnapshot:
    validate_manifest(manifest)
    text = canonical(manifest)
    digest = hashlib.sha256(text.encode()).hexdigest()
    candidate = GoldSnapshot(lake, -1, text)
    # No visibility change until all five real files and their fingerprints pass.
    candidate.verify()
    row = pa.table({"manifest_json": [text], "manifest_sha256": [digest]})
    def receipt():
        latest = lake.delta(LEDGER)
        if latest is None:
            raise ValueError("Gold publication commit could not be confirmed")
        # delta-rs 1.6.6 history() can stream newer commits while numbering them
        # relative to an older loaded snapshot. Pin the upper bound and load
        # actual versions, never trust those history labels for a receipt.
        for version in range(latest.version(), -1, -1):
            committed = read_snapshot(lake, version)
            if committed._manifest_json == text:
                return committed
        raise ValueError("Gold publication commit could not be confirmed")
    try:
        lake.write_delta(LEDGER, row, mode="overwrite", metadata={"release_id": candidate.release_id,
                                                               "manifest_sha256": digest})
    except Exception:
        # A transport/acknowledgment failure can follow a successful commit.
        # Confirm that exact manifest, rather than falsely claiming rollback.
        try:
            return receipt()
        except Exception:
            pass
        raise
    # This remains a receipt for our exact commit even if another complete
    # publisher activated a newer cohort before the acknowledgment was read.
    return receipt()
