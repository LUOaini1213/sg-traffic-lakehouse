# Coherent gold publication

The previous exporter overwrote five unrelated Delta tables. Each table had an atomic commit, but a failure
partway through export could leave two new tables and three old tables. The publisher now stages all five
tables under a fresh `gold/releases/<32-hex-release-id>/` directory. Each release table has exactly its initial
Delta commit. It verifies row/file counts, Parquet fingerprints and sorted content fingerprints from the
stored data before one Delta commit to `gold/_publication` selects the complete manifest.

Charts and gold tables in `report.html` are rendered in the same read-only DuckDB transaction as export.
The report is stored inside that release and its size and SHA-256 are checked before activation. The
`report` command copies those committed bytes, even if the warehouse has since changed. Captured quality
diagnostics remain explicitly labelled as previously generated: they include source-file checks and are
not recomputed in this transaction. Bronze counts likewise describe live bronze at publication time;
they are not a transaction spanning the mutable bronze tables.

## Read one cohort

```python
from lakehouse.config import load
from lakehouse.storage import Lake

lake = Lake(load())
snapshot = lake.gold_snapshot()  # capture once for this reader
speed = snapshot.delta("gold_link_speed_hourly").to_pyarrow_table()
demand = snapshot.delta("gold_stop_demand_hourly").to_pyarrow_table()
snapshot.verify()  # independently read and hash all stored tables and the report
```

Logical `Lake` reads such as `lake.scan("gold/gold_link_speed_hourly")` use the same captured manifest
for that Lake instance. A later publication does not move an existing reader to another cohort.
Call `lake.refresh_gold_snapshot()` at a deliberate boundary to capture a new cohort; previously held
`GoldSnapshot` objects still refer to their original release. Do not create a fresh Lake for every table
within one related analysis.

```bash
PYTHONPATH=src python -m lakehouse publish
PYTHONPATH=src python -m lakehouse publication --verify
PYTHONPATH=src python -m lakehouse publication --version 0 --verify
PYTHONPATH=src python -m lakehouse report
```

The manifest contains the release ID, exactly five paths, Delta versions, row/file counts, file and content
hashes, report fingerprint and normalized UTF-8/LF Python source hashes. The ledger version identifies the
activation, not a data-table version. Historical inspection does not reactivate that version.

## Failure and concurrency

- Failure before activation leaves the previous ledger and release intact. An interrupted staging directory
  is an unreferenced candidate, never a partially published cohort. On the first failed attempt there is no
  active gold publication, and reads fail explicitly.
- Concurrent publishers each prepare their own release. Delta may reject a conflicting writer; if both
  succeed, the latest ledger selects one complete manifest. A successful publisher confirms its exact
  manifest at an actual pinned ledger version, even when another complete publication has already become
  latest. It does not trust `history()` version labels: delta-rs 1.6.6 can number a longer concurrent history
  stream relative to an older snapshot. The receipt pins an upper version and compares stored manifests.
- A transport error after the ledger commit may lose the acknowledgment. The publisher confirms its exact
  committed manifest before returning success. If it cannot prove that commit, it raises the original error;
  inspect `publication --verify` before retrying. Retrying builds another release, not an in-place overwrite.
- Per-release local manifest preparation happens before activation. Replacing top-level JSON receipts happens
  afterward. If a local replacement fails, `publish` returns a committed result and receipt warnings. The
  Delta ledger remains authoritative; do not infer rollback from a stale local JSON file.

## Upgrade and retention

Run a new `publish` after upgrading. Legacy physical `gold/<table>` directories are left untouched, but
logical reads require the new ledger and never fall back to those independent paths. External consumers
must read the ledger once and use its release paths rather than construct the old physical paths.

This API prevents overwriting release tables, and detects a writer that bypasses it and adds a new Delta
version. Full `verify` detects changed data or report bytes. This is a consistency protocol, not an object
store access-control guarantee; readers depend on preserved release objects and Delta logs.

No automatic garbage collection or vacuum runs. Old readers and historical ledger versions need their
release directories. Delete an unreferenced candidate or old release only after separately proving no
active or retained reader/version needs it. Full-data verification reads every published file, adding I/O;
no new performance comparison is claimed from the synthetic failure tests.
