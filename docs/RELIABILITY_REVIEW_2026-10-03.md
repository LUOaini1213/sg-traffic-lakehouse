# Reliability review (2026-10-03)

This section was in the README until 2026-11-03. It records three defects that were reproduced and fixed on
2026-10-03. The README's full-data results were not re-measured for them (see the notes under "Measured results"
there).

Three defects were reproduced against the previous implementation and fixed:

- **Repeated invalid timestamps could stop consumption after every restart.** A rejected timestamp with a time
  zone was cached before validation. Its next copy bypassed validation and crashed when compared with the naive
  Singapore event times. Only validated timestamps now enter the cache. Tests use the real consumer with an
  in-memory broker and local Delta tables: repeated bad records and end markers go to dead letter, valid records
  persist, and a crash before offset commit can replay without duplicating either sink.
- **A historical cutoff ignored newer data already in bronze.** Previously it only limited file discovery.
  The CLI now passes the same normalized cutoff into dbt, and the staging model filters before deduplication or
  aggregation. Airflow passes its existing cutoff to the transform task as well as ingest and quality.
  A fixture test first stores all ten snapshots, rebuilds at 08:20 (24 observations, two snapshots and 23
  known-band observations in gold), then rebuilds without a cutoff (120 observations). Bronze is unchanged.
- **OD reconciliation could pass incomplete comparisons.** An inner join hid a missing day type, and zero tap-ins
  with positive OD trips became a zero difference. Shared months now compare the union of their day types;
  missing groups and undefined ratios fail explicitly and retain both totals in the report. Zero versus zero
  passes. Products with different month coverage are still compared only on their common months.

Standalone bounded transformation: `python -m lakehouse dbt --until 202609050820 build`. Put the cutoff before
the dbt command; Airflow uses `--cutoff-utc` in the same position. An unbounded invocation explicitly clears any
inherited cutoff. The new consumer and quality tests are offline; they do not re-establish the historical
throughput measurements or prove behavior on a live distributed cluster.

At the time, the full local test suite (including the fixture dbt builds and the two-run reproducibility check),
the standalone fixture pipeline and the README consistency check passed, and all six new mutations were killed
locally; the mutation harness was extended at the same time to run the bounded dbt test for SQL mutations. The
current test and mutant counts are in the README's Tests subsection, which CI re-measures on every push.
