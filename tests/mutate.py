"""Mutation check: each deliberate bug below must make at least one test fail.

Pattern: first the unmodified code must pass (otherwise every mutant would look "killed"); then, for each mutant,
every target text must occur exactly once, the mutated file must still be valid (Python: py_compile; SQL: the test
run must not stop on a parser/binder/compilation error), and the test run must fail. Every mutant must be killed.

Mutants tagged "review" are the ones an external review planted in the previous version of this repository (A-J);
most of them survived that version's tests. They are re-expressed here against the current code.

Run: python tests/mutate.py            (roughly 30-60 minutes: SQL mutants rebuild the fixture with dbt)
     python tests/mutate.py --spark    (the PySpark mutants below, against tests/test_spark.py; needs pyspark + a JDK)
"""
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PKG = REPO / "src" / "lakehouse"
MODELS = REPO / "dbt" / "models"
# fast unit tests first, so a Python mutant usually dies before the slower end-to-end fixture run
TESTS = [str(REPO / "tests" / f) for f in
         ("test_quality.py", "test_speedbands.py", "test_stream.py", "test_consumer.py", "test_batch.py", "test_publication.py",
          "test_cutoff.py", "test_e2e.py")]


def M(path, old, new=None, tag=""):
    """One mutant: a single (old, new) replacement, or a list of them applied together."""
    pairs = old if new is None else [(old, new)]
    return {"file": path, "pairs": pairs, "tag": tag}


MUTANTS = [
    # --- complete publication and pinned readers ----------------------------------------------------------
    M(PKG / "publication.py", "    candidate.verify()\n", "    pass\n", tag="review publication"),
    M(PKG / "publication.py", "set(tables) != set(TABLES)", "not tables", tag="review publication"),
    M(PKG / "publication.py", 'delta.version() != item["delta_version"]', "False", tag="review publication"),
    M(PKG / "publication.py", "hashlib.sha256(text.encode()).hexdigest() != digest", "False", tag="review publication"),
    M(PKG / "publication.py", "            committed = read_snapshot(lake, version)\n",
      "            committed = read_snapshot(lake)\n", tag="review publication"),
    M(PKG / "storage.py", "            if self._gold_snapshot is None:\n", "            if True:\n", tag="review publication"),
    M(PKG / "storage.py", "from threading import Lock\n", "from contextlib import nullcontext as Lock\n",
      tag="review publication"),
    M(PKG / "report.py", "    data = snapshot.read_report()\n", '    data = b"Wrong publication report"\n',
      tag="review publication"),
    # --- current review: repeated poison records, shared-lake cutoff, incomplete OD comparisons ---------------
    M(PKG / "consumer.py", "            ts = dt.datetime.fromisoformat(raw_ts)\n",
      "            ts = ts_cache[raw_ts] = dt.datetime.fromisoformat(raw_ts)\n", tag="review 20261003"),
    M(PKG / "cli.py", 'rc = run_dbt(settings, a.dbt_args or ["build"], until=_cutoff(a))',
      'rc = run_dbt(settings, a.dbt_args or ["build"], until=None)', tag="review 20261003"),
    M(MODELS / "silver" / "stg_speedband_obs.sql", "{% if cutoff %}", "{% if false %}", tag="review 20261003"),
    M(REPO / "airflow" / "dags" / "sg_traffic_lakehouse.py", 'f"{CLI} dbt {CUTOFF} build"',
      'f"{CLI} dbt build"', tag="review 20261003"),
    M(PKG / "quality.py", 'from t full outer join o using (year_month, day_type)',
      'from t join o using (year_month, day_type)', tag="review 20261003"),
    M(PKG / "quality.py", 'elif tap_in == 0 and trips != 0:', 'elif False:', tag="review 20261003"),
    # --- parsing and schema enforcement ------------------------------------------------------------------------
    M(PKG / "speedbands.py", r'SNAPSHOT_RE = re.compile(r"^sb_(\d{8})_(\d{4})\.csv\.gz$")',
      r'SNAPSHOT_RE = re.compile(r"sb_(\d{8})_(\d{4})")'),
    M(PKG / "speedbands.py", "    if header != SOURCE_HEADER:\n", "    if False:\n"),
    M(PKG / "speedbands.py", "        if now - p.stat().st_mtime < min_age_s:\n", "        if False:\n"),
    M(PKG / "speedbands.py", "        if not is_complete_gzip(p):\n", "        if False:\n"),
    M(PKG / "speedbands.py", "        malformed.append(row.text)\n", "        pass\n"),
    M(PKG / "speedbands.py",
      "        reject = pc.if_else(pc.and_(pc.is_null(reject), bad), pa.scalar(reason), reject)\n",
      "        reject = reject\n"),
    M(PKG / "batch_ingest.py", "    if header != list(ds.columns):\n", "    if False:\n"),
    M(PKG / "batch_ingest.py", '"TRY_CAST(trim(TIME_PER_HOUR) AS TINYINT) NOT BETWEEN 0 AND 23"',
      '"TRY_CAST(trim(TIME_PER_HOUR) AS TINYINT) NOT BETWEEN 0 AND 24"'),
    M(PKG / "batch_ingest.py", "'[0-9]{{5}}'", "'[0-9]{{4,5}}'"),
    M(PKG / "batch_ingest.py", """f"{col} IS NULL OR NOT regexp_full_match(trim({col}), '{INT_SQL}') \"""",
      """f"{col} IS NULL \""""),
    M(PKG / "batch_ingest.py", "    rows_read = stats[0] + len(bad_lines)\n", "    rows_read = stats[0]\n"),
    M(PKG / "batch_ingest.py", "                if len(row) != len(columns):\n                    continue\n",
      "                pass\n"),
    # --- partitioning ------------------------------------------------------------------------------------------
    M(PKG / "speedbands.py", '    cols["obs_date"] = pc.cast(ts, pa.date32())\n',
      '    cols["obs_date"] = pa.array([ingested_at.date()] * n, pa.date32())\n'),
    M(PKG / "messages.py", "    return int(link_id) % partitions", "    return 0"),
    # --- dedupe, idempotency, concurrency ----------------------------------------------------------------------
    M(PKG / "speedbands.py", '.aggregate([("_i", "min")])', '.aggregate([("_i", "max")])'),
    M(PKG / "speedbands.py", 'KEY = ["snapshot_ts", "link_id"]', 'KEY = ["link_id"]'),
    M(PKG / "speedbands.py", [  # C: the writer rewrites stored rows (last writer wins) instead of inserting new keys
        ('            new = incoming.join(existing.select(["link_id"]), keys="link_id", join_type="left anti",\n'
         '                                use_threads=False).sort_by("link_id")\n',
         "            new = incoming\n"),
        (".when_not_matched_insert_all().execute())", ".when_matched_update_all().when_not_matched_insert_all().execute())"),
    ], tag="review C"),
    M(PKG / "speedbands.py", "            existing = self._slice(ts, part)\n",
      '            existing = self._slice(ts, part).slice(0, 0)\n'),
    M(PKG / "speedbands.py", '"and t.snapshot_ts = s.snapshot_ts and t.link_id = s.link_id")',
      '"and t.snapshot_ts = s.snapshot_ts")'),
    M(PKG / "speedbands.py",
      "            except CommitFailedError:\n                time.sleep(min(0.05 * (attempt + 1), 1.0))\n"
      "                continue\n",
      "            except CommitFailedError:\n                raise\n"),
    M(PKG / "speedbands.py", "    if parsed.rejects.num_rows == 0:\n        if t is None:\n            return\n",
      "    if parsed.rejects.num_rows == 0:\n        return\n"),
    M(PKG / "consumer.py", "        if link_id in p.rows:\n", "        if False:\n"),
    M(PKG / "consumer.py", "            p.conflicts += p.rows[link_id] != value\n", "            p.conflicts += 0\n"),
    # --- consumer: delivery, rebalancing, bad messages ---------------------------------------------------------
    M(PKG / "consumer.py", "        if self.uncommitted and not self.asm.pending and not self.dlq.buffer:\n",
      "        if self.uncommitted:\n", tag="review A"),
    M(PKG / "consumer.py", "        self.asm.owned = {p.partition for p in partitions}\n",
      "        self.asm.owned = set()\n", tag="review B"),
    M(PKG / "consumer.py", "        self.dropped_on_revoke += self.asm.drop_partitions(gone)\n",
      "        self.dropped_on_revoke += 0\n"),
    M(PKG / "consumer.py", "                if not _no_offset(e):\n                    raise\n", "                pass\n"),
    M(PKG / "consumer.py", "    if m.partition_for(link, partitions) != partition:\n", "    if False:\n"),
    M(PKG / "consumer.py", "            self.dlq.add(msg, str(e))\n            return\n", "            raise\n"),
    M(PKG / "consumer.py", 'predicate="t.topic = s.topic and t.kafka_partition = s.kafka_partition "',
      'predicate="false and t.topic = s.topic and t.kafka_partition = s.kafka_partition "'),
    M(PKG / "consumer.py", "                                     late=info.late)\n",
      "                                     late=None)\n"),
    # --- streaming: completeness, lateness, ordering -----------------------------------------------------------
    M(PKG / "consumer.py", "            if p.expected is not None:\n", "            if p.expected is not None or p.rows:\n"),
    M(PKG / "consumer.py", "complete = p.expected is not None and len(links) >= p.expected",
      "complete = p.expected is not None"),
    M(PKG / "speedbands.py", "        if rows.num_rows and set(split_by_partition(rows, partitions)) != {part}:\n",
      "        if False:\n"),
    M(PKG / "consumer.py", "late=wm is not None and ts < wm,", "late=False,"),
    M(PKG / "consumer.py", "            elif now - p.last_update >= self.flush_timeout_s:\n", "            elif False:\n"),
    M(PKG / "producer.py", "yield p, m.EOS_KEY, m.encode_eos(ts_iso, len(seen[p]), total)",
      "yield p, m.EOS_KEY, m.encode_eos(ts_iso, total, total)"),
    M(PKG / "producer.py",
      "return [s for i, s in enumerate(snaps) if i not in held] + [s for i, s in enumerate(snaps) if i in held]",
      "return snaps"),
    # --- batch loads: atomic month replace, quarantine, reconciliation -----------------------------------------
    M(PKG / "batch_ingest.py",  # the old failure mode: empty the month first, then write
      '    lake.write_delta(f"bronze/{ds.name}", _reader(con, f"SELECT {typed}, \'{year_month}\' AS month FROM src "\n',
      '    lake.delta(f"bronze/{ds.name}") and lake.delta(f"bronze/{ds.name}").delete(pred)\n'
      '    lake.write_delta(f"bronze/{ds.name}", _reader(con, f"SELECT {typed}, \'{year_month}\' AS month FROM src "\n'),
    M(PKG / "batch_ingest.py", "    if t.num_rows == 0 and lake.delta(rel) is None:\n", "    if t.num_rows == 0:\n",
      tag="review J"),
    M(PKG / "profiling.py",
      '"match": b[0] + q[0] == src["rows"] and b[0] == x["rows_valid"] and lake_sums == src.get("sums", {})})',
      '"match": True})', tag="review E"),
    M(PKG / "profiling.py",  # the old design: compare the lake with the load's own numbers only
      '"match": b[0] + q[0] == src["rows"] and b[0] == x["rows_valid"] and lake_sums == src.get("sums", {})})',
      '"match": b[0] == x["rows_valid"]})'),
    # --- quality checks ----------------------------------------------------------------------------------------
    M(PKG / "quality.py", 'for ts, prev, s in shares if s < cfg["min_changed_share"]]', "for ts, prev, s in shares if s < 0]"),
    M(PKG / "quality.py", 'for t, p, g in rows if g > cfg["max_gap_minutes"]]',
      'for t, p, g in rows if g > cfg["max_gap_minutes"] * 100]'),
    M(PKG / "quality.py", 'for t, p, g in rows if g < cfg["min_gap_minutes"]]',
      'for t, p, g in rows if g < cfg["min_gap_minutes"] - 1]'),
    M(PKG / "quality.py", 'for ts, r in ratios if r < cfg["min_ratio"]]', "for ts, r in ratios if r < 0.5]"),
    M(PKG / "quality.py", 'bool(out) and worst <= cfg["max_relative_diff"], worst,', "bool(out), worst,",
      tag="review F"),
    M(PKG / "quality.py", 'len(ids) <= cfg["max_count"], len(ids), cfg["max_count"],',
      'True, len(ids), cfg["max_count"],', tag="review G"),
    M(PKG / "quality.py", 'share <= cfg["max_share_of_tap_ins"], round(share, 6),', "True, round(share, 6),",
      tag="review H"),
    M(PKG / "quality.py", 'share <= cfg["max_share"], round(share, 8), cfg["max_share"],',
      'True, round(share, 8), cfg["max_share"],', tag="review I"),
    M(PKG / "quality.py", 'share <= cfg["max_share"], round(share, 6), cfg["max_share"],',
      'share < cfg["max_share"], round(share, 6), cfg["max_share"],'),
    # --- models (SQL) ------------------------------------------------------------------------------------------
    M(MODELS / "gold" / "gold_congestion_episodes.sql", "having count(*) >= {{ var('congestion_min_snapshots') }}",
      "having count(*) > {{ var('congestion_min_snapshots') }}"),
    M(MODELS / "gold" / "gold_congestion_episodes.sql", "group by link_id, sampling_run, island",
      "group by link_id, island"),
    M(MODELS / "silver" / "dim_snapshot.sql",
      "coalesce({{ minutes_between('prev_snapshot_ts', 'snapshot_ts') }} > {{ var('max_gap_minutes') }}, true)",
      "coalesce({{ minutes_between('prev_snapshot_ts', 'snapshot_ts') }} > 100000, true)"),
    M(MODELS / "gold" / "gold_congestion_episodes.sql",
      "      and l.road_category in ({{ var('congestion_road_categories') | join(', ') }})\n", ""),
    M(MODELS / "silver" / "stg_speedband_obs.sql", "case when speed_band between 1 and 8 then speed_band end as speed_band",
      "speed_band"),
    M(MODELS / "silver" / "stg_speedband_obs.sql", "case when max_speed = 999 then null else max_speed end",
      "max_speed"),
    M(MODELS / "silver" / "stg_speedband_obs.sql", "where rn = 1\n", "where true\n", tag="review D"),
    M(MODELS / "silver" / "stg_speedband_obs.sql", "order by _ingested_at, _source) as rn",
      "order by _ingested_at desc, _source) as rn"),
    M(MODELS / "silver" / "dim_calendar.sql", "or h.holiday_date is not null", ""),
    M(MODELS / "silver" / "dim_calendar.sql",  # the old hard-coded 2026 calendar
      "select make_date(year(min(lo)), 1, 1) as lo, make_date(year(max(hi)), 12, 31) as hi",
      "select date '2026-01-01' as lo, date '2026-12-31' as hi"),
    M(MODELS / "gold" / "gold_link_speed_hourly.sql", "where o.speed_band is not null\n", ""),
    M(MODELS / "gold" / "gold_top_od_flows.sql", "order by trips desc, origin_stop_code, destination_stop_code",
      "order by trips desc, origin_stop_code desc, destination_stop_code"),
    M(MODELS / "gold" / "gold_link_hour_features.sql",
      "case when f.prev_hour_ts = f.obs_hour_ts - interval 1 hour then f.prev_mean_band end", "f.prev_mean_band"),
    M(MODELS / "gold" / "gold_link_hour_features.sql", "(s2 - s1 * s1 / n_obs) / (n_obs - 1)",
      "(s2 - s1 * s1 / n_obs) / n_obs"),
]

# Planted bugs in the PySpark layer (src/lakehouse/spark_jobs.py, spark_stream.py). Run with --spark; results go to
# results/mutation_spark.json. Each one changes what Spark computes; tests/test_spark.py must notice.
SPARK_MUTANTS = [
    # silver: dedup order, band rules
    M(PKG / "spark_jobs.py", 'F.col("_ingested_at").asc_nulls_last()', 'F.col("_ingested_at").desc_nulls_last()'),
    M(PKG / "spark_jobs.py", 'valid = F.col("speed_band").between(1, 8)', 'valid = F.col("speed_band").between(0, 8)'),
    M(PKG / "spark_jobs.py", 'F.when(F.col("max_speed") == 999, F.lit(None))', 'F.when(F.col("max_speed") == 998, F.lit(None))'),
    M(PKG / "spark_jobs.py", '        src = src.where(F.col("snapshot_ts") <= F.lit(', '        src = src.where(F.col("snapshot_ts") < F.lit('),
    # dim_snapshot: sampling runs, day type
    M(PKG / "spark_jobs.py", 'F.coalesce(F.col("gap_minutes") > F.lit(max_gap_minutes), F.lit(True))',
      'F.coalesce(F.col("gap_minutes") > F.lit(max_gap_minutes), F.lit(False))'),
    M(PKG / "spark_jobs.py", "weekend = F.dayofweek(obs_date).isin(1, 7)", "weekend = F.dayofweek(obs_date).isin(6, 7)"),
    M(PKG / "spark_jobs.py", 'F.when(weekend | F.array_contains(hol, obs_date), "WEEKENDS/HOLIDAY")',
      'F.when(weekend, "WEEKENDS/HOLIDAY")'),
    # gold
    M(PKG / "spark_jobs.py", 'F.countDistinct("obs_date").alias("n_days")', 'F.count("obs_date").alias("n_days")'),
    M(PKG / "spark_jobs.py", 'o = stg.where(F.col("speed_band").isNotNull())', "o = stg"),
    M(PKG / "spark_jobs.py", 'F.when(F.col("speed_band") <= congestion_band_max, 1)',
      'F.when(F.col("speed_band") < congestion_band_max, 1)'),
    M(PKG / "spark_jobs.py", 'islands.groupBy("link_id", "sampling_run", "island")', 'islands.groupBy("link_id", "island")'),
    M(PKG / "spark_jobs.py", '.where(F.col("road_category").isin(*road_categories)))', ")"),
    M(PKG / "spark_jobs.py", """F.date_format("start_ts", "yyyyMMdd'T'HHmm")""", """F.date_format("start_ts", "yyyyMMddHHmm")"""),
    # streaming sink: dedup, idempotent MERGE, validation
    M(PKG / "spark_stream.py", 'Window.partitionBy("snapshot_ts", "link_id").orderBy("partition", "offset")',
      'Window.partitionBy("snapshot_ts", "link_id").orderBy(F.col("offset").desc())'),
    M(PKG / "spark_stream.py", '        t.alias("t").merge(src.alias("s"), on).whenNotMatchedInsertAll().execute()\n',
      '        src.write.format("delta").mode("append").save(path)\n'),
    M(PKG / "spark_stream.py", '.when(F.pmod(link, F.lit(partitions)) != F.col("partition"), "wrong_partition")', ""),
    M(PKG / "spark_stream.py", "TS_FORMAT = \"yyyy-MM-dd'T'HH:mm:ss\"", "TS_FORMAT = \"yyyy-MM-dd'T'HH:mm:ss[XXX]\""),
    M(PKG / "spark_stream.py", '.when(F.col("k").isNull() | (F.col("k") != link.cast("string")), "key_does_not_match_link")',
      '.when(F.col("k").isNull(), "key_does_not_match_link")'),
]

INVALID_SQL = re.compile(r"Parser Error|Binder Error|Catalog Error|Compilation Error|syntax error", re.I)


def pytest(suffix: str):
    """Python mutants run unit tests first; SQL mutants run the cutoff and full end-to-end dbt checks."""
    tests = TESTS if suffix == ".py" else [str(REPO / "tests" / f) for f in ("test_cutoff.py", "test_e2e.py")]
    return subprocess.run([sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *tests],
                          capture_output=True, text=True, cwd=REPO)


def spark_main():
    """Baseline, then every Spark mutant against tests/test_spark.py (which also runs the dbt fixture pipeline)."""
    import json
    test = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", str(REPO / "tests" / "test_spark.py")]
    base = subprocess.run(test, capture_output=True, text=True, cwd=REPO)
    assert base.returncode == 0 and "skipped" not in base.stdout.splitlines()[-1], \
        "tests/test_spark.py must pass (not skip) on the unmodified code\n" + base.stdout[-3000:]
    n_passed = int(re.search(r"(\d+) passed", base.stdout).group(1))
    print(f"baseline: {n_passed} Spark tests passed", flush=True)
    escaped, killed_by = [], {}
    for mu in SPARK_MUTANTS:
        f = mu["file"]
        original_bytes = f.read_bytes()
        text = original_bytes.decode("utf-8").replace("\r\n", "\n")
        mutated = text
        for old, new in mu["pairs"]:
            assert text.count(old) == 1, f"mutation target not unique in {f.name}: {old[:70]!r}"
            mutated = mutated.replace(old, new)
        f.write_bytes(mutated.encode("utf-8"))
        label = f"{f.name}: {mu['pairs'][0][0].strip()[:80]}"
        try:
            c = subprocess.run([sys.executable, "-m", "py_compile", str(f)], capture_output=True)
            assert c.returncode == 0, f"mutant does not compile, so it proves nothing: {label}"
            r = subprocess.run(test, capture_output=True, text=True, cwd=REPO)
        finally:
            f.write_bytes(original_bytes)
            assert f.read_bytes() == original_bytes, f"source bytes not restored: {f}"
        failed = re.findall(r"^(?:FAILED|ERROR) \S+::(\S+)", r.stdout, re.M)
        killed = r.returncode != 0
        if killed:
            killed_by[label] = failed[:1]
        else:
            escaped.append(label)
        print(f"{'killed ' if killed else 'ESCAPED'}  {label}  {failed[:1]}", flush=True)
    print(f"{len(SPARK_MUTANTS) - len(escaped)}/{len(SPARK_MUTANTS)} Spark mutants killed")
    out = {"tests_passed": n_passed, "total": len(SPARK_MUTANTS), "killed": len(SPARK_MUTANTS) - len(escaped),
           "escaped": escaped, "first_failing_test": killed_by}
    (REPO / "results" / "mutation_spark.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    sys.exit(1 if escaped else 0)


if __name__ == "__main__":
    if sys.argv[1:] == ["--spark"]:
        spark_main()
    only = sys.argv[1:]  # optional: run only mutants whose tag or first target contains one of these strings
    # Baseline: the whole suite, including the slow reproducibility test, must pass on the unmodified code.
    base = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"], capture_output=True,
                          text=True, cwd=REPO)
    assert base.returncode == 0, "tests fail on the unmodified code; fix them before running the mutation check\n" + \
        base.stdout[-3000:]
    n_passed = int(re.search(r"(\d+) passed", base.stdout).group(1))
    print(f"baseline: {n_passed} tests passed", flush=True)

    selected = [mu for mu in MUTANTS if not only or any(o in mu["tag"] or o in mu["pairs"][0][0] for o in only)]
    escaped = []
    for mu in selected:
        f = mu["file"]
        original_bytes = f.read_bytes()
        text = original_bytes.decode("utf-8").replace("\r\n", "\n")
        mutated = text
        for old, new in mu["pairs"]:
            assert text.count(old) == 1, f"mutation target not unique in {f.name}: {old[:70]!r}"
            mutated = mutated.replace(old, new)
        f.write_bytes(mutated.encode("utf-8"))
        label = f"{f.name}: {mu['pairs'][0][0].strip()[:70]}" + (f"  [{mu['tag']}]" if mu["tag"] else "")
        try:
            if f.suffix == ".py":
                c = subprocess.run([sys.executable, "-m", "py_compile", str(f)], capture_output=True)
                assert c.returncode == 0, f"mutant does not compile, so it proves nothing: {label}"
            r = pytest(f.suffix)
            if f.suffix == ".sql":
                assert not INVALID_SQL.search(r.stdout), f"mutant is not valid SQL, so it proves nothing: {label}"
        finally:
            f.write_bytes(original_bytes)
            assert f.read_bytes() == original_bytes, f"source bytes not restored: {f}"
        killed = r.returncode != 0
        if not killed:
            escaped.append(label)
        print(f"{'killed ' if killed else 'ESCAPED'}  {label}", flush=True)

    print(f"{len(selected) - len(escaped)}/{len(selected)} mutants killed")
    if only:
        sys.exit(1 if escaped else 0)

    # Record the outcome for results/SUMMARY.md: test counts and mutants killed.
    sys.path.insert(0, str(REPO / "src"))
    import json  # noqa: E402
    import tempfile  # noqa: E402

    from lakehouse import config  # noqa: E402
    from lakehouse.cli import DBT_DIR, dbt_env  # noqa: E402
    with tempfile.TemporaryDirectory() as tmp:
        s = config.load(lake_root=tmp, warehouse=Path(tmp) / "w.duckdb")
        ls = subprocess.run([str(Path(sys.executable).parent / "dbt"), "ls", "--resource-type", "test", "--output",
                             "name", "--project-dir", str(DBT_DIR), "--quiet"], capture_output=True, text=True,
                            env=dbt_env(s), cwd=DBT_DIR).stdout
    n_dbt = len([x for x in ls.splitlines() if x.strip()])
    out = {"tests_passed": n_passed, "dbt_tests": n_dbt, "total": len(MUTANTS), "killed": len(MUTANTS) - len(escaped),
           "python": sum(mu["file"].suffix == ".py" for mu in MUTANTS),
           "sql": sum(mu["file"].suffix == ".sql" for mu in MUTANTS),
           "reviewer": sum(mu["tag"].startswith("review") for mu in MUTANTS),
           "reviewer_killed": sum(mu["tag"].startswith("review") for mu in MUTANTS) - sum("[review" in e for e in escaped),
           "escaped": escaped}
    (REPO / "results" / "mutation.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out))
    sys.exit(1 if escaped else 0)
