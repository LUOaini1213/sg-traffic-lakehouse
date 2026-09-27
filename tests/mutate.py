"""Mutation check: each deliberate bug below must make at least one test fail.

Pattern: first the unmodified code must pass (otherwise every mutant would look "killed"); then, for each mutant,
the target text must occur exactly once, the mutated file must still be valid (Python: py_compile; SQL: the test
run must not stop on a parser/binder/compilation error), and the test run must fail. Every mutant must be killed.

Run: python tests/mutate.py            (about 10-15 minutes: SQL mutants rebuild the fixture with dbt)
"""
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PKG = REPO / "src" / "lakehouse"
MODELS = REPO / "dbt" / "models"
# unit tests first, so a Python mutant usually dies before the slower end-to-end fixture run
TESTS = [str(REPO / "tests" / f) for f in
         ("test_speedbands.py", "test_stream.py", "test_batch.py", "test_e2e.py")]

MUTANTS = [
    # --- parsing -----------------------------------------------------------------------------------------------
    (PKG / "speedbands.py", r'SNAPSHOT_RE = re.compile(r"^sb_(\d{8})_(\d{4})\.csv\.gz$")',
     r'SNAPSHOT_RE = re.compile(r"sb_(\d{8})_(\d{4})")'),
    (PKG / "speedbands.py", "    if header != SOURCE_HEADER:\n", "    if False:\n"),
    (PKG / "speedbands.py", "        if now - p.stat().st_mtime < min_age_s:\n", "        if False:\n"),
    (PKG / "speedbands.py", "        if not is_complete_gzip(p):\n", "        if False:\n"),
    (PKG / "batch_ingest.py", "    if header != list(ds.columns):\n", "    if False:\n"),
    (PKG / "batch_ingest.py", '"TRY_CAST(TIME_PER_HOUR AS TINYINT) NOT BETWEEN 0 AND 23"',
     '"TRY_CAST(TIME_PER_HOUR AS TINYINT) NOT BETWEEN 0 AND 24"'),
    (PKG / "batch_ingest.py", "'[0-9]{{5}}'", "'[0-9]{{4,5}}'"),
    # --- partitioning ------------------------------------------------------------------------------------------
    (PKG / "speedbands.py", 'f"bronze/speedbands/obs_date={ts:%Y-%m-%d}/obs_hour={ts:%H}/"',
     'f"bronze/speedbands/obs_date={dt.datetime.now():%Y-%m-%d}/obs_hour={ts:%H}/"'),
    (PKG / "messages.py", "    return int(link_id) % partitions", "    return 0"),
    # --- dedupe / idempotency ----------------------------------------------------------------------------------
    (PKG / "speedbands.py", '.aggregate([("_i", "min")])', '.aggregate([("_i", "max")])'),
    (PKG / "speedbands.py", 'KEY = ["snapshot_ts", "link_id"]', 'KEY = ["link_id"]'),
    (PKG / "speedbands.py", "merged = dedupe(pa.concat_tables([existing, incoming]))",
     "merged = dedupe(pa.concat_tables([incoming, existing]))"),
    (PKG / "speedbands.py",
     "existing = self.lake.read_parquet(rel).cast(BRONZE_SCHEMA) if self.lake.exists(rel) else BRONZE_SCHEMA.empty_table()",
     "existing = BRONZE_SCHEMA.empty_table()"),
    (PKG / "consumer.py", "        if link_id in p.rows:\n", "        if False:\n"),
    # --- streaming: completeness, lateness, ordering -----------------------------------------------------------
    (PKG / "consumer.py", "            if p.expected is not None:\n", "            if p.expected is not None or p.rows:\n"),
    (PKG / "consumer.py", "complete = p.expected is not None and len(links) >= p.expected",
     "complete = p.expected is not None"),
    (PKG / "speedbands.py", "        if rows.num_rows and set(split_by_partition(rows, partitions)) != {part}:\n",
     "        if False:\n"),
    (PKG / "consumer.py", "late=wm is not None and ts < wm,", "late=False,"),
    (PKG / "consumer.py", "            elif now - p.last_update >= self.flush_timeout_s:\n", "            elif False:\n"),
    (PKG / "producer.py", "yield p, m.EOS_KEY, m.encode_eos(ts_iso, len(seen[p]), total)",
     "yield p, m.EOS_KEY, m.encode_eos(ts_iso, total, total)"),
    (PKG / "producer.py",
     "return [s for i, s in enumerate(snaps) if i not in held] + [s for i, s in enumerate(snaps) if i in held]",
     "return snaps"),
    # --- quality checks ----------------------------------------------------------------------------------------
    (PKG / "quality.py", 'for ts, prev, s in shares if s < cfg["min_changed_share"]]', "for ts, prev, s in shares if s < 0]"),
    (PKG / "quality.py", 'for t, p, g in rows if g > cfg["max_gap_minutes"]]',
     'for t, p, g in rows if g > cfg["max_gap_minutes"] * 100]'),
    (PKG / "quality.py", 'for ts, r in ratios if r < cfg["min_ratio"]]', "for ts, r in ratios if r < 0.5]"),
    # --- models (SQL) ------------------------------------------------------------------------------------------
    (MODELS / "gold" / "gold_congestion_episodes.sql", "having count(*) >= {{ var('congestion_min_snapshots') }}",
     "having count(*) > {{ var('congestion_min_snapshots') }}"),
    (MODELS / "gold" / "gold_congestion_episodes.sql", "group by link_id, sampling_run, island",
     "group by link_id, island"),
    (MODELS / "silver" / "dim_snapshot.sql",
     "coalesce({{ minutes_between('prev_snapshot_ts', 'snapshot_ts') }} > {{ var('max_gap_minutes') }}, true)",
     "coalesce({{ minutes_between('prev_snapshot_ts', 'snapshot_ts') }} > 100000, true)"),
    (MODELS / "gold" / "gold_congestion_episodes.sql",
     "      and l.road_category in ({{ var('congestion_road_categories') | join(', ') }})\n", ""),
    (MODELS / "silver" / "stg_speedband_obs.sql", "case when speed_band between 1 and 8 then speed_band end as speed_band",
     "speed_band"),
    (MODELS / "silver" / "stg_speedband_obs.sql", "case when max_speed = 999 then null else max_speed end",
     "max_speed"),
    (MODELS / "silver" / "dim_calendar.sql", "or h.holiday_date is not null", ""),
    (MODELS / "gold" / "gold_link_speed_hourly.sql", "where o.speed_band is not null\n", ""),
    (MODELS / "gold" / "gold_top_od_flows.sql", "order by trips desc, origin_stop_code, destination_stop_code",
     "order by trips desc, origin_stop_code desc, destination_stop_code"),
    (MODELS / "gold" / "gold_link_hour_features.sql",
     "case when f.prev_hour_ts = f.obs_hour_ts - interval 1 hour then f.prev_mean_band end", "f.prev_mean_band"),
    (MODELS / "gold" / "gold_link_hour_features.sql", "(s2 - s1 * s1 / n_obs) / (n_obs - 1)",
     "(s2 - s1 * s1 / n_obs) / n_obs"),
]

INVALID_SQL = re.compile(r"Parser Error|Binder Error|Catalog Error|Compilation Error|syntax error", re.I)


def pytest(*extra):
    return subprocess.run([sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *extra, *TESTS],
                          capture_output=True, text=True, cwd=REPO)


# Baseline: the whole suite, including the slow reproducibility test, must pass on the unmodified code.
base = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"], capture_output=True,
                      text=True, cwd=REPO)
assert base.returncode == 0, "tests fail on the unmodified code; fix them before running the mutation check\n" + \
    base.stdout[-3000:]
n_passed = int(re.search(r"(\d+) passed", base.stdout).group(1))

escaped = 0
for f, old, new in MUTANTS:
    text = f.read_text(encoding="utf-8")
    assert text.count(old) == 1, f"mutation target not unique in {f.name}: {old[:70]}"
    f.write_text(text.replace(old, new), encoding="utf-8")
    try:
        if f.suffix == ".py":
            c = subprocess.run([sys.executable, "-m", "py_compile", str(f)], capture_output=True)
            assert c.returncode == 0, f"mutant does not compile, so it proves nothing: {old[:70]}"
        r = pytest()
        if f.suffix == ".sql":
            assert not INVALID_SQL.search(r.stdout), f"mutant is not valid SQL, so it proves nothing: {old[:70]}"
    finally:
        f.write_text(text, encoding="utf-8")
    killed = r.returncode != 0
    escaped += not killed
    print(f"{'killed ' if killed else 'ESCAPED'}  {f.name}: {old.strip()[:70]}", flush=True)

print(f"{len(MUTANTS) - escaped}/{len(MUTANTS)} mutants killed")

# Record the outcome for results/SUMMARY.md: test counts and mutants killed.
sys.path.insert(0, str(REPO / "src"))
import json  # noqa: E402
import tempfile  # noqa: E402

from lakehouse import config  # noqa: E402
from lakehouse.cli import DBT_DIR, dbt_env  # noqa: E402
with tempfile.TemporaryDirectory() as tmp:
    s = config.load(lake_root=tmp, warehouse=Path(tmp) / "w.duckdb")
    ls = subprocess.run([str(Path(sys.executable).parent / "dbt"), "ls", "--resource-type", "test", "--output", "name",
                         "--project-dir", str(DBT_DIR), "--quiet"], capture_output=True, text=True, env=dbt_env(s),
                        cwd=DBT_DIR).stdout
n_dbt = len([x for x in ls.splitlines() if x.strip()])
out = {"tests_passed": n_passed, "dbt_tests": n_dbt, "total": len(MUTANTS), "killed": len(MUTANTS) - escaped,
       "python": sum(f.suffix == ".py" for f, _, _ in MUTANTS), "sql": sum(f.suffix == ".sql" for f, _, _ in MUTANTS)}
(REPO / "results" / "mutation.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
print(json.dumps(out))
sys.exit(1 if escaped else 0)
