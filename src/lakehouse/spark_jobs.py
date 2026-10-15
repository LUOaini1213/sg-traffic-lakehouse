"""The speed-band silver and gold models re-implemented in PySpark, reading and writing Delta Lake (delta-spark).

This is a second engine for part of the dbt project, not a replacement for it. It reads the same bronze Delta table
the dbt models read and builds four of them:

    silver  stg_speedband_obs   dedup by (snapshot_ts, link_id), band 0 and band 8's 999 bound set to NULL
    silver  dim_snapshot        snapshot sequence, sampling runs split at gaps, day type
    gold    gold_link_speed_hourly     link x day type x hour profile
    gold    gold_congestion_episodes   gaps-and-islands runs of congested snapshots

Outputs are Delta tables under <LAKE_ROOT>/spark/<layer>/<model>. `reconcile` compares them with the tables dbt
built in the DuckDB warehouse: row counts, rows present on one side only (EXCEPT ALL in both directions) and a
digest per partition. Both comparisons run in DuckDB over both outputs, so they test the Spark logic, not two
hash implementations.

The dbt variables (congestion thresholds, the sampling-gap limit) are read from dbt/dbt_project.yml and the public
holidays from the dbt seed, so the two engines cannot drift apart on configuration.

Spark runs in local mode (local[*]) on one machine; nothing here was run on a cluster.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import time
from pathlib import Path

import yaml

from .config import REPO

DBT_DIR = REPO / "dbt"
DELTA_PACKAGE_NOTE = "delta-spark (pip) adds io.delta:delta-spark to spark.jars.packages"
MODELS = ["stg_speedband_obs", "dim_snapshot", "gold_link_speed_hourly", "gold_congestion_episodes"]
LAYER = {"stg_speedband_obs": "silver", "dim_snapshot": "silver",
         "gold_link_speed_hourly": "gold", "gold_congestion_episodes": "gold"}
# Columns compared in `reconcile`, and the column that splits each table into partitions for the digest.
COMPARE = {
    "stg_speedband_obs": (["snapshot_ts", "link_id", "speed_band", "is_band_unknown", "speed_lower_kmh",
                           "speed_upper_kmh", "partition_date", "partition_hour", "ingest_source", "ingested_at_utc",
                           "arrived_late"], "partition_date"),
    "dim_snapshot": (["snapshot_ts", "snapshot_seq", "sampling_run", "prev_snapshot_ts", "gap_minutes", "n_rows",
                      "n_rows_known_band", "obs_date", "obs_hour", "day_type"], "obs_date"),
    "gold_link_speed_hourly": (["link_id", "day_type", "hour_of_day", "n_obs", "n_days", "mean_band", "min_band",
                                "max_band", "share_congested", "mean_speed_lower_kmh"], "hour_of_day"),
    "gold_congestion_episodes": (["episode_id", "link_id", "start_ts", "end_ts", "n_snapshots", "worst_band",
                                  "span_minutes"], "cast(start_ts as date)"),
}


def dbt_vars() -> dict:
    return yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8"))["vars"]


def holidays() -> list[dt.date]:
    with open(DBT_DIR / "seeds" / "public_holidays_sg.csv", encoding="utf-8", newline="") as f:
        return [dt.date.fromisoformat(r["holiday_date"]) for r in csv.DictReader(f)]


def session(app: str = "sgtl", extra_packages: list[str] | None = None, shuffle_partitions: int | None = None):
    """Local SparkSession with Delta Lake. Event times are naive SGT and stay TIMESTAMP_NTZ end to end; the session
    time zone is UTC so that nothing shifts them."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions or os.environ.get("SPARK_SHUFFLE_PARTITIONS", 16)))
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
         .config("spark.ui.enabled", "false")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.databricks.delta.snapshotPartitions", "4"))
    spark = configure_spark_with_delta_pip(b, extra_packages=extra_packages or []).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# -- the models -------------------------------------------------------------------------------------------------------

def stg_speedband_obs(bronze, cutoff: dt.datetime | None = None):
    """dbt/models/silver/stg_speedband_obs.sql. The copy written first wins if a key is stored twice."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    src = bronze
    if cutoff is not None:
        src = src.where(F.col("snapshot_ts") <= F.lit(cutoff.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz"))
    w = Window.partitionBy("snapshot_ts", "link_id").orderBy(F.col("_ingested_at").asc_nulls_last(),
                                                             F.col("_source").asc_nulls_last())
    ranked = src.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1)
    valid = F.col("speed_band").between(1, 8)
    return ranked.select(
        "snapshot_ts",
        "link_id",
        F.when(valid, F.col("speed_band")).alias("speed_band"),
        F.coalesce(~valid, F.lit(True)).alias("is_band_unknown"),
        F.col("min_speed").alias("speed_lower_kmh"),
        F.when(F.col("max_speed") == 999, F.lit(None)).otherwise(F.col("max_speed")).alias("speed_upper_kmh"),
        F.col("obs_date").alias("partition_date"),
        F.col("obs_hour").alias("partition_hour"),
        F.col("_source").alias("ingest_source"),
        F.col("_ingested_at").alias("ingested_at_utc"),
        F.col("_late").alias("arrived_late"),
    )


def _epoch_s(col):
    from pyspark.sql import functions as F
    # NTZ -> TIMESTAMP in a UTC session keeps the wall-clock value; only differences are used
    return F.unix_micros(col.cast("timestamp")) / F.lit(1_000_000.0)


def dim_snapshot(stg, holiday_dates: list[dt.date], max_gap_minutes: float):
    """dbt/models/silver/dim_snapshot.sql (day type as in dim_calendar: ISO weekend or public holiday)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    snaps = stg.groupBy("snapshot_ts").agg(F.count(F.lit(1)).alias("n_rows"),
                                           F.count("speed_band").alias("n_rows_known_band"))
    w = Window.orderBy("snapshot_ts")
    ordered = snaps.withColumn("prev_snapshot_ts", F.lag("snapshot_ts").over(w))
    gap = (_epoch_s(F.col("snapshot_ts")) - _epoch_s(F.col("prev_snapshot_ts"))) / F.lit(60.0)
    gaps = (ordered.withColumn("gap_minutes", gap)
            .withColumn("starts_sampling_run", F.coalesce(F.col("gap_minutes") > F.lit(max_gap_minutes), F.lit(True))))
    run_w = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    obs_date = F.to_date("snapshot_ts")
    hol = F.array(*[F.lit(d) for d in holiday_dates]) if holiday_dates else F.array().cast("array<date>")
    # Spark dayofweek: Sunday = 1 ... Saturday = 7 (DuckDB isodow >= 6 is Saturday or Sunday)
    weekend = F.dayofweek(obs_date).isin(1, 7)
    return gaps.select(
        "snapshot_ts",
        F.row_number().over(w).alias("snapshot_seq"),
        F.sum(F.when(F.col("starts_sampling_run"), 1).otherwise(0)).over(run_w).alias("sampling_run"),
        "prev_snapshot_ts",
        F.round("gap_minutes", 2).alias("gap_minutes"),
        "n_rows",
        "n_rows_known_band",
        obs_date.alias("obs_date"),
        F.hour("snapshot_ts").alias("obs_hour"),
        F.when(weekend | F.array_contains(hol, obs_date), "WEEKENDS/HOLIDAY").otherwise("WEEKDAY").alias("day_type"),
    )


def gold_link_speed_hourly(stg, snap, congestion_band_max: int):
    """dbt/models/gold/gold_link_speed_hourly.sql. Averages from integer sums, as in dbt."""
    from pyspark.sql import functions as F
    o = stg.where(F.col("speed_band").isNotNull())
    j = o.join(snap.select("snapshot_ts", "day_type", "obs_hour", "obs_date"), "snapshot_ts")
    n = F.count(F.lit(1))
    return (j.groupBy("link_id", "day_type", F.col("obs_hour").alias("hour_of_day"))
            .agg(n.alias("n_obs"),
                 F.countDistinct("obs_date").alias("n_days"),
                 F.round(F.sum("speed_band") / n, 4).alias("mean_band"),
                 F.min("speed_band").alias("min_band"),
                 F.max("speed_band").alias("max_band"),
                 F.round(F.sum(F.when(F.col("speed_band") <= congestion_band_max, 1).otherwise(0)) / n, 4)
                 .alias("share_congested"),
                 F.round(F.sum("speed_lower_kmh") / n, 2).alias("mean_speed_lower_kmh")))


def gold_congestion_episodes(stg, snap, links, congestion_band_max: int, min_snapshots: int,
                             road_categories: list[int]):
    """dbt/models/gold/gold_congestion_episodes.sql: consecutive in the global snapshot sequence and in one
    sampling run; links missing from the link metadata are dropped (inner join, as in dbt)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    obs = (stg.where(F.col("speed_band") <= congestion_band_max)
           .join(snap.select("snapshot_ts", "snapshot_seq", "sampling_run"), "snapshot_ts")
           .join(links.select(F.col("LinkID").alias("link_id"), F.col("RoadCategory").alias("road_category")),
                 "link_id")
           .where(F.col("road_category").isin(*road_categories)))
    w = Window.partitionBy("link_id").orderBy("snapshot_seq")
    islands = obs.withColumn("island", F.col("snapshot_seq") - F.row_number().over(w))
    g = (islands.groupBy("link_id", "sampling_run", "island")
         .agg(F.min("snapshot_ts").alias("start_ts"), F.max("snapshot_ts").alias("end_ts"),
              F.count(F.lit(1)).alias("n_snapshots"), F.min("speed_band").alias("worst_band"))
         .where(F.col("n_snapshots") >= min_snapshots))
    span = (_epoch_s(F.col("end_ts")) - _epoch_s(F.col("start_ts"))) / F.lit(60.0)
    return g.select(
        F.md5(F.concat(F.col("link_id").cast("string"), F.lit("|"),
                       F.date_format("start_ts", "yyyyMMdd'T'HHmm"))).alias("episode_id"),
        "link_id", "start_ts", "end_ts", "n_snapshots", "worst_band",
        F.round(span, 1).alias("span_minutes"),
    )


# -- running and comparing ----------------------------------------------------------------------------------------------

def spark_path(settings, model: str) -> str:
    return f"{settings.lake_root}/spark/{LAYER[model]}/{model}"


def build(settings, spark, cutoff: dt.datetime | None = None) -> dict:
    """Build the four models; each is written as one Delta overwrite and read back by the models downstream of it,
    as dbt materializes each model as a table. Returns rows and seconds per model."""
    if settings.is_s3:
        raise SystemExit("spark-batch reads a local-directory lake (no S3 connector is configured for Spark)")
    v = dbt_vars()
    lake = settings.lake_root
    bronze = spark.read.format("delta").load(f"{lake}/bronze/speedbands")
    links = spark.read.format("delta").load(f"{lake}/bronze/speedband_links")
    out, timings = {}, {}

    def materialize(model, df, partition_by=None):
        t0 = time.perf_counter()
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            w = w.partitionBy(partition_by)
        w.save(spark_path(settings, model))
        timings[model] = round(time.perf_counter() - t0, 2)
        back = spark.read.format("delta").load(spark_path(settings, model))
        out[model] = back.count()
        return back

    stg = materialize("stg_speedband_obs", stg_speedband_obs(bronze, cutoff), "partition_date")
    snap = materialize("dim_snapshot", dim_snapshot(stg, holidays(), float(v["max_gap_minutes"])))
    materialize("gold_link_speed_hourly", gold_link_speed_hourly(stg, snap, int(v["congestion_band_max"])))
    materialize("gold_congestion_episodes",
                gold_congestion_episodes(stg, snap, links, int(v["congestion_band_max"]),
                                         int(v["congestion_min_snapshots"]),
                                         [int(c) for c in v["congestion_road_categories"]]))
    return {"rows": out, "seconds": timings, "total_seconds": round(sum(timings.values()), 2)}


def _row_text(cols: list[str]) -> str:
    return "concat_ws('|', " + ", ".join(f"coalesce(cast({c} as varchar), '<null>')" for c in cols) + ")"


def reconcile(settings, models: list[str] | None = None) -> dict:
    """Compare every Spark output with the dbt table of the same name in the DuckDB warehouse."""
    from .storage import duckdb_connect
    con = duckdb_connect(settings, settings.warehouse, read_only=True)
    res = {}
    try:
        for model in models or MODELS:
            cols, part = COMPARE[model]
            dbt_t = f"{LAYER[model]}.{model}"
            spk_t = f"delta_scan('{spark_path(settings, model)}')"
            sel = ", ".join(cols)
            n_dbt = con.execute(f"select count(*) from {dbt_t}").fetchone()[0]
            n_spk = con.execute(f"select count(*) from {spk_t}").fetchone()[0]
            only_dbt = con.execute(f"select count(*) from (select {sel} from {dbt_t} except all "
                                   f"select {sel} from {spk_t})").fetchone()[0]
            only_spk = con.execute(f"select count(*) from (select {sel} from {spk_t} except all "
                                   f"select {sel} from {dbt_t})").fetchone()[0]
            digest = (f"select {part} as p, count(*) as n, sum(hash({_row_text(cols)})::hugeint)::varchar as h "
                      f"from {{t}} group by 1")
            d_dbt = {str(r[0]): (r[1], r[2]) for r in con.execute(digest.format(t=dbt_t)).fetchall()}
            d_spk = {str(r[0]): (r[1], r[2]) for r in con.execute(digest.format(t=spk_t)).fetchall()}
            parts = sorted(set(d_dbt) | set(d_spk))
            same = [p for p in parts if d_dbt.get(p) == d_spk.get(p)]
            res[model] = {"rows_dbt": n_dbt, "rows_spark": n_spk, "only_in_dbt": only_dbt, "only_in_spark": only_spk,
                          "partition_column": part, "partitions": len(parts), "partitions_same_digest": len(same),
                          "partitions_differ": [p for p in parts if p not in same][:20],
                          "identical": n_dbt == n_spk and only_dbt == 0 and only_spk == 0 and len(same) == len(parts)}
    finally:
        con.close()
    res["all_identical"] = all(r["identical"] for r in res.values())
    return res


def bench(settings, cutoff: dt.datetime | None, repeats: int = 3) -> dict:
    """Wall time of the same four models in dbt-duckdb and in Spark local mode, `repeats` times each.
    dbt runs `dbt run --select <the four models>` against a warehouse where `dbt build` already ran once (dim_calendar
    and the seeds exist), with the repository's DuckDB settings (4 threads, DUCKDB_MEMORY). All dbt runs come first,
    with no JVM alive; then one Spark session builds the models `repeats` times. Session start-up is reported
    separately. A failed dbt run is recorded with its error, not retried."""
    import statistics
    import subprocess
    import sys

    from .cli import dbt_env
    exe = Path(sys.executable).parent / "dbt"
    cmd = [str(exe), "run", "--select", *MODELS, "--project-dir", str(DBT_DIR), "--quiet"]
    runs = {"dbt_duckdb": [], "dbt_duckdb_failed": [], "spark_local": [], "spark_per_model": []}
    for _ in range(repeats):
        t0 = time.perf_counter()
        r = subprocess.run(cmd, env=dbt_env(settings, until=cutoff), cwd=DBT_DIR, capture_output=True, text=True)
        took = round(time.perf_counter() - t0, 2)
        if r.returncode == 0:
            runs["dbt_duckdb"].append(took)
        else:
            err = [x.strip() for x in (r.stdout + r.stderr).splitlines() if "Error" in x]
            runs["dbt_duckdb_failed"].append({"seconds": took, "error": err[-1][:300] if err else "unknown"})
    t0 = time.perf_counter()
    spark = session("sgtl-bench")
    startup = round(time.perf_counter() - t0, 2)
    try:
        for _ in range(repeats):
            t0 = time.perf_counter()
            b = build(settings, spark, cutoff)
            runs["spark_local"].append(round(time.perf_counter() - t0, 2))
            runs["spark_per_model"].append(b["seconds"])
        cores = spark.sparkContext.defaultParallelism
    finally:
        spark.stop()
    return {"repeats": repeats, "spark_session_startup_s": startup, "spark_cores": cores,
            "spark_driver_memory": os.environ.get("SPARK_DRIVER_MEMORY", "3g"),
            "dbt_threads": 4, "duckdb_memory_limit": settings.duckdb_memory, "rows": b["rows"], "seconds": runs,
            "median_s": {k: statistics.median(v) for k, v in runs.items() if k in ("dbt_duckdb", "spark_local") and v}}


def write_json(settings, name: str, obj) -> Path:
    out = Path(settings.results_dir)
    out.mkdir(parents=True, exist_ok=True)
    p = out / name
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    return p
