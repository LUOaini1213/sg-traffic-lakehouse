"""PySpark layer: the Spark models must reproduce the dbt tables exactly on the fixture, and the Structured Streaming
sink must be idempotent and route bad messages to dead letter. Skipped when pyspark is not installed (the main CI job);
the `spark` CI job installs requirements-spark.txt and a JDK.
"""
import dataclasses
import datetime as dt
import json
import shutil

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from lakehouse import messages as m  # noqa: E402
from lakehouse import spark_jobs, spark_stream  # noqa: E402

D = dt.datetime


@pytest.fixture(scope="module")
def spark():
    s = spark_jobs.session("sgtl-tests", shuffle_partitions=2)
    yield s
    s.stop()


@pytest.fixture(scope="module")
def built(pipeline_run, spark):
    """Spark models over the fixture bronze that the dbt end-to-end run used (with its planted duplicate key)."""
    s = pipeline_run[0]
    res = spark_jobs.build(s, spark)
    return s, res


def rows(spark, settings, model, cols, where=None, order=None):
    df = spark.read.format("delta").load(spark_jobs.spark_path(settings, model))
    if where:
        df = df.where(where)
    return [tuple(r) for r in df.select(*cols).orderBy(*(order or cols)).collect()]


def test_every_spark_model_matches_dbt_exactly(built):
    s, res = built
    rec = spark_jobs.reconcile(s)
    assert rec["all_identical"], json.dumps(rec, indent=1, default=str)
    assert {k: rec[k]["rows_spark"] for k in spark_jobs.MODELS} == {
        "stg_speedband_obs": 120, "dim_snapshot": 10, "gold_link_speed_hourly": res["rows"]["gold_link_speed_hourly"],
        "gold_congestion_episodes": 3}
    assert all(rec[k]["partitions"] >= 1 for k in spark_jobs.MODELS)


def test_reconcile_detects_a_single_changed_value(built, spark, tmp_path):
    """The comparison itself must be able to fail: change one mean_band in a copy of the Spark output."""
    s, _ = built
    from pyspark.sql import functions as F
    alt = dataclasses.replace(s, lake_root=str(tmp_path / "lake"))
    src = spark.read.format("delta").load(spark_jobs.spark_path(s, "gold_link_speed_hourly"))
    changed = src.withColumn("mean_band", F.when((F.col("link_id") == 101) & (F.col("hour_of_day") == 8),
                                                 F.col("mean_band") + 0.0001).otherwise(F.col("mean_band")))
    changed.write.format("delta").mode("overwrite").save(spark_jobs.spark_path(alt, "gold_link_speed_hourly"))
    alt = dataclasses.replace(alt, warehouse=s.warehouse)
    rec = spark_jobs.reconcile(alt, ["gold_link_speed_hourly"])
    r = rec["gold_link_speed_hourly"]
    assert not rec["all_identical"]
    assert (r["only_in_dbt"], r["only_in_spark"]) == (1, 1)
    assert r["partitions"] - r["partitions_same_digest"] == 1 and r["partitions_differ"] == ["8"]


def test_congestion_episodes_hand_values(built, spark):
    s, _ = built
    got = rows(spark, s, "gold_congestion_episodes",
               ["link_id", "start_ts", "end_ts", "n_snapshots", "worst_band", "span_minutes"], order=["link_id"])
    assert got == [
        (101, D(2026, 9, 5, 8, 0), D(2026, 9, 5, 9, 0), 4, 1, 60.0),
        (105, D(2026, 9, 5, 11, 0), D(2026, 9, 5, 12, 0), 4, 1, 60.0),
        (111, D(2026, 9, 5, 8, 20), D(2026, 9, 5, 9, 0), 3, 2, 40.0),
    ]


def test_snapshot_dimension_hand_values(built, spark):
    s, _ = built
    got = rows(spark, s, "dim_snapshot", ["snapshot_seq", "sampling_run", "gap_minutes", "day_type"])
    assert [r[1] for r in got] == [1, 1, 1, 1, 1, 2, 2, 2, 2, 3]
    assert got[5][2] == 100.0 and got[9][2] == 2635.0
    assert got[9][3] == "WEEKDAY" and got[0][3] == "WEEKENDS/HOLIDAY"


def test_planted_duplicate_first_copy_wins_and_band_rules(built, spark):
    from conftest import PLANTED
    s, _ = built
    assert rows(spark, s, "stg_speedband_obs", ["speed_band", "ingest_source"],
                where=f"link_id = {PLANTED['link_id']} and snapshot_ts = '{PLANTED['snapshot_ts']}'") == [(2, "file")]
    assert rows(spark, s, "stg_speedband_obs", ["speed_band", "is_band_unknown", "speed_lower_kmh"],
                where="link_id = 110 and snapshot_ts = '2026-09-05 08:00:00'") == [(None, True, None)]
    assert rows(spark, s, "stg_speedband_obs", ["speed_upper_kmh"],
                where="link_id = 107 and snapshot_ts = '2026-09-07 07:55:00'") == [(None,)]


def test_link_speed_hourly_hand_values(built, spark):
    s, _ = built
    got = rows(spark, s, "gold_link_speed_hourly",
               ["day_type", "hour_of_day", "n_obs", "mean_band", "min_band", "max_band", "share_congested",
                "mean_speed_lower_kmh"], where="link_id = 101", order=["day_type", "hour_of_day"])
    assert got == [
        ("WEEKDAY", 7, 1, 5.0, 5, 5, 0.0, 40.0),
        ("WEEKENDS/HOLIDAY", 8, 3, 1.6667, 1, 2, 1.0, 6.67),
        ("WEEKENDS/HOLIDAY", 9, 2, 3.5, 2, 5, 0.5, 25.0),
        ("WEEKENDS/HOLIDAY", 11, 3, 6.0, 6, 6, 0.0, 50.0),
        ("WEEKENDS/HOLIDAY", 12, 1, 6.0, 6, 6, 0.0, 50.0),
    ]


def test_cutoff_filters_bronze_before_silver(pipeline_run, spark, tmp_path):
    """Same expectations as tests/test_cutoff.py for dbt: at 08:20, 24 observations in two snapshots."""
    s = dataclasses.replace(pipeline_run[0])
    alt = dataclasses.replace(s, lake_root=str(tmp_path / "lake"))
    shutil.copytree(f"{s.lake_root}/bronze", f"{alt.lake_root}/bronze")
    res = spark_jobs.build(alt, spark, cutoff=D(2026, 9, 5, 8, 20))
    assert res["rows"]["stg_speedband_obs"] == 24 and res["rows"]["dim_snapshot"] == 2
    gold = spark.read.format("delta").load(spark_jobs.spark_path(alt, "gold_link_speed_hourly"))
    assert gold.groupBy().sum("n_obs").collect()[0][0] == 23


def test_day_type_uses_the_holiday_seed_and_iso_weekend(spark):
    """2026-08-10 (Monday) is in the seed; the other dates are not. The fixture has only a Saturday and a Monday, so
    it cannot tell Spark's dayofweek (Sunday = 1) from DuckDB's isodow (Sunday = 7): Friday and Sunday are here."""
    hol = spark_jobs.holidays()
    assert dt.date(2026, 8, 10) in hol and not {dt.date(2026, 8, d) for d in (11, 14, 15, 16)} & set(hol)
    days = [10, 11, 14, 15, 16]   # Mon (holiday), Tue, Fri, Sat, Sun
    stg = spark.createDataFrame([(D(2026, 8, d, 8, 0), 1, 3) for d in days],
                                "snapshot_ts timestamp_ntz, link_id bigint, speed_band smallint")
    got = {r["obs_date"].day: r["day_type"] for r in spark_jobs.dim_snapshot(stg, hol, 30.0).collect()}
    W, H = "WEEKDAY", "WEEKENDS/HOLIDAY"
    assert got == {10: H, 11: W, 14: W, 15: H, 16: H}


# -- Structured Streaming sink (no broker: foreachBatch is called with Kafka-shaped batches) ---------------------------

KAFKA_SCHEMA = "key binary, value binary, topic string, partition int, offset bigint, timestamp timestamp"
TS = "2026-09-05T08:00:00"


def kafka_batch(spark, msgs):
    """msgs: (partition, key, value) in send order; offsets are assigned per partition."""
    nxt, out = {}, []
    for i, (p, k, v) in enumerate(msgs):
        off = nxt.get(p, 0)
        nxt[p] = off + 1
        out.append((k, v, "t", p, off, D(2026, 10, 14, 0, 0, 0) + dt.timedelta(milliseconds=i)))
    return spark.createDataFrame(out, KAFKA_SCHEMA)


def rec(link, band, mn=10, mx=19, ts=TS):
    return (link % 3, str(link).encode(), m.encode_record(ts, link, band, mn, mx))


def test_stream_sink_is_idempotent_and_dead_letters_bad_messages(spark, tmp_path):
    lake = str(tmp_path / "lake")
    log = tmp_path / "batches.jsonl"
    sink = spark_stream.BronzeSink(spark, lake, partitions=3, log_path=str(log))
    msgs = [rec(101, 2), rec(102, 5), rec(103, 0), rec(104, 8, 70, 999),
            rec(101, 7),                                             # repeat, later offset: dropped
            (2, b"105", b"{not json"),                                # malformed_json
            (0, b"106", m.encode_record("2026-09-05T08:00:00+08:00", 106, 3, 30, 39)),  # zoned ts: bad_ts
            (1, b"107", m.encode_record(TS, 108, 3, 30, 39)),        # key != link
            (1, b"109", m.encode_record(TS, 109, 3, 30, 39)),        # 109 % 3 = 1 ok
            (2, b"110", m.encode_record(TS, 110, 3, 30, 39)),        # 110 % 3 = 2 ok
            (0, b"111", m.encode_record(TS, 111, 3, 30, 39)),        # 111 % 3 = 0 ok
            (0, b"112", m.encode_record(TS, 112, 3, 30, 39)),        # 112 % 3 = 1 on partition 0: wrong_partition
            (1, b"113", m.encode_record(TS, 113, None, None, None)),  # band null: bad_band
            *[(p, m.EOS_KEY, m.encode_eos(TS, 3, 9)) for p in range(3)]]
    batch = kafka_batch(spark, msgs)
    sink(batch, 0)
    sink(batch, 0)   # Spark replays the last planned batch after a crash
    sink(kafka_batch(spark, [rec(101, 6), rec(110, 4)]), 1)   # redelivered keys with another payload

    bronze = spark.read.format("delta").load(f"{lake}/{spark_stream.TABLE}")
    got = sorted((r["link_id"], r["speed_band"], r["min_speed"], r["max_speed"], r["part"], r["_source"])
                 for r in bronze.collect())
    assert got == [(101, 2, 10, 19, 2, "spark"), (102, 5, 10, 19, 0, "spark"), (103, 0, 10, 19, 1, "spark"),
                   (104, 8, 70, 999, 2, "spark"), (109, 3, 30, 39, 1, "spark"), (110, 3, 30, 39, 2, "spark"),
                   (111, 3, 30, 39, 0, "spark")]
    r0 = bronze.where("link_id = 101").collect()[0]
    assert (r0["snapshot_ts"], r0["obs_date"], r0["obs_hour"]) == (D(2026, 9, 5, 8, 0), dt.date(2026, 9, 5), 8)

    dlq = spark.read.format("delta").load(f"{lake}/{spark_stream.DEAD_LETTER}")
    reasons = sorted((r["key"], r["reason"]) for r in dlq.collect())
    assert reasons == [("105", "malformed_json"), ("106", "bad_ts"), ("107", "key_does_not_match_link"),
                       ("112", "wrong_partition"), ("113", "bad_band")]

    lines = [json.loads(x) for x in log.read_text().splitlines()]
    assert [x["bronze"]["inserted"] for x in lines] == [7, 0, 0]
    assert [x["dead_letter"]["inserted"] for x in lines[:2]] == [5, 0]
    assert sorted(e["partition"] for e in lines[0]["eos"]) == [0, 1, 2]
    assert all(e["ts"] == TS for e in lines[0]["eos"])
