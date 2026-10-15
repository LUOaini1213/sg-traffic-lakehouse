"""Spark Structured Streaming: the speed-band topic (Kafka API, Redpanda locally) -> a Delta bronze table.

A second sink for the same topic and wire format as consumer.py, written with Spark instead of a hand-written
consumer. It writes its own table, bronze/speedbands_spark by default, with the bronze schema of speedbands.py;
it does not share a table with the delta-rs writers.

Delivery semantics
  - Spark tracks Kafka offsets in the checkpoint directory. A micro-batch is planned (its offset range written to
    the checkpoint's offsets log) before it runs and marked done (commits log) after the sink returns. After a crash
    Spark runs the last planned batch again with the same offset range.
  - The sink (`process_batch`, run through foreachBatch) makes that replay harmless: it is one Delta MERGE that
    inserts only (snapshot_ts, link_id) keys the table does not hold and never updates a stored row, the same rule as
    the delta-rs writer. A batch that already committed before the crash inserts nothing the second time.
  - Within a batch, a key that appears more than once keeps the copy with the lowest (partition, offset).
  - Messages that fail validation go to bronze/_dead_letter/speedband_spark, merged on (partition, offset), so a
    replayed batch does not repeat them either.
  So the table ends with every key exactly once (effectively exactly-once), which `spark-stream-bench` checks
  after real process kills.

Not implemented here, unlike consumer.py: snapshot reassembly and the watermark verdict (`_late` is NULL), and the
count of repeated keys whose payload differs (insert-only keeps the first one, but does not count the others).

End-to-end latency is measured as in the Python consumer benchmark: from the producer's timestamp on a partition's
end-of-snapshot (EOS) marker to the end of the Delta commit of the micro-batch that contains it. Kafka keeps order
within a partition, so every record of that (snapshot, partition) slice is committed by then.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from pathlib import Path

from . import messages as m

TABLE = "bronze/speedbands_spark"
DEAD_LETTER = "bronze/_dead_letter/speedband_spark"
RECORD_SCHEMA = "ts string, link bigint, band int, min int, max int, expected int, _corrupt string"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
BRONZE_DDL = [("snapshot_ts", "timestamp_ntz"), ("link_id", "bigint"), ("speed_band", "smallint"),
              ("min_speed", "smallint"), ("max_speed", "smallint"), ("_source", "string"),
              ("_ingested_at", "timestamp_ntz"), ("_late", "boolean"), ("obs_date", "date"),
              ("obs_hour", "smallint"), ("part", "smallint")]
DLQ_DDL = [("topic", "string"), ("partition", "int"), ("offset", "bigint"), ("key", "string"), ("value", "string"),
           ("reason", "string"), ("dead_lettered_at", "timestamp_ntz")]


def kafka_package() -> str:
    import pyspark
    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def ensure_table(spark, path: str, ddl, partition_by=None) -> None:
    from delta.tables import DeltaTable
    b = DeltaTable.createIfNotExists(spark).location(path)
    for name, typ in ddl:
        b = b.addColumn(name, typ)
    if partition_by:
        b = b.partitionedBy(*partition_by)
    b.execute()


def classify(kdf, partitions: int):
    """Kafka rows (key, value, partition, offset, timestamp) -> the same rows with the parsed fields, `kind`
    ('eos' or 'rec') and `reason` (NULL when the message is valid). Mirrors consumer.decode_message."""
    from pyspark.sql import functions as F
    value = F.col("value").cast("string")
    key = F.col("key").cast("string")
    # PERMISSIVE parsing: text that is not a JSON object lands in _corrupt (the struct itself is not NULL)
    parsed = F.from_json(value, RECORD_SCHEMA, {"columnNameOfCorruptRecord": "_corrupt"})
    df = kdf.select("key", "value", "partition", "offset", "timestamp", "topic",
                    key.alias("k"), value.alias("v"), parsed.alias("r"))
    # zone offsets or fractional formats do not match the pattern -> NULL -> bad_ts (naive SGT only, as consumer.py)
    ts = F.try_to_timestamp(F.col("r.ts"), F.lit(TS_FORMAT)).cast("timestamp_ntz")
    is_eos = F.col("k") == F.lit(m.EOS_KEY.decode())
    band, mn, mx, link = F.col("r.band"), F.col("r.min"), F.col("r.max"), F.col("r.link")
    small = lambda c: c.isNull() | c.between(-32768, 32767)  # noqa: E731  (int16 columns)
    reason = (F.when(F.col("r").isNull() | F.col("r._corrupt").isNotNull(), "malformed_json")
              .when(ts.isNull(), "bad_ts")
              .when(is_eos & (F.col("r.expected").isNull() | (F.col("r.expected") < 0)), "bad_eos")
              .when(is_eos, F.lit(None))
              .when(link.isNull() | (link <= 0), "bad_link")
              .when(band.isNull() | ~small(band), "bad_band")
              .when(~small(mn) | ~small(mx), "bad_speed")
              .when(F.col("k").isNull() | (F.col("k") != link.cast("string")), "key_does_not_match_link")
              .when(F.pmod(link, F.lit(partitions)) != F.col("partition"), "wrong_partition"))
    return df.select("*", ts.alias("snapshot_ts"), F.when(is_eos, "eos").otherwise("rec").alias("kind"),
                     reason.alias("reason"))


def to_bronze(recs, ingested_at: dt.datetime):
    """Valid records -> bronze rows, one per (snapshot_ts, link_id): the copy with the lowest (partition, offset)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    w = Window.partitionBy("snapshot_ts", "link_id").orderBy("partition", "offset")
    first = (recs.select("snapshot_ts", F.col("r.link").alias("link_id"), F.col("r.band").alias("speed_band"),
                         F.col("r.min").alias("min_speed"), F.col("r.max").alias("max_speed"), "partition", "offset")
             .withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1))
    return first.select(
        "snapshot_ts", "link_id",
        F.col("speed_band").cast("smallint").alias("speed_band"),
        F.col("min_speed").cast("smallint").alias("min_speed"),
        F.col("max_speed").cast("smallint").alias("max_speed"),
        F.lit("spark").alias("_source"),
        F.lit(ingested_at.strftime("%Y-%m-%d %H:%M:%S.%f")).cast("timestamp_ntz").alias("_ingested_at"),
        F.lit(None).cast("boolean").alias("_late"),
        F.to_date("snapshot_ts").alias("obs_date"),
        F.hour("snapshot_ts").cast("smallint").alias("obs_hour"),
        F.col("partition").cast("smallint").alias("part"),
    )


class BronzeSink:
    """foreachBatch target. Keeps per-process counters and appends one JSON line per batch to `log_path`."""

    def __init__(self, spark, lake_root: str, partitions: int, table: str = TABLE, dead_letter: str = DEAD_LETTER,
                 log_path: str | None = None, crash_after_batches: int = 0):
        self.spark = spark
        self.path = f"{lake_root}/{table}"
        self.dlq_path = f"{lake_root}/{dead_letter}"
        self.partitions = partitions
        self.log_path = log_path
        self.crash_after_batches = crash_after_batches
        self.batches_written = 0
        ensure_table(spark, self.path, BRONZE_DDL, ["obs_date", "obs_hour", "part"])
        ensure_table(spark, self.dlq_path, DLQ_DDL)

    def _merge(self, path: str, src, on: str) -> dict:
        from delta.tables import DeltaTable
        t = DeltaTable.forPath(self.spark, path)
        before = t.history(1).select("version").collect()[0]["version"]
        t.alias("t").merge(src.alias("s"), on).whenNotMatchedInsertAll().execute()
        h = t.history(1).select("version", "operation", "operationMetrics").collect()[0]
        if h["version"] == before:   # a MERGE that inserts nothing writes no new version
            return {"version": before, "operation": None, "inserted": 0}
        return {"version": h["version"], "operation": h["operation"],
                "inserted": int((h["operationMetrics"] or {}).get("numTargetRowsInserted", 0))}

    def __call__(self, batch, batch_id: int) -> None:
        from pyspark.sql import functions as F
        t_start = time.time()
        c = classify(batch, self.partitions).persist()
        try:
            counts = {r["kind"] + ("_bad" if r["bad"] else ""): r["n"] for r in
                      c.groupBy("kind", F.col("reason").isNotNull().alias("bad")).count()
                      .withColumnRenamed("count", "n").collect()}
            if not counts:
                return
            res = {"batch_id": batch_id, "messages": sum(counts.values()), "counts": counts}
            good = c.where((F.col("kind") == "rec") & F.col("reason").isNull())
            if counts.get("rec", 0):
                bronze = to_bronze(good, dt.datetime.now(dt.timezone.utc).replace(tzinfo=None))
                keys = bronze.select("obs_date", "obs_hour").distinct().collect()
                # literal partition predicate so the MERGE reads only the slices this batch can touch
                prune = " OR ".join(f"(t.obs_date = DATE'{k['obs_date']}' AND t.obs_hour = {k['obs_hour']})"
                                    for k in keys)
                on = (f"({prune}) AND t.obs_date = s.obs_date AND t.obs_hour = s.obs_hour AND t.part = s.part "
                      "AND t.snapshot_ts = s.snapshot_ts AND t.link_id = s.link_id")
                res["bronze"] = self._merge(self.path, bronze, on)
            bad = c.where(F.col("reason").isNotNull())
            if sum(v for k, v in counts.items() if k.endswith("_bad")):
                dlq = bad.select("topic", "partition", "offset", F.col("k").alias("key"), F.col("v").alias("value"),
                                 "reason", F.current_timestamp().cast("timestamp_ntz").alias("dead_lettered_at"))
                res["dead_letter"] = self._merge(self.dlq_path, dlq,
                                                 "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset")
            t_commit = time.time()
            eos = (c.where((F.col("kind") == "eos") & F.col("reason").isNull())
                   .select(F.col("r.ts").alias("ts"), "partition", F.unix_millis("timestamp").alias("create_ms"))
                   .collect())
            res.update(t_start=t_start, t_commit=t_commit, seconds=round(t_commit - t_start, 3),
                       eos=[{"ts": e["ts"], "partition": e["partition"],
                             "latency_ms": round(t_commit * 1000 - e["create_ms"], 1)} for e in eos])
        finally:
            c.unpersist()
        if self.log_path:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(res) + "\n")
        if "bronze" in res:
            self.batches_written += 1
            if self.crash_after_batches and self.batches_written >= self.crash_after_batches:
                # crash test: the Delta commit is done, Spark's commits log is not; no clean shutdown of anything
                os._exit(70)


def run(settings, *, checkpoint: str, log_path: str | None = None, table: str = TABLE,
        starting_offsets: str = "earliest", max_offsets_per_trigger: int = 500_000, idle_exit_s: float = 30.0,
        max_seconds: float = 3600.0, crash_after_batches: int = 0, ready_file: str | None = None) -> dict:
    from . import spark_jobs
    spark = spark_jobs.session("sgtl-stream", extra_packages=[kafka_package()], shuffle_partitions=4)
    sink = BronzeSink(spark, settings.lake_root, settings.partitions, table=table, log_path=log_path,
                      crash_after_batches=crash_after_batches)
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
           .option("subscribe", settings.topic)
           .option("startingOffsets", starting_offsets)
           .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
           .option("failOnDataLoss", "true")
           .load())
    q = (src.writeStream.foreachBatch(sink).option("checkpointLocation", checkpoint)
         .queryName("speedbands_bronze").start())
    if ready_file:
        Path(ready_file).write_text(str(os.getpid()), encoding="utf-8")
    t0 = last_input = time.time()
    seen_input = False
    try:
        while q.isActive and time.time() - t0 < max_seconds:
            time.sleep(1.0)
            prog = q.lastProgress
            if prog and prog.get("numInputRows", 0) > 0:
                last_input, seen_input = time.time(), True
            if seen_input and time.time() - last_input > idle_exit_s and not _has_backlog(q):
                break
        if q.exception():
            raise RuntimeError(str(q.exception()))
    finally:
        q.stop()
        spark.stop()
    return {"batches_written": sink.batches_written, "seconds": round(time.time() - t0, 1)}


def _has_backlog(q) -> bool:
    """True while Kafka reports offsets beyond what the last batch read."""
    prog = q.lastProgress or {}
    for s in prog.get("sources", []):
        end, latest = s.get("endOffset"), s.get("latestOffset")
        if latest and end and json.dumps(end, sort_keys=True) != json.dumps(latest, sort_keys=True):
            return True
    return False
