#!/usr/bin/env bash
# Smoke test of the Docker stack on the synthetic fixture:
#   1. Kafka replay (with duplicates and late snapshots) -> a consumer that crashes after 3 bronze writes, before
#      committing -> a restarted consumer in the same group -> Delta bronze on MinIO: every row exactly once;
#   2. a second replay to a new group that must not add a single Delta version;
#   3. the batch pipeline against MinIO (its file backfill must add nothing);
#   4. MinIO honours the conditional PUT that makes Delta commits safe: a commit file that already exists is never
#      overwritten by a writer that loses the race.
# Used by CI and runnable locally after `docker compose up -d redpanda minio`.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-python}
set -a; . ./.env; set +a
export PYTHONPATH=$PWD/src SNAPSHOT_MIN_AGE_S=0 SOURCE_DIR=$PWD/tests/fixtures/raw/datamall
export LAKE_ROOT=s3://sgtl-smoke KAFKA_TOPIC=sg.speedbands.smoke-$(date +%s)
WORK=$(mktemp -d)
export DUCKDB_PATH=$WORK/warehouse.duckdb RESULTS_DIR=$WORK/results

for i in $(seq 1 30); do curl -sf http://localhost:29000/minio/health/live >/dev/null && break; sleep 2; done

$PY - <<'PY'
from lakehouse import config
from lakehouse.storage import Lake
lake = Lake(config.load()); lake.ensure_bucket()
for p in ("bronze", "gold"): lake.delete_prefix(p)
PY

consume() {  # consume <group> <out> [extra args...]
  rm -f $WORK/ready
  $PY -m lakehouse consume --group "$1" --idle-exit 20 --ready-file $WORK/ready --out "$2" "${@:3}" > /dev/null &
  CONSUMER=$!
  for i in $(seq 1 60); do [ -f $WORK/ready ] && break; sleep 1; done
}

GROUP=smoke-1-$RANDOM
consume $GROUP consume_1.json --crash-after-writes 3
$PY -m lakehouse produce --dup-rate 0.2 --late 2 > /dev/null
set +e; wait $CONSUMER; CODE=$?; set -e
[ $CODE = 70 ] || { echo "the crashing consumer exited with $CODE, expected 70"; exit 1; }
consume $GROUP consume_1b.json
wait $CONSUMER
$PY - <<'PY'
from lakehouse import config
from lakehouse.storage import Lake
print("bronze after the crash and restart:", Lake(config.load()).delta_stats("bronze/speedbands"))
PY
consume smoke-2-$RANDOM consume_2.json
$PY -m lakehouse produce > /dev/null
wait $CONSUMER
$PY -m lakehouse pipeline > $WORK/pipeline.log 2>&1 || { tail -50 $WORK/pipeline.log; exit 1; }

$PY - <<'PY'
import json, os, pathlib
import pyarrow as pa
from deltalake import DeltaTable
from lakehouse import config
from lakehouse.storage import Lake
r = pathlib.Path(os.environ["RESULTS_DIR"])
c1b, c2 = (json.loads((r / f"consume_{i}.json").read_text()) for i in ("1b", "2"))
layers = json.loads((r / "layer_counts.json").read_text())
files = json.loads((r / "speedband_files.json").read_text())
prof = json.loads((r / "profile_bronze.json").read_text())
assert c1b["records_consumed"] > 0, c1b  # the restarted consumer resumed from the committed offsets
assert c2["rows_new_in_bronze"] == 0 and c2["bronze_commits"] == 0, c2
assert all(f["rows_new_in_bronze"] == 0 for f in files["snapshots"]), "file backfill duplicated stream rows"
assert layers["bronze"]["speedbands"]["rows"] == 120, layers
assert {d["dataset"]: d["duplicate_keys"] for d in prof["datasets"]}["speedbands"] == 0, prof
assert prof["all_reconciled"], prof["reconciliation"]
assert layers["gold"]["gold_congestion_episodes"] == 3, layers

# a losing writer must not overwrite an existing commit file (conditional PUT, If-None-Match)
lake = Lake(config.load())
row = pa.table({"k": pa.array([1], pa.int64())})
lake.write_delta("smoke/cas", row, mode="overwrite")
t = lake.delta("smoke/cas")
nxt = f"{lake.path('smoke/cas')}/_delta_log/{t.version() + 1:020d}.json"
with lake.fs.open_output_stream(nxt) as f:  # another writer takes the next version first
    f.write(b'{"commitInfo":{"timestamp":1,"operation":"SMOKE-TEST","operationParameters":{}}}\n')
t.merge(pa.table({"k": pa.array([2], pa.int64())}), predicate="t.k = s.k", source_alias="s",
        target_alias="t").when_not_matched_insert_all().execute()  # this writer loaded the older version
with lake.fs.open_input_stream(nxt) as f:
    assert b"SMOKE-TEST" in f.read(), "an existing Delta commit file was overwritten"
print("stack smoke test passed:", json.dumps(layers["gold"]))
PY
rm -rf "$WORK"
