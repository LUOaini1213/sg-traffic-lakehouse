#!/usr/bin/env bash
# Smoke test of the Docker stack on the synthetic fixture: Kafka replay (with duplicates and late snapshots) ->
# consumer -> MinIO bronze, a second replay that must change nothing, then the batch pipeline against MinIO.
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

for run in 1 2; do
  rm -f $WORK/ready
  $PY -m lakehouse consume --group smoke-$run-$RANDOM --idle-exit 8 --ready-file $WORK/ready       --out consume_$run.json > /dev/null &
  CONSUMER=$!
  for i in $(seq 1 60); do [ -f $WORK/ready ] && break; sleep 1; done
  if [ $run = 1 ]; then $PY -m lakehouse produce --dup-rate 0.2 --late 2 > /dev/null
  else $PY -m lakehouse produce > /dev/null; fi
  wait $CONSUMER
done
$PY -m lakehouse pipeline > $WORK/pipeline.log 2>&1 || { tail -50 $WORK/pipeline.log; exit 1; }

$PY - <<'PY'
import json, os, pathlib
r = pathlib.Path(os.environ["RESULTS_DIR"])
c1, c2 = (json.loads((r / f"consume_{i}.json").read_text()) for i in (1, 2))
layers = json.loads((r / "layer_counts.json").read_text())
files = json.loads((r / "speedband_files.json").read_text())
assert c1["rows_new_in_bronze"] == 120 and c1["snapshots_late"] == 2 and c1["duplicates_dropped_in_consumer"] > 0, c1
assert c2["rows_new_in_bronze"] == 0, c2
assert all(f["rows_new_in_bronze"] == 0 for f in files["snapshots"]), "file backfill duplicated stream rows"
assert layers["bronze"]["speedbands"] == {"files": 30, "rows": 120}, layers
assert layers["gold"]["gold_congestion_episodes"] == 3, layers
print("stack smoke test passed:", json.dumps(layers["gold"]))
PY
rm -rf "$WORK"
