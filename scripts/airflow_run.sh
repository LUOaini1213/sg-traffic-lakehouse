#!/usr/bin/env bash
# Start Airflow (standalone) next to MinIO, trigger the DAG with a pinned snapshot cutoff, wait for the
# run to finish and save its task states and durations to results/airflow/dag_run.json.
# Usage: bash scripts/airflow_run.sh [YYYYmmddHHMM]
set -euo pipefail
cd "$(dirname "$0")/.."
UNTIL=${1:-}
DAG=sg_traffic_lakehouse
docker compose up -d minio   # the DAG needs only object storage
docker compose --profile airflow up -d airflow
af() { docker compose exec -T airflow airflow "$@" 2>/dev/null; }
json() { python3 -c "import sys; t=sys.stdin.read(); i=min([x for x in (t.find('['), t.find('{')) if x >= 0]); print(t[i:])"; }

echo "waiting for the DAG to be parsed..."
for i in $(seq 1 120); do af dags list -o json | grep -q "\"$DAG\"" && break; sleep 5; done
af dags unpause $DAG > /dev/null
if [ -n "${COLLECT_RUN_ID:-}" ]; then   # only re-collect the summary of an earlier run
  RUN_ID=$COLLECT_RUN_ID
else
  RUN_ID=manual__$(date +%Y%m%dT%H%M%S)
  af dags trigger $DAG --run-id "$RUN_ID" --conf "{\"until\": \"$UNTIL\"}" > /dev/null
  echo "triggered $RUN_ID"
fi
while true; do
  STATE=$(af dags list-runs $DAG -o json | json | python3 -c "
import sys, json
runs = [r for r in json.load(sys.stdin) if r['run_id'] == '$RUN_ID']
print(runs[0]['state'] if runs else 'pending')")
  echo "$(date +%T) $STATE"
  case $STATE in success|failed) break ;; esac
  sleep 20
done
mkdir -p results/airflow
af tasks states-for-dag-run $DAG "$RUN_ID" -o json | json > results/airflow/.tasks.json
af dags list-runs $DAG -o json | json > results/airflow/.runs.json
# attempts per task = number of attempt=N.log files Airflow wrote for it
docker compose exec -T airflow bash -c "cd '/opt/airflow/logs/dag_id=$DAG/run_id=$RUN_ID' && for d in task_id=*; do echo \${d#task_id=} \$(ls \$d | grep -c '^attempt=') \$(grep -l \"shouldn't be running anymore\" \$d/attempt=*.log 2>/dev/null | wc -l); done"   > results/airflow/.attempts.txt
python3 - "$RUN_ID" "$UNTIL" <<'PY'
import datetime as dt, json, sys
run_id, until = sys.argv[1], sys.argv[2]
tasks = json.load(open("results/airflow/.tasks.json"))
attempts = {l.split()[0]: (int(l.split()[1]), int(l.split()[2])) for l in open("results/airflow/.attempts.txt") if l.strip()}
run = [r for r in json.load(open("results/airflow/.runs.json")) if r["run_id"] == run_id][0]
def secs(a, b):
    try:
        return round((dt.datetime.fromisoformat(b) - dt.datetime.fromisoformat(a)).total_seconds(), 1)
    except (TypeError, ValueError):
        return None
out = {"dag_id": run["dag_id"], "run_id": run_id, "state": run["state"], "conf": {"until": until},
       "start_date": run.get("start_date"), "end_date": run.get("end_date"),
       "duration_s": secs(run.get("start_date"), run.get("end_date")),
       "tasks": [{"task_id": t["task_id"], "state": t["state"], "start_date": t.get("start_date"),
                  "end_date": t.get("end_date"), "duration_s": secs(t.get("start_date"), t.get("end_date")),
                  "attempts": attempts.get(t["task_id"], (None, None))[0],
                  "attempts_terminated_by_scheduler": attempts.get(t["task_id"], (None, None))[1]}
                 for t in sorted(tasks, key=lambda t: t.get("start_date") or "")]}
json.dump(out, open("results/airflow/dag_run.json", "w"), indent=2)
print(json.dumps(out, indent=2))
PY
rm -f results/airflow/.tasks.json results/airflow/.runs.json results/airflow/.attempts.txt
[ "$STATE" = success ]
