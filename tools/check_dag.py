"""Parse airflow/dags with the real Airflow (the version the Docker image uses) and check the task graph.

tests/test_cutoff.py checks the DAG's commands against stubbed `airflow` modules, which cannot notice an import or
API error against Airflow itself. This script needs `apache-airflow` installed (CI installs 3.3.2, the version in
docker/airflow/Dockerfile, in a job of its own). It fails if the DAG folder has an import error, if the DAG is
missing, or if its tasks or their order differ from the pipeline's.

    python tools/check_dag.py              # the repository's DAG
    python tools/check_dag.py --self-test  # also show that a broken copy of the DAG is reported
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DAGS = REPO / "airflow" / "dags"
DAG_ID = "sg_traffic_lakehouse"
CHAIN = ["ingest_batch", "ingest_speedbands", "profile_bronze", "dbt_build", "quality_checks", "publish_metrics"]


def _dagbag(folder: Path):
    try:
        from airflow.dag_processing.dagbag import DagBag  # Airflow 3.1+
    except ImportError:  # pragma: no cover - older layout
        from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=str(folder), include_examples=False)


def problems(folder: Path) -> list[str]:
    bag = _dagbag(folder)
    bad = [f"import error in {f}: {e.strip().splitlines()[-1] if e.strip() else e}"
           for f, e in bag.import_errors.items()]
    dag = bag.dags.get(DAG_ID)
    if dag is None:
        return bad + [f"DAG {DAG_ID} not found in {folder}"]
    if sorted(dag.task_ids) != sorted(CHAIN):
        bad.append(f"tasks are {sorted(dag.task_ids)}, expected {sorted(CHAIN)}")
        return bad
    for up, down in zip(CHAIN, CHAIN[1:]):
        if dag.get_task(down).upstream_task_ids != {up}:
            bad.append(f"{down} runs after {sorted(dag.get_task(down).upstream_task_ids)}, expected [{up!r}]")
    if dag.get_task("quality_checks").retries != 0:
        bad.append("the strict quality gate must not retry")
    return bad


def self_test() -> None:
    """A copy of the DAG with an operator name that Airflow does not have must be reported."""
    src = (DAGS / f"{DAG_ID}.py").read_text(encoding="utf-8")
    old = "from airflow.providers.standard.operators.bash import BashOperator"
    assert src.count(old) == 1, "self-test target not found in the DAG"
    with tempfile.TemporaryDirectory() as tmp:
        broken = Path(tmp) / f"{DAG_ID}.py"
        broken.write_text(src.replace(old, old.replace("import BashOperator", "import BashOperatr as BashOperator")),
                          encoding="utf-8")
        bad = problems(Path(tmp))
    assert any("import error" in b for b in bad), f"a broken DAG was not reported: {bad}"
    print(f"self-test: a broken copy of the DAG is reported ({bad[0][:120]})")


def main() -> int:
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    if "--self-test" in sys.argv[1:]:
        self_test()
    bad = problems(DAGS)
    for b in bad:
        print("FAIL", b)
    if not bad:
        print(f"{DAG_ID}: parsed by Airflow without import errors; tasks {' >> '.join(CHAIN)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
