# Run from WSL/Linux. `make ci` needs only Python; the stack targets need Docker.
PY ?= python
export PYTHONPATH := $(CURDIR)/src
FIXTURE_ENV = SOURCE_DIR=$(CURDIR)/tests/fixtures/raw/datamall SNAPSHOT_MIN_AGE_S=0 \
	LAKE_ROOT=$(CURDIR)/.lake/fixture DUCKDB_PATH=$(CURDIR)/.lake/fixture/warehouse.duckdb \
	RESULTS_DIR=$(CURDIR)/.lake/fixture/results
# Stack settings come from .env (copy .env.example); results of real-data runs go to results/
STACK_ENV = set -a && . ./.env && set +a && export RESULTS_DIR=$(CURDIR)/results
UNTIL ?= 202609271923

.PHONY: fixture test fixture-pipeline mutate ci up down stream pipeline repro airflow docs summary \n	spark-test spark-mutate spark-batch spark-stream-bench

fixture:            ## regenerate the synthetic fixture
	$(PY) tests/make_fixture.py

test:               ## unit + end-to-end tests on the fixture (includes dbt build and dbt tests)
	$(PY) -m pytest -q

fixture-pipeline:   ## the batch pipeline on the fixture, local directory lake, no Docker
	rm -rf .lake/fixture && $(FIXTURE_ENV) $(PY) -m lakehouse pipeline

mutate:             ## every planted bug must fail a test
	$(PY) tests/mutate.py

ci: test fixture-pipeline mutate

up:                 ## Redpanda + MinIO
	docker compose up -d redpanda minio

down:
	docker compose --profile airflow down

stream:             ## streaming measurements: throughput, paced latency, idempotent re-run
	$(STACK_ENV) && $(PY) -m lakehouse stream-bench --until $(UNTIL)

pipeline:           ## batch pipeline on real data against MinIO
	$(STACK_ENV) && $(PY) -m lakehouse pipeline --until $(UNTIL) --strict

repro:              ## run the batch pipeline twice from empty buckets and compare gold hashes
	$(STACK_ENV) && $(PY) -m lakehouse repro --until $(UNTIL)

airflow:            ## start Airflow, trigger the DAG and wait for it
	bash scripts/airflow_run.sh $(UNTIL)

docs:               ## dbt docs (catalogue + lineage) and docs/CATALOG.md
	$(STACK_ENV) && $(PY) -m lakehouse dbt docs generate --static && $(PY) tools/catalog.py

summary:            ## results/SUMMARY.md and the generated block in README.md
	$(STACK_ENV) && $(PY) -m lakehouse summarize --update-readme

# --- PySpark layer (pip install -r requirements-spark.txt; needs a JDK 17+) ---------------------------------------
spark-test:         ## Spark models vs dbt on the fixture, streaming sink
	$(PY) -m pytest -q tests/test_spark.py

spark-mutate:       ## every planted bug in the Spark code must fail tests/test_spark.py
	$(PY) tests/mutate.py --spark

# The two targets below need a local-directory lake (LAKE_ROOT=/some/dir) where `ingest-speedbands` and `dbt build`
# already ran with the same UNTIL; Spark has no S3 connector configured here.
spark-batch:        ## Spark silver/gold, compared with the dbt tables, timed against dbt -> results/spark_batch.json
	$(PY) -m lakehouse spark-batch --until $(UNTIL) --bench 3 --reconcile --out spark_batch.json

spark-stream-bench: ## Spark Structured Streaming crash, SIGKILL and paced-latency runs -> results/spark_stream.json
	$(PY) -m lakehouse spark-stream-bench --until $(UNTIL)
