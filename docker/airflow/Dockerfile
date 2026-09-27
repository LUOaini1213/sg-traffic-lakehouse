FROM apache/airflow:3.3.2-python3.12
# The pipeline runs in its own virtualenv so dbt's dependencies cannot conflict with Airflow's.
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN python -m venv /opt/airflow/pipeline \
 && /opt/airflow/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
 && /opt/airflow/pipeline/bin/python -c "import duckdb; duckdb.sql('INSTALL httpfs'); duckdb.sql('INSTALL delta')"
# The DuckDB warehouse volume is mounted here; a new named volume copies this directory's owner (the airflow user).
USER root
RUN mkdir -p /data/warehouse && chown airflow:0 /data/warehouse
USER airflow
