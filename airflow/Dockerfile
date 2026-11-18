# Airflow 2.10.5 (same version as the infraops harness) + this project's task dependencies.
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt

COPY airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt \
        --constraint "${CONSTRAINTS}"

# dbt in its own venv; tasks call it through DBT_EXECUTABLE
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.8.0" "dbt-snowflake==1.8.0" "dbt-duckdb~=1.8.0"

ENV DBT_EXECUTABLE=/home/airflow/dbt-venv/bin/dbt
