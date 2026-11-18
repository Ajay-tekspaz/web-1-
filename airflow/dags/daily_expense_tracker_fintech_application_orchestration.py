"""
[K-12] daily-expense-tracker-fintech-application daily pipeline.

    generate_data -> choose_ingestion -> seed_source_postgres -> airbyte_sync ─┐   (INGESTION_MODE=airbyte, default)
                                      └> load_raw ─────────────────────────────┴> verify_raw   (python fallback)
        -> dbt_build_marts -> forecast -> dbt_build_forecast

Every Snowflake call this DAG makes goes through infraops' TrackedSnowflakeHook: each statement
is tagged {"dag_id","task_id","run_id","try_number"} (Path A), its query id is logged as
"Snowflake query id: ..." and pushed to XCom "snowflake_query_ids", including when it fails.
dbt opens its own connection; it receives the same four keys through AIRFLOW_CTX_* and
dbt_project.yml's query_tag, and the dbt tasks log/push the ids from run_results.json.

Environment: LAKEHOUSE_TARGET=dev (Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX, connection `snowflake_lakehouse`)
or local (DuckDB stand-in). Trigger with {"break_the_build": true} to add a deliberately
broken model and see the failure (and its query id) in the task log.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from infraops.telemetry.tracked_hook import TrackedSnowflakeHook

import lakehouse_tasks as tasks

DAG_ID = "daily_expense_tracker_fintech_application_orchestration"
SNOWFLAKE_CONN_ID = "snowflake_lakehouse"
AIRBYTE_CONN_ID = "airbyte_local"


def snowflake_hook(context: dict) -> TrackedSnowflakeHook:
    """A TrackedSnowflakeHook whose QUERY_TAG carries this task's Path A keys."""
    ti = context["ti"]
    return TrackedSnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        context={"dag_id": ti.dag_id, "task_id": ti.task_id, "run_id": ti.run_id,
                 "try_number": ti.try_number, "ti": ti},
    )


with DAG(
    dag_id=DAG_ID,
    description="Generate -> load raw -> dbt build -> forecast, with traceable Snowflake queries",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=5)},
    params={"break_the_build": Param(False, type="boolean",
                                     description="Add a model that fails, to demo failure tracing")},
    tags=["daily-expense-tracker-fintech-application", "dev", "infraops-tenant"],
) as dag:
    generate_data = PythonOperator(task_id="generate_data", python_callable=tasks.generate_data)

    # K-15: Airbyte by default (INGESTION_MODE=airbyte), the Python loader as the fallback
    choose_ingestion = BranchPythonOperator(task_id="choose_ingestion", python_callable=tasks.choose_ingestion)

    seed_source_postgres = PythonOperator(task_id="seed_source_postgres",
                                          python_callable=tasks.seed_source_postgres)

    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=os.environ.get("AIRBYTE_CONNECTION_ID", ""),
        asynchronous=False,  # wait for the sync to finish before dbt runs
        timeout=3600,
        wait_seconds=15,
    )

    load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=tasks.load_raw_python,
        op_kwargs={"hook_factory": snowflake_hook},
    )

    verify_raw = PythonOperator(
        task_id="verify_raw",
        python_callable=tasks.verify_raw,
        op_kwargs={"hook_factory": snowflake_hook},
        trigger_rule="none_failed_min_one_success",  # after whichever ingestion branch ran
    )

    dbt_build_marts = PythonOperator(
        task_id="dbt_build_marts",
        python_callable=tasks.dbt_build,
        op_kwargs={"dbt_args": ["--exclude", "tag:forecast"]},
    )

    forecast = PythonOperator(
        task_id="forecast",
        python_callable=tasks.forecast,
        op_kwargs={"hook_factory": snowflake_hook},
    )

    dbt_build_forecast = PythonOperator(
        task_id="dbt_build_forecast",
        python_callable=tasks.dbt_build,
        op_kwargs={"dbt_args": ["--select", "tag:forecast"]},
    )

    generate_data >> choose_ingestion
    choose_ingestion >> seed_source_postgres >> airbyte_sync >> verify_raw
    choose_ingestion >> load_raw >> verify_raw
    verify_raw >> dbt_build_marts >> forecast >> dbt_build_forecast
