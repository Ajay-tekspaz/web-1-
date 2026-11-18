"""DAG import test (K-12). Needs Airflow 2.10 and infraops on the path; CI installs both."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytest.importorskip("infraops.telemetry.tracked_hook")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def _assert_chain(dag, order):
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)


def test_pipeline_order(dagbag):
    dag = dagbag.get_dag("daily_expense_tracker_fintech_application_orchestration")
    tail = ["verify_raw", "dbt_build_marts", "forecast", "dbt_build_forecast"]
    _assert_chain(dag, ["generate_data", "choose_ingestion", "seed_source_postgres", "airbyte_sync", *tail])
    _assert_chain(dag, ["generate_data", "choose_ingestion", "load_raw", *tail])
    assert dag.get_task("verify_raw").trigger_rule == "none_failed_min_one_success"
    assert dag.catchup is False and dag.max_active_runs == 1


def test_airbyte_sync_runs_before_dbt_and_waits(dagbag):
    from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

    task = dagbag.get_dag("daily_expense_tracker_fintech_application_orchestration").get_task("airbyte_sync")
    assert isinstance(task, AirbyteTriggerSyncOperator)
    assert task.asynchronous is False


def test_dag_uses_tracked_snowflake_hook():
    import daily_expense_tracker_fintech_application_orchestration as module
    from infraops.telemetry.tracked_hook import TrackedSnowflakeHook, build_query_tag

    assert module.TrackedSnowflakeHook is TrackedSnowflakeHook

    class TI:
        dag_id, task_id, run_id, try_number = "daily_expense_tracker_fintech_application_orchestration", "load_raw", "manual__1", 3

    hook = module.snowflake_hook({"ti": TI()})
    assert isinstance(hook, TrackedSnowflakeHook)
    assert hook.snowflake_conn_id == "snowflake_lakehouse"
    assert build_query_tag(hook.context) == (
        '{"dag_id":"daily_expense_tracker_fintech_application_orchestration",'
        '"task_id":"load_raw","run_id":"manual__1","try_number":3}'
    )
