"""
This project's daily run on your own Airflow (Airflow 2.4+ or 3): two DAGs,

    tekspaz__<project>__dev         raw_data -> dbt_build -> forecast
    tekspaz__<project>__production  dbt_build

Turn them on from Tekspaz (TeksInfra > Deployments > "Run on your Airflow"): it writes their settings into
an Airflow Variable and unpauses the DAG. Until then they stay paused. What each step does, and where its
code and settings come from: airflow/include/tekspaz_daily_steps.py and docs/your-airflow.md.

Unlike daily_expense_tracker_fintech_application_orchestration.py (the template's own demo DAG), this needs nothing beyond Airflow
itself: no providers, no private packages. The project's packages go into a virtualenv of their own.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
import tekspaz_daily_steps as steps  # noqa: E402

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG, Variable
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.python import PythonOperator

try:  # Airflow 3
    from airflow.sdk.log import mask_secret
except ImportError:
    try:
        from airflow.utils.log.secrets_masker import mask_secret
    except ImportError:  # the Variable's name contains "secret", so Airflow still hides its whole value
        def mask_secret(secret, name=None):  # noqa: ARG001
            return None

log = logging.getLogger(__name__)

PROJECT_DIR = steps.project_dir(__file__)
SLUG = steps.project_slug(PROJECT_DIR)


def _variable(key: str):
    try:
        return Variable.get(key, default_var=None)  # Airflow 2
    except TypeError:
        return Variable.get(key, default=None)  # Airflow 3


def _schedule(environment: str) -> str:
    try:
        cron = (_variable(steps.schedule_variable(SLUG, environment)) or "").strip()
    except Exception:  # noqa: BLE001 - no metadata database while parsing in a test: the default
        cron = ""
    return cron if steps.valid_schedule(cron) else steps.DEFAULT_SCHEDULE[environment]


def _step(command_key: str, environment: str, **context) -> None:
    settings = steps.parse_settings(_variable(steps.settings_variable(SLUG, environment)), SLUG, environment)
    for value in settings.values():
        if len(value) >= 6:
            mask_secret(value)
    ti = context["ti"]
    ids = {
        "AIRFLOW_CTX_DAG_ID": ti.dag_id,
        "AIRFLOW_CTX_TASK_ID": ti.task_id,
        "AIRFLOW_CTX_DAG_RUN_ID": ti.run_id,
        "AIRFLOW_CTX_TRY_NUMBER": str(ti.try_number),
    }
    steps.run_step(command_key, environment, PROJECT_DIR, settings, ids, ti.run_id, ti.task_id, log=log.info)


for _environment in steps.ENVIRONMENTS:
    with DAG(
        dag_id=steps.dag_id(SLUG, _environment),
        description=f"{SLUG}: the daily {_environment} run, set up from Tekspaz",
        schedule=_schedule(_environment),
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        default_args={"owner": "tekspaz", "retries": 1, "retry_delay": timedelta(minutes=5)},
        tags=["tekspaz", _environment],
    ) as _dag:
        _previous = None
        for _task_id, _command_key in steps.steps_for(_environment):
            _task = PythonOperator(
                task_id=_task_id,
                python_callable=_step,
                op_kwargs={"command_key": _command_key, "environment": _environment},
                execution_timeout=timedelta(hours=3),
            )
            if _previous is not None:
                _previous >> _task
            _previous = _task
    globals()[_dag.dag_id] = _dag
