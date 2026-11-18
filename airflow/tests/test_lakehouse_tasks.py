"""Task logic tests: no Airflow needed (lakehouse_tasks is plain Python)."""

import json
import logging
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import lakehouse_tasks as tasks

FIXTURES = Path(__file__).parent / "fixtures"


class FakeTI(SimpleNamespace):
    def __init__(self):
        super().__init__(dag_id="daily_expense_tracker_fintech_application_orchestration", task_id="dbt_build_marts",
                         run_id="scheduled__2026-10-22T02:00:00+00:00", try_number=2, pushed={})

    def xcom_push(self, key, value):
        self.pushed[key] = value


def test_query_ids_from_success_and_compile_failure():
    run_results = json.loads((FIXTURES / "run_results_broken_model.json").read_text())
    ids = tasks.query_ids_from_run_results(run_results)
    assert ids == {
        "model.daily_expense_tracker_fintech_application.stg_orders": ["01b7f0c2-0001-a1b2-0000-0004d2a1b0a1"],
        "model.daily_expense_tracker_fintech_application.broken_model": ["01b7f0c2-0001-a1b2-0000-0004d2a1b0c6"],
    }


def test_airflow_context_is_exported_for_dbt():
    env = tasks.airflow_ctx_env({"ti": FakeTI()})
    assert env == {"AIRFLOW_CTX_DAG_ID": "daily_expense_tracker_fintech_application_orchestration",
                   "AIRFLOW_CTX_TASK_ID": "dbt_build_marts",
                   "AIRFLOW_CTX_DAG_RUN_ID": "scheduled__2026-10-22T02:00:00+00:00", "AIRFLOW_CTX_TRY_NUMBER": "2"}


def test_failed_dbt_build_logs_query_id_pushes_xcom_and_fails(tmp_path, monkeypatch, caplog):
    (tmp_path / "target").mkdir()
    (tmp_path / "dbt_packages").mkdir()  # deps already installed
    stale = tmp_path / "target" / "run_results.json"
    stale.write_text('{"results": [{"unique_id": "model.x.old", "adapter_response": '
                     '{"query_id": "00000000-0000-0000-0000-000000000000"}}]}')
    monkeypatch.setattr(tasks, "DBT_DIR", tmp_path)
    seen = {}

    def fake_run(cmd, cwd, env, capture_output, text):
        seen.update(cmd=cmd, env=env, stale_removed=not stale.exists())
        stale.write_text((FIXTURES / "run_results_broken_model.json").read_text())  # what dbt writes
        return subprocess.CompletedProcess(cmd, 1, stdout="1 of 2 ERROR", stderr="")

    monkeypatch.setattr(tasks.subprocess, "run", fake_run)
    ti = FakeTI()
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError, match="1 failed nodes"):
        tasks.dbt_build(["--exclude", "tag:forecast"], ti=ti, params={"break_the_build": True})

    assert "Snowflake query id: 01b7f0c2-0001-a1b2-0000-0004d2a1b0c6" in caplog.text
    assert ti.pushed[tasks.XCOM_KEY] == ["01b7f0c2-0001-a1b2-0000-0004d2a1b0a1", "01b7f0c2-0001-a1b2-0000-0004d2a1b0c6"]
    assert seen["stale_removed"]  # the previous run's ids can't leak into this task
    assert seen["env"]["AIRFLOW_CTX_TRY_NUMBER"] == "2"
    assert seen["cmd"][-2:] == ["--vars", '{"break_the_build": true}']


def test_snowflake_steps_go_through_the_hook(tmp_path, monkeypatch, small_data):
    folder, _ = small_data
    monkeypatch.setenv("LAKEHOUSE_TARGET", "dev")
    monkeypatch.setattr(tasks, "DATA_DIR", folder)
    calls = []

    class FakeHook:
        def run(self, sql, handler=None):
            calls.append(sql)
            return handler(SimpleNamespace(fetchone=lambda: (5,))) if handler else None

    counts = tasks.load_raw_python(hook_factory=lambda ctx: FakeHook(), ti=FakeTI())
    assert set(counts.values()) == {5}
    assert any(s.startswith("COPY INTO") for s in calls)
    assert tasks.verify_raw(hook_factory=lambda ctx: FakeHook(), ti=FakeTI())


@pytest.mark.parametrize("target, mode, expected", [
    ("dev", None, "seed_source_postgres"),     # Airbyte is the default
    ("dev", "airbyte", "seed_source_postgres"),
    ("dev", "python", "load_raw"),             # documented fallback
    ("local", "airbyte", "load_raw"),          # Airbyte's destination is Snowflake
])
def test_ingestion_branch(monkeypatch, target, mode, expected):
    monkeypatch.setenv("LAKEHOUSE_TARGET", target)
    if mode:
        monkeypatch.setenv("INGESTION_MODE", mode)
    else:
        monkeypatch.delenv("INGESTION_MODE", raising=False)
    assert tasks.choose_ingestion() == expected


def test_unknown_target_is_rejected(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_TARGET", "prod")
    with pytest.raises(ValueError):
        tasks.target()
