"""The daily run on the customer's own Airflow (airflow/include/tekspaz_daily_steps.py), without Airflow."""

import json
import re
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
import tekspaz_daily_steps as steps  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
UNIX = sys.platform != "win32" and shutil.which("bash") is not None
DEV_DATABASE = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV"  # stamped with the project's name: kept on a line of its own


def test_names_come_from_the_stamped_project(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        '[build-system]\nrequires = []\n\n[project]\nname = "fresh-mart-daily"\nversion = "1"\n'
    )
    slug = steps.project_slug(tmp_path)
    assert slug == "fresh-mart-daily"
    assert steps.dag_id(slug, "dev") == "tekspaz__fresh-mart-daily__dev"
    assert steps.settings_variable(slug, "production") == "tekspaz__fresh-mart-daily__production__secret"
    assert "secret" in steps.settings_variable(slug, "dev")  # Airflow hides values of Variables named like this
    assert steps.project_slug(ROOT)  # the template itself has a name


def test_the_repository_is_two_folders_up_unless_configured(tmp_path):
    dag = ROOT / "airflow" / "dags" / "tekspaz_daily.py"
    assert steps.project_dir(str(dag), {}) == ROOT
    assert steps.project_dir(str(dag), {"TEKSPAZ_PROJECT_DIR": str(tmp_path)}) == tmp_path


def test_the_steps_match_the_other_runners():
    dev = [("raw_data", "raw_data"), ("dbt_build", "dbt_build_dev"), ("forecast", "forecast")]
    assert steps.steps_for("dev") == dev
    assert steps.steps_for("production") == [("dbt_build", "dbt_build_production")]
    assert "dbt deps" in steps.STEP_COMMANDS["forecast"]  # each step starts from a fresh copy
    for key in ("dbt_build_dev", "forecast", "dbt_build_production"):  # dbt Core, or the customer's dbt Cloud job
        command = steps.STEP_COMMANDS[key]
        assert 'if [ -n "${DBT_CLOUD_JOB_ID:-}" ]; then python -m loaders.dbt_cloud_run --step' in command


def test_settings_say_what_to_do_when_missing_and_keep_only_setting_names():
    with pytest.raises(RuntimeError, match="Run on your Airflow"):
        steps.parse_settings(None, "p", "dev")
    with pytest.raises(RuntimeError, match="off and on again"):
        steps.parse_settings("not json", "p", "dev")
    raw = json.dumps({"env": {"SNOWFLAKE_ACCOUNT": "acme-xy", "bad name": "x", "PATH": "/evil"}})
    assert steps.parse_settings(raw, "p", "dev") == {"SNOWFLAKE_ACCOUNT": "acme-xy", "PATH": "/evil"}


def test_project_code_sees_its_settings_not_airflows(tmp_path):
    environ = {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow:pw@db/airflow",
        "HTTPS_PROXY": "http://proxy:3128",
        "HOME": "/home/airflow",
    }
    settings = {"SNOWFLAKE_USER": "loader", "PATH": "/evil"}
    context = {"AIRFLOW_CTX_DAG_ID": "d", "not_ctx": "x"}
    env = steps.step_env(settings, "production", tmp_path / "w", tmp_path / "venv", context, environ)
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in env
    assert env["HTTPS_PROXY"] == "http://proxy:3128" and env["SNOWFLAKE_USER"] == "loader"
    assert env["PATH"].startswith(str(tmp_path / "venv" / "bin"))
    assert env["PYTHONPATH"] == str(tmp_path / "w" / "python")
    assert env["DBT_TARGET"] == "prod" and env["AIRFLOW_CTX_DAG_ID"] == "d" and "not_ctx" not in env


def test_steps_only_use_unset_variables_with_a_default():
    """Steps run under `set -u`: a setting the project doesn't have (no Airbyte, no Postgres) must not stop them."""
    always = {"PATH", "PYTHONPATH", "DBT_TARGET", "DBT_PROFILES_DIR", "HOME"}
    for key, command in steps.STEP_COMMANDS.items():
        for name in re.findall(r"\$\{?([A-Z][A-Z0-9_]*)", command):
            assert name in always or "${" + name + ":-}" in command, f"{key} uses ${name} without a default"


def test_the_schedule_rule_matches_the_platform():
    assert steps.valid_schedule("30 2 * * *")
    assert not steps.valid_schedule("*/5 * * * *") and not steps.valid_schedule("0 2 * *")


def _fake_project(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "python" / "data_generation").mkdir(parents=True)
    (root / "dbt").mkdir()
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_text("ref")
    (root / "requirements.txt").write_text("dbt-core\n")
    return root


def _fake_venv(tmp_path: Path, calls: Path) -> Path:
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    for tool in ("python", "dbt"):
        stub = venv / "bin" / tool
        line = f'echo "{tool} $* | db=${{SNOWFLAKE_DATABASE:-}} | pp=$PYTHONPATH" >> "{calls}"'
        stub.write_text(f"#!/usr/bin/env bash\n{line}\n")
        stub.chmod(0o755)
    return venv


@pytest.mark.skipif(not UNIX, reason="needs a Unix bash")
def test_a_step_runs_in_a_fresh_copy_with_the_projects_venv_and_cleans_up(tmp_path, monkeypatch):
    root, calls = _fake_project(tmp_path), tmp_path / "calls.txt"
    venv = _fake_venv(tmp_path, calls)
    monkeypatch.setattr(steps, "ensure_venv", lambda r, c: venv)
    logged = []
    settings = {"SNOWFLAKE_DATABASE": DEV_DATABASE}
    steps.run_step("raw_data", "dev", root, settings, {}, "manual__2026-11-12T07:39:01+00:00", "raw_data",
                   log=logged.append, environ={"TEKSPAZ_CACHE_DIR": str(tmp_path / "cache")})
    lines = calls.read_text().splitlines()
    assert lines[0].startswith("python python/data_generation/deterministic_generator.py")
    assert lines[1].startswith("python -m loaders.snowflake_loader") and f"db={DEV_DATABASE}" in lines[1]
    assert lines[1].endswith("/python") and "manual__2026-11-12T07_39_01_00_00__raw_data" in lines[1]
    assert not any((tmp_path / "cache" / "runs").iterdir())  # the copy is removed afterwards


@pytest.mark.skipif(not UNIX, reason="needs a Unix bash")
def test_production_refuses_a_dev_database(tmp_path, monkeypatch):
    root, calls = _fake_project(tmp_path), tmp_path / "calls.txt"
    monkeypatch.setattr(steps, "ensure_venv", lambda r, c: _fake_venv(tmp_path, calls))
    logged = []
    with pytest.raises(RuntimeError, match="dbt_build exited 1"):
        steps.run_step("dbt_build_production", "production", root, {"SNOWFLAKE_DATABASE": DEV_DATABASE}, {},
                       "r", "dbt_build", log=logged.append, environ={"TEKSPAZ_CACHE_DIR": str(tmp_path / "cache")})
    assert "SNOWFLAKE_DATABASE must be the production database" in logged
    assert not calls.exists()


@pytest.mark.skipif(not UNIX, reason="needs fcntl")
def test_the_venv_is_made_once_per_requirements(tmp_path):
    root, made = _fake_project(tmp_path), []

    def fake_run(command, check):
        made.append(command)
        if command[1:3] == ["-m", "venv"]:
            Path(command[3], "bin").mkdir(parents=True)

    first = steps.ensure_venv(root, tmp_path / "cache", run=fake_run)
    again = steps.ensure_venv(root, tmp_path / "cache", run=fake_run)
    assert first == again and (first / ".ready").exists() and len(made) == 2
    (root / "requirements.txt").write_text("dbt-core\npandas\n")
    assert steps.ensure_venv(root, tmp_path / "cache", run=fake_run) != first


@pytest.mark.skipif(not UNIX, reason="needs fcntl")
def test_a_failed_install_leaves_no_half_made_venv(tmp_path):
    root = _fake_project(tmp_path)

    def failing(command, check):
        if command[1:3] == ["-m", "venv"]:
            Path(command[3], "bin").mkdir(parents=True)
            return
        raise OSError("no network")

    with pytest.raises(OSError):
        steps.ensure_venv(root, tmp_path / "cache", run=failing)
    assert not any(p.is_dir() for p in (tmp_path / "cache").iterdir())


def test_the_dag_file_needs_nothing_but_airflow():
    source = (ROOT / "airflow" / "dags" / "tekspaz_daily.py").read_text(encoding="utf-8")
    imports = re.findall(r"^\s*(?:from|import) ([\w.]+)", source, re.M)
    allowed = {"__future__", "logging", "sys", "datetime", "pathlib", "airflow", "tekspaz_daily_steps"}
    assert all(m.split(".")[0] in allowed for m in imports), imports
    assert "is_paused_upon_creation=True" in source  # nothing runs before Tekspaz has written its settings


def test_dags_with_airflow():
    """Both DAGs import and have the steps in order (runs in CI, where Airflow is installed)."""
    pytest.importorskip("airflow.models")
    from airflow.models import DagBag

    import inspect

    options = {"include_examples": False} if "include_examples" in inspect.signature(DagBag.__init__).parameters else {}
    bag = DagBag(dag_folder=str(ROOT / "airflow" / "dags" / "tekspaz_daily.py"), **options)  # Airflow 2 and 3
    assert not bag.import_errors
    slug = steps.project_slug(ROOT)
    dev, prod = bag.dags[steps.dag_id(slug, "dev")], bag.dags[steps.dag_id(slug, "production")]
    assert [t.task_id for t in dev.topological_sort()] == ["raw_data", "dbt_build", "forecast"]
    assert [t.task_id for t in prod.tasks] == ["dbt_build"]
    assert dev.max_active_runs == 1 and dev.catchup is False


def test_airflow_loads_only_the_daily_dag_from_this_repository():
    """.airflowignore, read as Airflow 2 (regexp) does: every other file that mentions airflow and dag is skipped."""
    lines = (ROOT / ".airflowignore").read_text().splitlines()
    patterns = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
    candidates = [p for p in ROOT.rglob("*.py") if ".git" not in p.parts and "__pycache__" not in p.parts]
    mentions = [p for p in candidates if all(w in p.read_text(errors="ignore").lower() for w in ("airflow", "dag"))]
    looked_at = [p.relative_to(ROOT).as_posix() for p in mentions]
    loaded = [path for path in looked_at if not any(re.search(pattern, path) for pattern in patterns)]
    assert loaded == ["airflow/dags/tekspaz_daily.py"]


@pytest.mark.skipif(not UNIX, reason="needs a Unix bash")
@pytest.mark.parametrize("job_id", ["", "678"])
def test_dbt_runs_here_or_on_the_customers_dbt_cloud_job(tmp_path, monkeypatch, job_id):
    root, calls = _fake_project(tmp_path), tmp_path / "calls.txt"
    monkeypatch.setattr(steps, "ensure_venv", lambda r, c: _fake_venv(tmp_path, calls))
    settings = {"DBT_CLOUD_JOB_ID": job_id} if job_id else {}
    steps.run_step("dbt_build_dev", "dev", root, settings, {}, "r", "dbt_build", log=lambda m: None,
                   environ={"TEKSPAZ_CACHE_DIR": str(tmp_path / "cache")})
    lines = calls.read_text().splitlines()
    if job_id:
        cloud = "python -m loaders.dbt_cloud_run --step dbt build --exclude tag:forecast"
        assert [line.split(" | ")[0] for line in lines] == [cloud]
    else:
        assert [line.split(" | ")[0] for line in lines] == ["dbt deps", "dbt build --exclude tag:forecast"]
