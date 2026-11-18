"""
The project's daily run on the customer's own Airflow (airflow/dags/tekspaz_daily.py).

Plain Python, so it is tested without Airflow (airflow/tests/test_tekspaz_daily_steps.py). The same steps
as the GitHub Actions workflows and Tekspaz's hosted Airflow:

    dev         raw_data -> dbt_build -> forecast
    production  dbt_build (a database that looks like dev is refused)

Where things come from:
- The code: this repository, as deployed to the Airflow (git-sync, the image, or the dags bucket with the
  whole repository). Set TEKSPAZ_PROJECT_DIR when the repository isn't two folders above this file.
- The settings (Snowflake key pair, raw data source): one Airflow Variable per environment,
  `tekspaz__<project>__<env>__secret`, which Tekspaz writes when "Run on your Airflow" is turned on
  (TeksInfra > Deployments). Its name contains "secret", so Airflow hides its value in the UI and in logs;
  each value is masked in the task log as well.
- The schedule: the Variable `tekspaz__<project>__<env>__schedule` (cron, UTC), read when the DAG is parsed.

Isolation: each step runs in a fresh copy of the project, with the project's own Python packages in a
virtualenv cached per requirements.txt (never Airflow's), and with only the settings it needs. Airflow's own
environment (its database URL, its secrets) is not passed to project code.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Mapping, Optional, Tuple

ENVIRONMENTS = ("dev", "production")
DEFAULT_SCHEDULE = {"dev": "0 2 * * *", "production": "0 3 * * *"}
SETTING_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


def _dbt(command: str) -> str:
    """dbt Core here, or the same command on the customer's own dbt Cloud job (DBT_CLOUD_JOB_ID; it runs the job
    environment's branch). python/loaders/dbt_cloud_run.py starts the job and waits for it."""
    return (f'if [ -n "${{DBT_CLOUD_JOB_ID:-}}" ]; then python -m loaders.dbt_cloud_run --step "{command}"; '
            f"else cd dbt && dbt deps && {command}; fi")


# The same commands as Tekspaz's hosted Airflow (deploy/airflow/dags/tekspaz_runtime.py), run in a copy of the
# project. Each step starts from a fresh copy, so dbt's packages are installed in every step that runs dbt.
STEP_COMMANDS: Dict[str, str] = {
    # The customer's own Airbyte, else their Postgres, else the template's generator.
    "raw_data": (
        'if [ -n "${AIRBYTE_CONNECTION_ID:-}" ]; then '
        "python -m loaders.airbyte_sync; "
        'elif [ -n "${SOURCE_POSTGRES_HOST:-}" ]; then '
        "python -m loaders.postgres_extractor --data_dir data/generated && "
        "python -m loaders.snowflake_loader --data_dir data/generated --full_refresh --apply; "
        "else python python/data_generation/deterministic_generator.py --output_path data/generated && "
        "python -m loaders.snowflake_loader --data_dir data/generated --apply; fi"
    ),
    "dbt_build_dev": _dbt("dbt build --exclude tag:forecast"),
    "forecast": (
        "python -m forecasts.demand_forecast --target snowflake --apply && " + _dbt("dbt build --select tag:forecast")
    ),
    "dbt_build_production": (
        'case "${SNOWFLAKE_DATABASE:-}" in ""|*_DEV|*_DEV_*) '
        'echo "SNOWFLAKE_DATABASE must be the production database"; exit 1;; esac; ' + _dbt("dbt build")
    ),
}

# What the copy of the project leaves out: history, and anything an earlier local run produced.
_IGNORED = shutil.ignore_patterns(
    ".git", ".venv", "venv", "data", "target", "dbt_packages", "logs", "__pycache__", "*.duckdb"
)
# Network settings a company's Airflow may need for pip, Snowflake and Airbyte; nothing else of Airflow's.
_PASSED_THROUGH = ("HOME", "LANG", "LC_ALL", "TZ", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy",
                   "no_proxy", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "PIP_INDEX_URL", "PIP_EXTRA_INDEX_URL")


def steps_for(environment: str) -> List[Tuple[str, str]]:
    """(task id, command key) in order."""
    if environment == "production":
        return [("dbt_build", "dbt_build_production")]
    return [("raw_data", "raw_data"), ("dbt_build", "dbt_build_dev"), ("forecast", "forecast")]


def project_dir(dag_file: str, environ: Mapping[str, str] = os.environ) -> Path:
    """The repository: TEKSPAZ_PROJECT_DIR, else two folders above airflow/dags/tekspaz_daily.py."""
    configured = environ.get("TEKSPAZ_PROJECT_DIR")
    return Path(configured) if configured else Path(dag_file).resolve().parents[2]


def project_slug(root: Path) -> str:
    """The project's name, as Tekspaz stamped it into pyproject.toml (read without tomllib: Python 3.8+)."""
    text = (root / "pyproject.toml").read_text(encoding="utf-8")
    section = re.search(r"^\[project\]\s*$(.*?)(?=^\[|\Z)", text, re.M | re.S)
    name = re.search(r'^name\s*=\s*"([^"]+)"', section.group(1) if section else "", re.M)
    if not name:
        raise ValueError(f"{root / 'pyproject.toml'} has no [project] name.")
    return re.sub(r"[^A-Za-z0-9_.-]", "-", name.group(1))


def dag_id(slug: str, environment: str) -> str:
    return f"tekspaz__{slug}__{environment}"


def settings_variable(slug: str, environment: str) -> str:
    return f"tekspaz__{slug}__{environment}__secret"


def schedule_variable(slug: str, environment: str) -> str:
    return f"tekspaz__{slug}__{environment}__schedule"


def valid_schedule(cron: str) -> bool:
    """Five fields, a fixed minute: the same rule as the platform."""
    fields = cron.split()
    return len(fields) == 5 and fields[0].isdigit() and 0 <= int(fields[0]) <= 59


def parse_settings(raw: Optional[str], slug: str, environment: str) -> Dict[str, str]:
    """The Variable Tekspaz wrote: {"env": {NAME: value}}. Raises with what to do when it's missing or malformed."""
    name = settings_variable(slug, environment)
    if not raw:
        raise RuntimeError(f"The Airflow Variable {name} isn't set. In Tekspaz, turn on \"Run on your Airflow\" "
                           f"for {environment} (TeksInfra > Deployments): it writes the settings there.")
    malformed = f"The Airflow Variable {name} isn't what Tekspaz writes. Turn \"Run on your Airflow\" off and on again."
    try:
        env = json.loads(raw)["env"]
    except (ValueError, KeyError, TypeError) as error:
        raise RuntimeError(malformed) from error
    if not isinstance(env, dict):
        raise RuntimeError(malformed)
    return {str(k): str(v) for k, v in env.items() if SETTING_NAME.match(str(k))}


def step_env(settings: Mapping[str, str], environment: str, work: Path, venv: Path, context: Mapping[str, str],
             environ: Mapping[str, str] = os.environ) -> Dict[str, str]:
    """What project code sees: its settings, where things are, and the run's ids for the query tags."""
    env = {k: environ[k] for k in _PASSED_THROUGH if k in environ}
    env.update(settings)
    env.update({
        "PATH": f"{venv / 'bin'}:/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": str(work / "python"),
        "DBT_TARGET": "prod" if environment == "production" else "dev",
        "DBT_PROFILES_DIR": ".",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
    })
    env.setdefault("HOME", str(work))
    # dbt's query_tag and InfraOps trace a query back to this task through these.
    env.update({k: str(v) for k, v in context.items() if k.startswith("AIRFLOW_CTX_")})
    return env


def requirements_key(root: Path) -> str:
    digest = hashlib.sha256((root / "requirements.txt").read_bytes())
    digest.update(f"{sys.version_info.major}.{sys.version_info.minor}".encode())
    return digest.hexdigest()[:16]


@contextlib.contextmanager
def _locked(path: Path) -> Iterator[None]:
    import fcntl  # Airflow workers are Linux

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def ensure_venv(root: Path, cache: Path, run: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> Path:
    """The project's packages in a virtualenv of their own, made once per requirements.txt on each worker."""
    venv = cache / f"venv-{requirements_key(root)}"
    with _locked(cache / f"{venv.name}.lock"):
        if (venv / ".ready").exists():
            return venv
        shutil.rmtree(venv, ignore_errors=True)
        try:
            run([sys.executable, "-m", "venv", str(venv)], check=True)
            run([str(venv / "bin" / "pip"), "install", "-q", "-r", str(root / "requirements.txt")], check=True)
        except BaseException:
            shutil.rmtree(venv, ignore_errors=True)
            raise
        (venv / ".ready").write_text("ok")
    return venv


def work_copy(root: Path, base: Path, run_id: str, task_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", f"{run_id}__{task_id}")
    work = base / safe
    shutil.rmtree(work, ignore_errors=True)
    shutil.copytree(root, work, ignore=_IGNORED)
    return work


def run_step(command_key: str, environment: str, root: Path, settings: Mapping[str, str], context: Mapping[str, str],
             run_id: str, task_id: str, log: Callable[[str], None] = print,
             environ: Mapping[str, str] = os.environ) -> None:
    """Runs one step in a fresh copy of the project; raises RuntimeError on a non-zero exit."""
    cache = Path(environ.get("TEKSPAZ_CACHE_DIR") or Path(tempfile.gettempdir()) / "tekspaz")
    venv = ensure_venv(root, cache)
    work = work_copy(root, cache / "runs", run_id, task_id)
    try:
        process = subprocess.Popen(
            ["bash", "-c", f"set -euo pipefail; {STEP_COMMANDS[command_key]}"],
            cwd=work,
            env=step_env(settings, environment, work, venv, context, environ),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        )
        assert process.stdout is not None
        for line in process.stdout:
            log(line.rstrip("\n"))
        code = process.wait()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if code != 0:
        raise RuntimeError(f"{task_id} exited {code}")
