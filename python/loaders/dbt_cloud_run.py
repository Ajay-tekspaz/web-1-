"""
dbt on the customer's own dbt Cloud: run one of their jobs with this pipeline's dbt command, and wait for it.

Where the customer runs dbt in dbt Cloud, their job (a project on this repository, with its Snowflake
connection) does the dbt part of the pipeline instead of dbt Core here: the raw data and the forecast still run
in the pipeline, and each dbt step becomes a run of that job with the step's command (`steps_override`). The run's
run_results.json is saved where dbt Core would have written it, so the checks that read it still work.

The platform that deploys a project writes into the CI environment (and both Airflow runners pass the same):
    DBT_CLOUD_ACCOUNT_ID   the dbt Cloud account
    DBT_CLOUD_JOB_ID       the job to run
    DBT_CLOUD_TOKEN        a service token allowed to run it (the Job Runner permission, or Job Admin)
    DBT_CLOUD_HOST         cloud.getdbt.com (the default), emea.dbt.com, au.dbt.com, or the account's access URL
    DBT_CLOUD_GIT_BRANCH   run this branch instead of the job environment's (the dev build sets it; production
                           never does: it runs what the job's environment is set to, the default branch)
    DBT_CLOUD_TIMEOUT_MINUTES   how long to wait (default 120)

Standard library only.

Usage:
    python -m loaders.dbt_cloud_run --step "dbt build --exclude tag:forecast" --run-results dbt/target/run_results.json
    python -m loaders.dbt_cloud_run --check    # only check the token and the job
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_HOST = "cloud.getdbt.com"
QUEUED, STARTING, RUNNING, SUCCESS, ERROR, CANCELLED = 1, 2, 3, 10, 20, 30
STATE = {
    QUEUED: "queued", STARTING: "starting", RUNNING: "running",
    SUCCESS: "succeeded", ERROR: "failed", CANCELLED: "cancelled",
}
POLL_SECONDS = 15
LOG_LINES = 40

# (method, url, headers, body) -> (status, parsed JSON or None)
Http = Callable[[str, str, Mapping[str, str], Any], "tuple[int, Any]"]


class DbtCloudError(Exception):
    """The job couldn't start or didn't succeed; the message says why, without the token."""


@dataclass(frozen=True)
class Settings:
    host: str
    account_id: str
    job_id: str
    token: str
    git_branch: str | None = None
    timeout_minutes: float = 120

    @property
    def api(self) -> str:
        return f"https://{self.host}/api/v2/accounts/{self.account_id}"

    @staticmethod
    def from_env(env: Mapping[str, str] | None = None) -> "Settings | None":
        """None when no dbt Cloud job is configured: the pipeline then runs dbt Core itself."""
        env = os.environ if env is None else env
        job_id = (env.get("DBT_CLOUD_JOB_ID") or "").strip()
        if not job_id:
            return None
        account_id = (env.get("DBT_CLOUD_ACCOUNT_ID") or "").strip()
        token = (env.get("DBT_CLOUD_TOKEN") or "").strip()
        if not account_id or not token:
            raise DbtCloudError("DBT_CLOUD_JOB_ID is set, but DBT_CLOUD_ACCOUNT_ID or DBT_CLOUD_TOKEN isn't.")
        if not (job_id.isdigit() and account_id.isdigit()):
            raise DbtCloudError("DBT_CLOUD_ACCOUNT_ID and DBT_CLOUD_JOB_ID are numbers.")
        host = re.sub(r"^https?://", "", (env.get("DBT_CLOUD_HOST") or "").strip()).rstrip("/") or DEFAULT_HOST
        if not re.fullmatch(r"[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?", host):
            raise DbtCloudError("DBT_CLOUD_HOST is a host name, like cloud.getdbt.com or ab123.us1.dbt.com.")
        branch = (env.get("DBT_CLOUD_GIT_BRANCH") or "").strip() or None
        minutes = float(env.get("DBT_CLOUD_TIMEOUT_MINUTES") or 120)
        return Settings(host, account_id, job_id, token, branch, minutes)


def urllib_http(method: str, url: str, headers: Mapping[str, str], body: Any) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    content = {"Content-Type": "application/json"} if data else {}
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Accept": "application/json", **content, **headers}
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            return error.code, json.loads(raw) if raw else None
        except ValueError:
            return error.code, None


def _detail(payload: Any) -> str:
    if isinstance(payload, dict):
        status = payload.get("status")
        if isinstance(status, dict) and isinstance(status.get("user_message"), str):
            return status["user_message"][:300]
        for key in ("detail", "message"):
            if isinstance(payload.get(key), str):
                return payload[key][:300]
    return ""


def _cause(env: Mapping[str, str]) -> str:
    """What dbt Cloud shows as the run's reason: which pipeline run started it."""
    if env.get("GITHUB_RUN_ID"):
        where = f"GitHub Actions {env.get('GITHUB_WORKFLOW', '')} run {env['GITHUB_RUN_ID']}".replace("  ", " ")
    elif env.get("AIRFLOW_CTX_DAG_RUN_ID"):
        where = f"Airflow {env.get('AIRFLOW_CTX_DAG_ID', '')} {env['AIRFLOW_CTX_DAG_RUN_ID']}"
    else:
        where = "the pipeline"
    return f"Tekspaz: {where}"[:255]


class DbtCloud:
    def __init__(self, settings: Settings, http: Http | None = None, clock: Callable[[], float] = time.monotonic):
        self.settings = settings
        self.http = http or urllib_http
        self.clock = clock

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        auth = {"Authorization": f"Token {self.settings.token}"}
        status, payload = self.http(method, f"{self.settings.api}{path}", auth, body)
        if status in (401, 403):
            refused = "the token was refused, or can't run this job (it needs the Job Runner or Job Admin permission)"
            raise DbtCloudError(f"dbt Cloud answered {status}: {refused}. {_detail(payload)}".strip())
        return status, payload

    def job(self) -> dict:
        status, payload = self.call("GET", f"/jobs/{self.settings.job_id}/")
        data = payload.get("data") if isinstance(payload, dict) else None
        if status == 404 or not data:
            raise DbtCloudError(f"dbt Cloud has no job {self.settings.job_id} in account {self.settings.account_id}.")
        if status != 200:
            raise DbtCloudError(f"dbt Cloud answered {status} for the job. {_detail(payload)}".strip())
        return data

    def start(self, steps: Sequence[str], cause: str) -> dict:
        body: dict[str, Any] = {"cause": cause}
        if steps:
            body["steps_override"] = list(steps)
        if self.settings.git_branch:
            body["git_branch"] = self.settings.git_branch
        status, payload = self.call("POST", f"/jobs/{self.settings.job_id}/run/", body)
        data = payload.get("data") if isinstance(payload, dict) else None
        if status not in (200, 201) or not isinstance(data, dict) or data.get("id") is None:
            raise DbtCloudError(f"dbt Cloud didn't start the job ({status}). {_detail(payload)}".strip())
        return data

    def run(self, run_id: int) -> dict:
        status, payload = self.call("GET", f'/runs/{run_id}/?include_related=["run_steps"]')
        data = payload.get("data") if isinstance(payload, dict) else None
        if status != 200 or not isinstance(data, dict):
            raise DbtCloudError(f"dbt Cloud answered {status} for run {run_id}. {_detail(payload)}".strip())
        return data

    def wait(
        self, run_id: int, sleep: Callable[[float], None] = time.sleep, log: Callable[[str], None] = print
    ) -> dict:
        deadline = self.clock() + self.settings.timeout_minutes * 60
        last = None
        while True:
            data = self.run(run_id)
            state = data.get("status")
            if state != last:
                log(f"dbt Cloud run {run_id}: {STATE.get(state, state)}")
                last = state
            if state == SUCCESS:
                return data
            if state in (ERROR, CANCELLED):
                raise DbtCloudError(self.failure(run_id, data))
            if self.clock() > deadline:
                raise DbtCloudError(
                    f"The dbt Cloud run {run_id} was still {STATE.get(state, state)} after "
                    f"{self.settings.timeout_minutes:g} minutes. It goes on in dbt Cloud; this run stops waiting."
                )
            sleep(POLL_SECONDS)

    def failure(self, run_id: int, data: dict) -> str:
        """Which step failed, with the end of its log (dbt's own output names the failing model or test)."""
        steps = data.get("run_steps") or []
        failed = next((s for s in steps if s.get("status") in (ERROR, CANCELLED)), None)
        where = f" at '{failed.get('name')}'" if failed else ""
        ended = STATE.get(data.get("status"), "ended")
        message = f"The dbt Cloud run {run_id} {ended}{where}. {data.get('href') or ''}"
        logs = (failed or {}).get("logs") or ""
        tail = "\n".join(logs.strip().splitlines()[-LOG_LINES:])
        return f"{message.strip()}\n{tail}" if tail else message.strip()

    def save_run_results(self, run_id: int, path: Path) -> bool:
        """The last step's run_results.json, where dbt Core writes it. False when dbt Cloud has none."""
        status, payload = self.call("GET", f"/runs/{run_id}/artifacts/run_results.json")
        if status != 200 or not isinstance(payload, dict):
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return True


def run_job(
    settings: Settings,
    steps: Sequence[str],
    run_results: Path | None = None,
    env: Mapping[str, str] | None = None,
    http: Http = urllib_http,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
) -> dict:
    cloud = DbtCloud(settings, http, clock)
    started = cloud.start(steps, _cause(os.environ if env is None else env))
    run_id = int(started["id"])
    branch = f" on {settings.git_branch}" if settings.git_branch else ""
    log(f"Started dbt Cloud job {settings.job_id}{branch}: run {run_id} {started.get('href') or ''}".rstrip())
    try:
        return cloud.wait(run_id, sleep, log)
    finally:
        if run_results is not None and cloud.save_run_results(run_id, run_results):
            log(f"Saved the run's run_results.json to {run_results}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--step", action="append", default=[], help="a dbt command for the job to run (repeatable)")
    parser.add_argument("--run-results", type=Path, help="where to save the run's run_results.json")
    parser.add_argument("--check", action="store_true", help="only check the token and the job")
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        if settings is None:
            print("DBT_CLOUD_JOB_ID isn't set: dbt runs in the pipeline itself.", file=sys.stderr)
            return 2
        if args.check:
            job = DbtCloud(settings).job()
            print(f"dbt Cloud job {settings.job_id}: {job.get('name')} (environment {job.get('environment_id')}).")
            return 0
        if not args.step:
            parser.error("give the dbt command with --step")
        run_job(settings, args.step, args.run_results)
        return 0
    except DbtCloudError as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
