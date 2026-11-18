"""dbt on the customer's dbt Cloud (python/loaders/dbt_cloud_run.py), against a fake dbt Cloud API."""

import json

import pytest

from loaders import dbt_cloud_run as dc

ENV = {"DBT_CLOUD_ACCOUNT_ID": "12345", "DBT_CLOUD_JOB_ID": "678", "DBT_CLOUD_TOKEN": "dbtc_secret_value"}


class FakeCloud:
    """Answers like dbt Cloud's v2 API: a run goes queued -> running -> the given end state."""

    def __init__(self, end=dc.SUCCESS, steps=None, artifacts=True):
        self.calls = []
        self.states = [dc.QUEUED, dc.RUNNING, end]
        self.steps = steps or []
        self.artifacts = artifacts

    def __call__(self, method, url, headers, body):
        self.calls.append((method, url, dict(headers), body))
        assert headers["Authorization"] == "Token dbtc_secret_value"
        if method == "POST" and url.endswith("/jobs/678/run/"):
            return 200, {"data": {"id": 9001, "href": "https://cloud.getdbt.com/deploy/12345/projects/1/runs/9001/"}}
        if method == "GET" and "/runs/9001/?include_related" in url:
            state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
            href = "https://cloud.getdbt.com/runs/9001"
            return 200, {"data": {"id": 9001, "status": state, "href": href, "run_steps": self.steps}}
        if url.endswith("/runs/9001/artifacts/run_results.json"):
            results = {"results": [{"unique_id": "model.retail.fct_sales", "status": "success"}]}
            return (200, results) if self.artifacts else (404, None)
        if url.endswith("/jobs/678/"):
            return 200, {"data": {"id": 678, "name": "Tekspaz pipeline", "environment_id": 55}}
        return 404, {"status": {"user_message": "not found"}}


def test_no_job_means_dbt_runs_in_the_pipeline():
    assert dc.Settings.from_env({}) is None


def test_settings_are_checked_without_repeating_the_token():
    with pytest.raises(dc.DbtCloudError, match="DBT_CLOUD_TOKEN"):
        dc.Settings.from_env({"DBT_CLOUD_JOB_ID": "678", "DBT_CLOUD_ACCOUNT_ID": "12345"})
    with pytest.raises(dc.DbtCloudError, match="numbers"):
        dc.Settings.from_env({**ENV, "DBT_CLOUD_JOB_ID": "abc"})
    with pytest.raises(dc.DbtCloudError, match="host name"):
        dc.Settings.from_env({**ENV, "DBT_CLOUD_HOST": "evil.com/x?"})
    settings = dc.Settings.from_env({**ENV, "DBT_CLOUD_HOST": "https://ab123.us1.dbt.com/"})
    assert settings.api == "https://ab123.us1.dbt.com/api/v2/accounts/12345"
    assert dc.Settings.from_env(ENV).host == "cloud.getdbt.com"


def test_runs_the_job_with_the_steps_command_on_the_branch_and_saves_run_results(tmp_path):
    fake, logged = FakeCloud(), []
    settings = dc.Settings.from_env({**ENV, "DBT_CLOUD_GIT_BRANCH": "factory/p-v1"})
    out = tmp_path / "target" / "run_results.json"
    env = {"GITHUB_RUN_ID": "42", "GITHUB_WORKFLOW": "dbt Test"}
    steps = ["dbt build --exclude tag:forecast"]
    result = dc.run_job(settings, steps, out, env, fake, sleep=lambda s: None, log=logged.append)
    assert result["status"] == dc.SUCCESS
    method, url, _, body = fake.calls[0]
    assert (method, url) == ("POST", "https://cloud.getdbt.com/api/v2/accounts/12345/jobs/678/run/")
    assert body == {"cause": "Tekspaz: GitHub Actions dbt Test run 42", "steps_override": steps,
                    "git_branch": "factory/p-v1"}
    assert json.loads(out.read_text())["results"][0]["status"] == "success"
    assert any("queued" in line for line in logged) and any("succeeded" in line for line in logged)
    assert not any("dbtc_secret_value" in line for line in logged)


def test_production_runs_the_jobs_own_branch():
    fake = FakeCloud()
    dc.run_job(dc.Settings.from_env(ENV), ["dbt build"], None, {}, fake, sleep=lambda s: None, log=lambda m: None)
    assert "git_branch" not in fake.calls[0][3]


def test_a_failed_run_names_the_step_and_shows_the_end_of_its_log(tmp_path):
    steps = [
        {"name": "Invoke dbt with `dbt deps`", "status": dc.SUCCESS, "logs": "ok"},
        {"name": "Invoke dbt with `dbt build --exclude tag:forecast`", "status": dc.ERROR,
         "logs": "\n".join(f"line {i}" for i in range(100)) + "\nFailure in test not_null_fct_sales_order_id"},
    ]
    fake = FakeCloud(end=dc.ERROR, steps=steps)
    out = tmp_path / "run_results.json"
    with pytest.raises(dc.DbtCloudError) as error:
        dc.run_job(dc.Settings.from_env(ENV), ["dbt build"], out, {}, fake, sleep=lambda s: None, log=lambda m: None)
    message = str(error.value)
    assert "failed at 'Invoke dbt with `dbt build --exclude tag:forecast`'" in message
    assert "not_null_fct_sales_order_id" in message and "line 0\n" not in message
    assert out.exists()  # the failed run's results are kept too


def test_a_run_that_takes_too_long_stops_the_wait_not_the_job():
    fake = FakeCloud(end=dc.RUNNING)
    ticks = iter(range(0, 10_000, 600))
    settings = dc.Settings.from_env({**ENV, "DBT_CLOUD_TIMEOUT_MINUTES": "30"})
    with pytest.raises(dc.DbtCloudError, match="still running after 30 minutes"):
        dc.run_job(settings, ["dbt build"], None, {}, fake, sleep=lambda s: None, clock=lambda: next(ticks),
                   log=lambda m: None)


def test_a_refused_token_says_which_permission(monkeypatch):
    def refused(method, url, headers, body):
        return 403, {"status": {"user_message": "Forbidden"}}

    with pytest.raises(dc.DbtCloudError, match="Job Runner"):
        dc.DbtCloud(dc.Settings.from_env(ENV), refused).start(["dbt build"], "x")


def test_check_reads_the_job(monkeypatch, capsys):
    monkeypatch.setattr(dc, "urllib_http", FakeCloud())
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    assert dc.main(["--check"]) == 0
    assert "Tekspaz pipeline" in capsys.readouterr().out
