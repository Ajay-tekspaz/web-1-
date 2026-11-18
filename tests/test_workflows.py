"""Guardrails on the workflows: dev runs on a schedule, production only through its gate (AGENTS.md)."""

from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"


def load(name: str) -> dict:
    data = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    data["on"] = data.pop(True, data.get("on"))  # YAML reads a bare `on` as True
    return data


def test_every_workflow_parses():
    for path in WORKFLOWS.glob("*.yml"):
        assert isinstance(yaml.safe_load(path.read_text(encoding="utf-8")), dict), path.name


def test_production_runs_only_by_hand_behind_every_lock():
    wf = load("dbt-production.yml")
    assert set(wf["on"]) == {"workflow_dispatch"}, "no schedule, push or pull_request trigger"
    job = wf["jobs"]["production"]
    assert job["environment"] == "production"  # required reviewers live on this environment
    assert "vars.PRODUCTION_ENABLED == 'true'" in job["if"] and "inputs.confirm == 'PRODUCTION'" in job["if"]
    assert wf["on"]["workflow_dispatch"]["inputs"]["apply"]["default"] is False  # dry run unless ticked


def test_the_platform_writes_production_secrets_but_variables_still_work():
    env = load("dbt-production.yml")["jobs"]["production"]["env"]
    for name in ("SNOWFLAKE_ROLE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE"):
        assert env[name] == f"${{{{ secrets.{name} || vars.{name} }}}}", name


def test_the_production_nightly_only_rebuilds_approved_code_behind_its_locks():
    wf = load("dbt-production-nightly.yml")
    assert set(wf["on"]) == {"schedule"}, "scheduled only: no manual, push or pull_request trigger"
    job = wf["jobs"]["nightly"]
    locks = (
        "vars.PRODUCTION_ENABLED == 'true'",
        "vars.PRODUCTION_SCHEDULE == 'on'",
        "github.ref_name == github.event.repository.default_branch",
    )
    for lock in locks:
        assert lock in job["if"], lock
    # its own environment, which only the default branch may use; never the reviewed `production` one
    assert job["environment"] == "production-nightly"
    steps = {s.get("name"): s for s in job["steps"]}
    assert "*_DEV|*_DEV_*" in steps["Refuse a target that looks like dev"]["run"]
    gated = load("dbt-production.yml")
    assert wf["concurrency"]["group"] == gated["concurrency"]["group"], "never two production builds at once"


def test_nothing_but_the_gate_targets_production():
    for path in WORKFLOWS.glob("*.yml"):
        if path.name in ("dbt-production.yml", "dbt-production-nightly.yml"):
            continue
        text = path.read_text(encoding="utf-8")
        assert "DBT_TARGET: prod" not in text and "environment: production" not in text, path.name


def test_production_target_has_no_defaults():
    profiles = yaml.safe_load((WORKFLOWS.parents[1] / "dbt" / "profiles.yml").read_text(encoding="utf-8"))
    prod = profiles["daily_expense_tracker_fintech_application"]["outputs"]["prod"]
    for key in ("account", "user", "role", "database", "warehouse", "private_key"):
        value = prod[key]
        assert "env_var('" in value and "'," not in value, f"{key} must come from the production environment only"


def test_the_schedule_is_dev_only_and_off_until_switched_on():
    wf = load("dbt-scheduled.yml")
    assert wf["on"]["schedule"][0]["cron"]
    job = wf["jobs"]["nightly"]
    assert job["if"] == "vars.SNOWFLAKE_SCHEDULE == 'on'"
    assert job["uses"] == "./.github/workflows/snowflake-build.yml"


def test_the_shared_snowflake_build_is_dev_only_and_chooses_its_data_source():
    wf = load("snowflake-build.yml")
    assert "workflow_call" in wf["on"]
    job = wf["jobs"]["build"]
    assert job["environment"] == "dev" and job["env"]["DBT_TARGET"] == "dev"
    steps = {s["name"]: s for s in job["steps"] if "name" in s}
    # The customer's Airbyte wins, then their Postgres, then the generator: exactly one runs.
    assert steps["Raw data from the customer's Airbyte"]["if"] == "env.AIRBYTE_CONNECTION_ID != ''"
    assert steps["Raw data from the customer's Airbyte"]["run"] == "python -m loaders.airbyte_sync"
    no_airbyte = "env.AIRBYTE_CONNECTION_ID == ''"
    postgres = steps["Raw data from the customer's source Postgres"]
    generator = steps["Raw data from the generator (no source configured)"]
    assert postgres["if"] == f"{no_airbyte} && env.SOURCE_POSTGRES_HOST != ''"
    assert generator["if"] == f"{no_airbyte} && env.SOURCE_POSTGRES_HOST == ''"
    assert "--full_refresh" in postgres["run"]
    airbyte = ("AIRBYTE_CONNECTION_ID", "AIRBYTE_API_URL", "AIRBYTE_CLIENT_ID", "AIRBYTE_CLIENT_SECRET",
               "AIRBYTE_TOKEN")
    for name in airbyte:
        assert job["env"][name] == f"${{{{ secrets.{name} }}}}", name


def test_dbt_runs_in_the_pipeline_or_on_the_customers_dbt_cloud_never_both():
    """With DBT_CLOUD_JOB_ID their job runs each dbt step, else dbt Core here; production never picks the branch."""
    names = ("DBT_CLOUD_ACCOUNT_ID", "DBT_CLOUD_JOB_ID", "DBT_CLOUD_TOKEN", "DBT_CLOUD_HOST")
    build = load("snowflake-build.yml")["jobs"]["build"]
    production = load("dbt-production.yml")["jobs"]["production"]
    nightly = load("dbt-production-nightly.yml")["jobs"]["nightly"]
    for job in (build, production, nightly):
        for name in names:
            assert job["env"][name] == f"${{{{ secrets.{name} }}}}", name
        local = [s for s in job["steps"] if "dbt build" in s.get("run", "") and "dbt_cloud_run" not in s.get("run", "")
                 and "demand_forecast" not in s.get("run", "")]
        cloud = [s for s in job["steps"]
                 if "dbt_cloud_run" in s.get("run", "") and "demand_forecast" not in s.get("run", "")]
        assert local and cloud
        assert all("env.DBT_CLOUD_JOB_ID == ''" in s["if"] for s in local)
        assert all("env.DBT_CLOUD_JOB_ID != ''" in s["if"] for s in cloud)
    assert build["env"]["DBT_CLOUD_GIT_BRANCH"] == "${{ github.ref_name }}"
    assert "DBT_CLOUD_GIT_BRANCH" not in production["env"] and "DBT_CLOUD_GIT_BRANCH" not in nightly["env"]
    forecast = next(s for s in build["steps"] if s.get("name") == "Forecast + dbt build (forecast)")["run"]
    assert 'if [ -n "$DBT_CLOUD_JOB_ID" ]' in forecast and "dbt_cloud_run" in forecast


def test_the_manual_snowflake_run_uses_the_shared_build():
    job = load("dbt-test.yml")["jobs"]["build-snowflake-dev"]
    assert job["uses"] == "./.github/workflows/snowflake-build.yml" and job["secrets"] == "inherit"
    assert "inputs.snowflake" in job["if"]


def test_the_snowflake_connection_can_name_its_own_role_warehouse_and_database():
    env = load("snowflake-build.yml")["jobs"]["build"]["env"]
    # a secret from the dev environment wins; the stamped name is the fallback
    fallbacks = (
        ("SNOWFLAKE_ROLE", "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE"),
        ("SNOWFLAKE_WAREHOUSE", "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS"),
        ("SNOWFLAKE_DATABASE", "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV"),
    )
    for name, fallback in fallbacks:
        assert env[name] == f"${{{{ secrets.{name} || '{fallback}' }}}}", name
