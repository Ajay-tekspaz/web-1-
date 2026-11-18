"""
Task logic for the daily-expense-tracker-fintech-application DAG (K-12). Plain Python: no Airflow import, so it is
unit-tested without an Airflow install (airflow/tests/test_lakehouse_tasks.py).

Snowflake access is always through a hook object passed in by the DAG
(infraops' TrackedSnowflakeHook); dbt runs as a subprocess with the Airflow context exported
so its queries carry the same dag_id/task_id/run_id/try_number tag.

LAKEHOUSE_TARGET=local runs every step on the DuckDB stand-in instead of Snowflake.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

LAKEHOUSE_HOME = Path(os.environ.get("LAKEHOUSE_HOME", Path(__file__).resolve().parents[2]))
DATA_DIR = LAKEHOUSE_HOME / "data" / "generated"
DUCKDB_PATH = LAKEHOUSE_HOME / "data" / "lakehouse.duckdb"
DBT_DIR = LAKEHOUSE_HOME / "dbt"
XCOM_KEY = "snowflake_query_ids"  # same key TrackedSnowflakeHook uses

# Same pattern as infraops.telemetry.tracked_hook.SFQID_REGEX
SFQID_REGEX = re.compile(r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b", re.IGNORECASE)

HookFactory = Callable[[dict], Any]

for p in (LAKEHOUSE_HOME / "python",):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def target() -> str:
    """'dev' (Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX) or 'local' (DuckDB)."""
    value = os.environ.get("LAKEHOUSE_TARGET", "dev")
    if value not in ("dev", "local"):
        raise ValueError(f"LAKEHOUSE_TARGET must be dev or local, not {value!r}")
    return value


def airflow_ctx_env(context: dict) -> dict[str, str]:
    """AIRFLOW_CTX_* variables for a subprocess, from the task instance (Path A keys)."""
    ti = context["ti"]
    return {
        "AIRFLOW_CTX_DAG_ID": str(ti.dag_id),
        "AIRFLOW_CTX_TASK_ID": str(ti.task_id),
        "AIRFLOW_CTX_DAG_RUN_ID": str(ti.run_id),
        "AIRFLOW_CTX_TRY_NUMBER": str(ti.try_number),
    }


# ------------------------------------------------------------------- ingest
def ingestion_mode() -> str:
    """K-15: Airbyte is the default ingestion; the Python loader is the fallback.

    The local DuckDB target always uses the Python loader (Airbyte's destination is Snowflake).
    """
    mode = os.environ.get("INGESTION_MODE", "airbyte")
    if mode not in ("airbyte", "python"):
        raise ValueError(f"INGESTION_MODE must be airbyte or python, not {mode!r}")
    return "python" if target() == "local" else mode


def choose_ingestion(**context: Any) -> str:
    """Branch: task id to run next."""
    return "seed_source_postgres" if ingestion_mode() == "airbyte" else "load_raw"


def seed_source_postgres(**context: Any) -> dict:
    """Refresh the Airbyte source (the retail chain's operational Postgres) from the generator files."""
    from loaders import postgres_seeder

    counts = postgres_seeder.seed(DATA_DIR, os.environ["SOURCE_POSTGRES_DSN"])
    log.info("source postgres row counts %s", json.dumps(counts))
    return counts


def generate_data(**context: Any) -> dict:
    """The generator stands in for the source systems (POS, warehouse, product master)."""
    from data_generation.deterministic_generator import GeneratorConfig, generate

    if DATA_DIR.exists():  # never mix daily files from an earlier run into this one
        shutil.rmtree(DATA_DIR)
    truth = generate(GeneratorConfig(), DATA_DIR)
    log.info("generated %s", json.dumps(truth.row_counts))
    return truth.row_counts


def load_raw_python(hook_factory: HookFactory, **context: Any) -> dict:
    """Python loader (the fallback ingestion since K-15)."""
    if target() == "local":
        from loaders import duckdb_loader

        counts = duckdb_loader.load(DATA_DIR, DUCKDB_PATH)
    else:
        from loaders import snowflake_loader

        hook = hook_factory(context)
        counts = snowflake_loader.load(
            DATA_DIR,
            execute=lambda sql: hook.run(sql),
            fetch_one=lambda sql: hook.run(sql, handler=lambda cur: cur.fetchone()),
        )
    log.info("raw row counts %s", json.dumps(counts))
    return counts


def verify_raw(hook_factory: HookFactory, **context: Any) -> dict:
    """Fail fast if any raw table is empty."""
    from lakehouse.raw_tables import RAW_SCHEMA, RAW_TABLES

    sql = {t.name: f"select count(*) from {RAW_SCHEMA}.{t.name}" for t in RAW_TABLES}
    if target() == "local":
        import duckdb

        with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
            counts = {name: con.execute(q).fetchone()[0] for name, q in sql.items()}
    else:
        hook = hook_factory(context)
        counts = {name: hook.run(q, handler=lambda cur: cur.fetchone())[0] for name, q in sql.items()}
    empty = [name for name, n in counts.items() if not n]
    if empty:
        raise RuntimeError(f"raw tables are empty: {empty}")
    return counts


# ---------------------------------------------------------------------- dbt
def query_ids_from_run_results(run_results: dict) -> dict[str, list[str]]:
    """node_id -> Snowflake query ids: adapter_response.query_id, else the id in the error message."""
    found: dict[str, list[str]] = {}
    for result in run_results.get("results", []):
        ids = []
        qid = (result.get("adapter_response") or {}).get("query_id")
        if qid:
            ids.append(qid)
        for match in SFQID_REGEX.findall(result.get("message") or ""):
            if match not in ids:
                ids.append(match)
        if ids:
            found[result["unique_id"]] = ids
    return found


def dbt_build(dbt_args: list[str], **context: Any) -> list[str]:
    """Run `dbt build`, log every Snowflake query id the way TrackedSnowflakeHook does, push them to XCom."""
    ti = context["ti"]
    env = {
        **os.environ,
        **airflow_ctx_env(context),
        "DBT_TARGET": target(),
        "DBT_PROFILES_DIR": str(DBT_DIR),
        "LAKEHOUSE_DUCKDB_PATH": str(DUCKDB_PATH),
    }
    dbt_exe = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt") or "dbt"
    if context.get("params", {}).get("break_the_build"):
        dbt_args = [*dbt_args, "--vars", '{"break_the_build": true}']
    if not (DBT_DIR / "dbt_packages").exists():  # git-ignored; a fresh checkout has none
        subprocess.run([dbt_exe, "deps"], cwd=DBT_DIR, env=env, check=True)
    # Remove the previous run's results, so a run that fails before writing them can't report stale ids
    run_results_path = DBT_DIR / "target" / "run_results.json"
    run_results_path.unlink(missing_ok=True)

    cmd = [dbt_exe, "build", *dbt_args]
    log.info("running %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    log.info(proc.stdout)
    if proc.stderr:
        log.warning(proc.stderr)

    by_node = query_ids_from_run_results(json.loads(run_results_path.read_text())) if run_results_path.exists() else {}
    all_ids = []
    for node, ids in by_node.items():
        for qid in ids:
            log.info("Snowflake query id: %s (%s)", qid, node)
            all_ids.append(qid)
    ti.xcom_push(key=XCOM_KEY, value=all_ids)

    if proc.returncode != 0:
        failed = [r for r in json.loads(run_results_path.read_text()).get("results", [])
                  if r.get("status") in ("error", "fail")] if run_results_path.exists() else []
        for r in failed:
            log.error("dbt node failed: %s query_ids=%s :: %s", r["unique_id"], by_node.get(r["unique_id"], []),
                      (r.get("message") or "").strip())
        raise RuntimeError(f"dbt build failed ({len(failed)} failed nodes, exit {proc.returncode})")
    return all_ids


# ----------------------------------------------------------------- forecast
def forecast(hook_factory: HookFactory, **context: Any) -> dict:
    from forecasts import demand_forecast as df

    cfg = df.ForecastConfig()
    if target() == "local":
        import duckdb

        with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
            sales = con.execute(df.SALES_SQL).df()
        output, metrics = df.run(sales, cfg)
        df.write_duckdb(output, DUCKDB_PATH)
    else:
        hook = hook_factory(context)
        sales = hook.run(df.SALES_SQL, handler=lambda cur: cur.fetch_pandas_all())
        sales.columns = [c.lower() for c in sales.columns]
        output, metrics = df.run(sales, cfg)
        df.write_snowflake(output, hook.run, hook.insert_rows)
    log.info("forecast metrics %s", json.dumps(metrics))
    if metrics["holdout_mape"] is not None and metrics["holdout_mape"] >= 0.30:
        log.warning("holdout MAPE %.1f%% is above the 30%% target", 100 * metrics["holdout_mape"])
    return metrics
