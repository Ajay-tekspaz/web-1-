# Daily Expense Tracker Fintech Application Reference Project

A reference data project: synthetic retail data → raw load → dbt (staging, gold marts) → Holt-Winters demand forecast → BI dashboards, orchestrated by Airflow and monitored by InfraOps and TeksOptima.


**Owner:** @KaviyaSree2703 · **Reviewer:** @Giveson-Tekspaz · Rules for people and AI assistants: [AGENTS.md](AGENTS.md)

## How the data flows

```
generator (K-10) ──► raw DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX ──► dbt STAGING (views) ──► dbt GOLD (marts) ──► Superset
  seeded CSVs        Airbyte / Python     de-dup, repair,         fct_sales_daily       Sales performance
  + known defects    loader (K-15)        drop bad rows           fct_inventory_position Inventory health
                                                                  fct_demand_forecast ◄── Holt-Winters (K-16)
                     └──────────────── Airflow DAG (K-12): every Snowflake call via TrackedSnowflakeHook ───┘
```

Two warehouses run the same dbt project:

| Target | What it is | Needs |
|---|---|---|
| `local` | DuckDB file `data/lakehouse.duckdb`, a stand-in for Snowflake | Nothing; runs on a laptop and in CI |
| `dev` | Snowflake `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV`, role `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE`, warehouse `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS` | The `RETAIL_DEV_SVC` key pair |

There is no production target. One will be added only after written sign-off.

## Run it yourself (local, about 5 minutes)

Requirements: Python 3.10–3.12, git. No accounts or Docker needed.

```bash
git clone https://github.com/skilled-coderAI/daily-expense-tracker-fintech-application.git
cd daily-expense-tracker-fintech-application
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pip install -e . --no-deps

python scripts/run_pipeline.py       # generate -> load -> dbt build -> forecast -> dbt build
pytest tests                         # unit tests
```

Expected output, in this order:

1. The generator writes 290,990 orders, 498,660 order lines and 91,000 inventory rows to `data/generated/`.
2. `dbt build` finishes with `PASS=70 WARN=4 ERROR=0`. The 4 warnings are the planted defects ([docs/data-ground-truth.md](docs/data-ground-truth.md)).
3. The forecast reports a holdout MAPE of about 18% (target under 30%), and the last `dbt build` finishes with `PASS=12`.

`python scripts/run_pipeline.py --small --incremental_check` is the CI version. It uses less data and also proves the incremental marts pick up late-arriving rows. Run evidence (`run_results_*.json`, `forecast_metrics.json`) is written to `data/artifacts/`.

To explore the result:

```bash
cd dbt && dbt docs generate && dbt docs serve          # lineage graph and column docs at http://localhost:8080
python -c "import duckdb; print(duckdb.connect('data/lakehouse.duckdb').sql('select * from gold.rpt_sales_daily limit 10'))"
```

Dashboards: see [superset/README.md](superset/README.md).

## Run it on Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX

@Giveson-Tekspaz provides the `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE` role and the `RETAIL_DEV_SVC` key pair. Never use a password, and never commit the key.
Migration 001 creates a resource monitor, so an admin applies the migrations once with `SNOWFLAKE_ROLE=ACCOUNTADMIN`;
everything after that runs as `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE`.

```bash
export SNOWFLAKE_ACCOUNT=<account>  SNOWFLAKE_USER=RETAIL_DEV_SVC   # role DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE, warehouse DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS (the defaults)
export SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_dev_sandbox.p8   # or SNOWFLAKE_PRIVATE_KEY=<PEM text>
export DBT_TARGET=dev DBT_PROFILES_DIR=$PWD/dbt

python snowflake/apply_migrations.py                    # dry run: shows the warehouse, schemas, BI role
SNOWFLAKE_MIGRATION_ROLE=ACCOUNTADMIN python snowflake/apply_migrations.py --apply   # admin, once: role, 10-credit monitor, XS warehouse (60 s), tags
python python/data_generation/deterministic_generator.py --output_path data/generated
python -m loaders.snowflake_loader --data_dir data/generated            # dry run
python -m loaders.snowflake_loader --data_dir data/generated --apply    # PUT + COPY INTO DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX
cd dbt && dbt deps && dbt build --exclude tag:forecast && cd ..
python -m forecasts.demand_forecast --target snowflake --apply
cd dbt && dbt build --select tag:forecast
```

Every dbt query carries a JSON `QUERY_TAG` (project, environment, dbt `invocation_id` and `node_id`, plus the Airflow `dag_id`/`task_id`/`run_id`/`try_number` when Airflow runs it). [snowflake/scripts/query_tags_for_run.sql](snowflake/scripts/query_tags_for_run.sql) lists one run's queries from `QUERY_HISTORY`.

## CI

| Workflow | When | What |
|---|---|---|
| **dbt Lint & Validate** | every PR | flake8, mypy, pytest, sqlfluff, `dbt parse` |
| **dbt Test** | every PR | Full pipeline on DuckDB with an incremental re-run. Posts a PR comment with the pass/warn/error counts and the forecast MAPE, and uploads `run_results` as an artifact. Snowflake build on demand. |
| **Orchestration & Template** | every PR | Airflow DAG import test, Airbyte Terraform validate |
| **dbt Deploy** | manual only | `dev` only. Dry run by default; `apply` builds in DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX |



## Repository layout

```
python/data_generation/    seeded synthetic data (K-10)            docs/data.md, docs/data-ground-truth.md
python/loaders/            DuckDB + Snowflake raw loaders
python/forecasts/          Holt-Winters demand forecast (K-16)
python/lakehouse/          raw-table contract, Snowflake key-pair auth
dbt/                       staging -> gold marts -> forecast; profiles.yml (local + dev)
snowflake/                 idempotent migrations (warehouse, schemas, BI role) + apply script
airflow/                   DAG, task helpers, docker-compose (K-12)
superset/                  dashboards as YAML + import script + docker-compose (K-13)
ingestion/                 Airbyte OSS: Postgres source, connection as Terraform (K-15)
scripts/run_pipeline.py    the whole pipeline in one command
```



## Governance

Every cloud resource carries the tags `project: daily-expense-tracker-fintech-application`, `environment: dev`, `cost-center: engineering` and `owner: data-platform`.

