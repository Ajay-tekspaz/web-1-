# Ingestion (K-15)

| | Default: Airbyte OSS | Fallback: Python loader |
|---|---|---|
| Path | generator → Postgres `daily_expense_tracker_fintech_application_source.retail.*` → **Airbyte** → Snowflake `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX.*_RAW` | generator files → `PUT` + `COPY INTO` → Snowflake `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX.*_RAW` |
| Code | [ingestion/postgres](../ingestion/postgres), [python/loaders/postgres_seeder.py](../python/loaders/postgres_seeder.py), [ingestion/airbyte/terraform](../ingestion/airbyte/terraform) | [python/loaders/snowflake_loader.py](../python/loaders/snowflake_loader.py) |
| In Airflow | `INGESTION_MODE=airbyte`: `seed_source_postgres` → `airbyte_sync` (`AirbyteTriggerSyncOperator`, waits for the job) | `INGESTION_MODE=python`: `load_raw` (through `TrackedSnowflakeHook`) |
| Use when | Normal runs; this is what BUILD generates for a customer | Airbyte is down, or a quick load from files |

Both land the same tables with the same columns. The seeder writes `_loaded_at` and `_source_file` into Postgres, so Airbyte carries them through. dbt doesn't care which path ran: loaders type columns differently (Airbyte's Snowflake destination writes every number as FLOAT), so the staging models cast each column to the raw contract's type, and the marts come out the same whichever loader filled the raw tables. The local DuckDB target always uses the Python path.

## Run Airbyte locally

Needs Docker and about 8 GB of free RAM for Airbyte's kind cluster.

```bash
# 1. Airbyte OSS
curl -LsfS https://get.airbyte.com | bash -        # installs abctl (Windows: download abctl from the Airbyte releases page)
abctl local install                                # UI + API on http://localhost:8000
abctl local credentials                            # client id/secret for the API and Terraform

# 2. The source Postgres, seeded by the generator
docker compose -f ingestion/postgres/docker-compose.yml up -d
python python/data_generation/deterministic_generator.py --output_path data/generated
python -m loaders.postgres_seeder --data_dir data/generated

# 3. Source, destination and connection, as code
cd ingestion/airbyte/terraform
cp terraform.tfvars.example terraform.tfvars       # fill in; git-ignored
terraform init && terraform plan && terraform apply
terraform output connection_id                     # -> AIRBYTE_CONNECTION_ID for Airflow
```

Then in `airflow/.env` set `INGESTION_MODE=airbyte`, `AIRBYTE_CONNECTION_ID` and `AIRFLOW_CONN_AIRBYTE_LOCAL`, and trigger the DAG.

## How the connection is configured

- **Source**: `source-postgres` 3.8.9, schema `retail`, user-defined cursor replication (`Standard`).
- **Destination**: `destination-snowflake` 5.2.0, role `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE`, warehouse `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS`, schema `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX`, key-pair auth for `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_AIRBYTE`. JDBC `query_tag={"tool":"airbyte",...}` lets TeksOptima attribute ingestion cost.
- **Streams**: `orders_raw`, `order_lines_raw` and `inventory_raw` use incremental append on `ingested_at`. This keeps raw faithful: the D2 duplicate and late arrivals still reach dbt. `stores_raw`, `products_raw` and `promotions_raw` use full-refresh overwrite.
- **Schema changes**: `propagate_columns`, so the D4 rename arrives as a new `ORDER_TS` column, exactly like the Python loader's schema evolution.

## Checked, and still to check

- Checked: `terraform validate` passes against provider `airbytehq/airbyte` 1.3 (it runs in CI, **Orchestration & Template**). Connector versions and config keys were taken from Airbyte's OSS registry spec. The seeder's SQL runs in tests against an attached DuckDB.
- Not yet run: `abctl` + `terraform apply` + a sync into DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX. This needs Docker and the Snowflake key pair. On the first sync, check that the Snowflake destination names the tables `ORDERS_RAW` etc. in upper case. dbt's raw source uses unquoted, case-insensitive names; if the tables come out lower case and quoted, set `quoting.identifier: true` on the `raw` source.

## Your own Airbyte (Cloud or self-hosted)

A customer who already runs Airbyte keeps it: their connection loads the raw tables, and the pipeline only starts it and waits. When the repository's `dev` environment has `AIRBYTE_CONNECTION_ID`, `.github/workflows/snowflake-build.yml` runs [python/loaders/airbyte_sync.py](../python/loaders/airbyte_sync.py) instead of the Postgres extractor or the generator: it starts a sync of that connection (or follows one already running), waits for it to succeed, and fails the run if it fails, is cancelled or takes longer than `AIRBYTE_SYNC_TIMEOUT_MINUTES` (180). dbt then builds on what the sync landed. Tekspaz's hosted Airflow does the same in its `raw_data` step.

**What the connection must load.** Its destination is this project's Snowflake database and raw schema (the `sandbox` parameter), with the tables named as the raw contract names them (`STORES_RAW`, `PRODUCTS_RAW`, `PROMOTIONS_RAW`, `ORDERS_RAW`, `ORDER_LINES_RAW`, `INVENTORY_RAW`) and the contract columns in [python/lakehouse/raw_tables.py](../python/lakehouse/raw_tables.py). The Terraform above sets up exactly that; a customer's existing connection can use a stream prefix or a namespace, or the source exposes views in that shape. The sync step doesn't check the tables; the dbt build that follows fails on a missing one and names it.

**Credentials.** An Airbyte application's client id and secret (in Airbyte, your user settings › Applications: click your name at the bottom left): `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET`, plus `AIRBYTE_API_URL` for a self-hosted Airbyte (`https://<host>/api/public/v1`; Cloud is the default). Airbyte's access tokens expire within minutes, so the step asks for a fresh one as it goes; `AIRBYTE_TOKEN` (an access token) works only for runs shorter than that. The Tekspaz platform writes them from the project's Airbyte connector; they are secrets of the `dev` environment. Check a connection without syncing:

```bash
python -m loaders.airbyte_sync --check
```

## Your own source (a customer's Postgres)

The Snowflake build can read a customer's operational Postgres instead of the generator. When the repository's `dev` environment has `SOURCE_POSTGRES_HOST`, `.github/workflows/snowflake-build.yml` runs [python/loaders/postgres_extractor.py](../python/loaders/postgres_extractor.py), which copies the source's tables into the same files the generator writes, then loads them with `snowflake_loader --full_refresh`. Otherwise it generates data as before. Nothing about dbt changes.

**What the source must look like.** Schema `retail` with one table per raw table (`stores_raw`, `products_raw`, `promotions_raw`, `orders_raw`, `order_lines_raw`, `inventory_raw`), each carrying the contract columns in [python/lakehouse/raw_tables.py](../python/lakehouse/raw_tables.py). Extra columns are kept (schema evolution), and `_loaded_at` / `_source_file` are ignored. A customer whose tables are named differently exposes views in that shape. Check a source without loading anything:

```bash
python -m loaders.postgres_extractor --data_dir data/generated --check
```

A missing table or column fails the run and names it.

**Credentials.** `SOURCE_POSTGRES_HOST`, `_PORT`, `_DATABASE`, `_USER`, `_PASSWORD` and `_SSLMODE` (default `require`) are secrets of the `dev` environment. The Tekspaz platform writes them from the project's PostgreSQL connector when the project is deployed to Snowflake; nothing else ever sees them. Give the Factory a read-only account.

**Full refresh.** Each run replaces the raw tables with the source's current rows (`TRUNCATE` and a forced `COPY`), so a nightly run does not pile up duplicates. This is the simple path for a source that is small enough to copy whole; a large source should use the Airbyte path above, which is incremental.
