# Airflow orchestration (K-12)

> **Running this project on your own Airflow?** Use `dags/tekspaz_daily.py` instead, and see [docs/your-airflow.md](../docs/your-airflow.md). The DAG below is the template's own demo: it needs infraops (private) and the Airbyte provider.

One DAG, `daily_expense_tracker_fintech_application_orchestration`, runs daily at 02:00:

```
generate_data ─► ingest (K-15: Airbyte, or the Python loader) ─► verify_raw ─► dbt_build_marts ─► forecast ─► dbt_build_forecast
```

This project is InfraOps' first real tenant, so every query must be traceable:

| Who talks to Snowflake | How it's traced |
|---|---|
| `load_raw`, `verify_raw`, `forecast` (the DAG's own calls) | `infraops.telemetry.tracked_hook.TrackedSnowflakeHook`. Each statement gets `QUERY_TAG = {"dag_id","task_id","run_id","try_number"}` (Path A). Its id is logged as `Snowflake query id: <id>` and pushed to XCom `snowflake_query_ids`, also when the statement fails. |
| `dbt_build_*` (dbt opens its own connection) | The task exports `AIRFLOW_CTX_DAG_ID/TASK_ID/DAG_RUN_ID/TRY_NUMBER`, and `dbt_project.yml`'s `+query_tag` puts the same four keys plus `invocation_id` and `node_id` into every query. After the run, the task reads `run_results.json` and logs/pushes every query id the same way as the hook. For failed nodes it takes the id from the error message, because dbt leaves `adapter_response` empty for compile errors. |

The task logic lives in [include/lakehouse_tasks.py](include/lakehouse_tasks.py), which is plain Python and unit-tested without Airflow. The DAG file wires it to the hook.

## Run it locally (Docker)

```bash
git clone https://github.com/skilled-coderAI/infraops.git ../infraops   # private; provides TrackedSnowflakeHook
cp airflow/.env.example airflow/.env                                    # LAKEHOUSE_TARGET=local needs no account
docker compose -f airflow/docker-compose.yml up --build
```

Open http://localhost:8080. The user is `admin`; the password is in the container log (`standalone_admin_password.txt`). Unpause and trigger `daily_expense_tracker_fintech_application_orchestration`.

- `LAKEHOUSE_TARGET=local` runs every task on the DuckDB stand-in (`data/lakehouse.duckdb`), the same file Superset reads.
- `LAKEHOUSE_TARGET=dev` runs against Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX. Put the key pair in `.secrets/dev_sandbox.p8` and fill in the Snowflake lines in `.env`.

## Tests

- `airflow/tests/test_lakehouse_tasks.py`: query-id extraction (success, and the compile-time failure fixture), Airflow context export, hook usage. Runs everywhere.
- `airflow/tests/test_daily_expense_tracker_fintech_application_dag.py`: DagBag import, task order, and that the DAG builds a `TrackedSnowflakeHook` whose tag matches infraops' `build_query_tag`. Needs Airflow and infraops. The **Orchestration & Template** workflow installs Airflow 2.10.5 and checks out infraops with the `INFRAOPS_READ_TOKEN` secret.

