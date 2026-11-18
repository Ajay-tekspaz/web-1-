# Superset dashboards on the gold layer (K-13)

Two dashboards, kept as code in [assets/](assets/) in Superset's own export format (YAML):

| Dashboard | Reads | Shows |
|---|---|---|
| **Sales performance** | `gold.rpt_sales_daily`, `gold.fct_demand_forecast` | Net revenue, units, margin and discount KPIs; daily revenue by region; weekly revenue by store; revenue by category; top products; daily sales by store × product; the 13-week forecast with its 80% band. Filters: date range, region, store, category. |
| **Inventory health** | `gold.rpt_inventory_daily` | Stockout rate, out-of-stock count, inventory at cost and units on hand (latest day); stockout rate by region over time; stock status mix; stockout days by store; at-risk items (< 3 days of cover). Filters: region, store, stock status. |

![Sales performance](../docs/screenshots/sales-performance.png)
![Inventory health](../docs/screenshots/inventory-health.png)

The screenshots were taken after importing `assets/` into an empty Superset 4.1.1 with `import_dashboards.py`. The data is the default K-10 dataset on the DuckDB stand-in.

## Run it

```bash
python scripts/run_pipeline.py                         # builds data/lakehouse.duckdb
docker compose -f superset/docker-compose.yml up --build
# http://localhost:8088  admin / admin
```

Without Docker, a pip install works too (tested on Windows with Python 3.11):

```bash
python -m venv .venv-superset && .venv-superset/Scripts/pip install "apache-superset==4.1.1" "duckdb~=1.1.0" "duckdb-engine==0.13.6" "marshmallow<4" "setuptools<81"
export SUPERSET_CONFIG_PATH=superset/superset_config.py FLASK_APP=superset
superset db upgrade && superset fab create-admin --username admin --password admin --firstname A --lastname B --email a@example.com && superset init
superset run -p 8088 &
python superset/import_dashboards.py --url http://localhost:8088 --duckdb-uri "duckdb:///$PWD/data/lakehouse.duckdb"
```

## On Snowflake

`snowflake/migrations/003_bi_readonly_role.sql` creates `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_READER` (SELECT on `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV.GOLD` only), the `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_XS` warehouse (XS, 60 s auto-suspend, tagged) and the `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SUPERSET` service user (key-pair only). Then:

```bash
SNOWFLAKE_ACCOUNT=<account> SUPERSET_SNOWFLAKE_PRIVATE_KEY_PATH=.secrets/superset_svc.p8 \
python superset/import_dashboards.py --url http://localhost:8088 --target snowflake
```

The connection sets the Snowflake session `QUERY_TAG` to `{"tool":"superset","project":"daily-expense-tracker-fintech-application","environment":"dev"}`, so TeksOptima can attribute BI cost to Superset. `superset_config.py` also prefixes every query with the same JSON as a comment. The private key is stored encrypted on the connection, never in the YAML.

## Changing a dashboard

Edit it in the UI, then use **Dashboards › select both › Export**, unzip the archive over `assets/`, and run `pytest tests/test_superset_assets.py`. The tests check that every layout slot points at an exported chart (by uuid), that only the gold schema is used, and that no local paths or secrets are in the YAML.
