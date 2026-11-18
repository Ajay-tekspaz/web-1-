#!/usr/bin/env bash
# First start: create the metadata DB and admin user, import the dashboards from superset/assets, then serve.
set -euo pipefail

superset db upgrade
superset fab create-admin --username "${SUPERSET_ADMIN_USER:-admin}" --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
  --firstname Admin --lastname User --email admin@example.com || true   # already exists on restart
superset init

python /app/repo/import_dashboards.py --zip-only /tmp/dashboards.zip --duckdb-uri "${DUCKDB_URI:-duckdb:////app/data/lakehouse.duckdb}"
superset import-dashboards --path /tmp/dashboards.zip --username "${SUPERSET_ADMIN_USER:-admin}"

exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 "superset.app:create_app()"
