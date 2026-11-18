"""
Superset configuration for the daily-expense-tracker-fintech-application BI layer (K-13).

Used by docker-compose (mounted at /app/pythonpath) and by a local pip install
(SUPERSET_CONFIG_PATH=superset/superset_config.py).
"""

import os

# Metadata DB: SQLite in SUPERSET_HOME is enough for one analyst; use Postgres for a team.
SUPERSET_HOME = os.environ.get("SUPERSET_HOME", os.path.join(os.path.dirname(__file__), "superset_home"))
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_METADATA_DB_URI", "sqlite:///" + os.path.join(SUPERSET_HOME, "superset.db").replace("\\", "/")
)

# Must be set to a long random value outside local dev (openssl rand -base64 42)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "local-dev-only-change-me")

ROW_LIMIT = 50000
FEATURE_FLAGS = {"DASHBOARD_RBAC": True}

# Cost attribution: every query Superset sends is prefixed with a comment naming the tool,
# in addition to the Snowflake session QUERY_TAG set on the database connection
# (superset/assets/databases/*.yaml -> extra.engine_params.connect_args.session_parameters).


def SQL_QUERY_MUTATOR(sql, **kwargs):  # noqa: N802 (Superset's config name)
    return '/* {"tool":"superset","project":"daily-expense-tracker-fintech-application"} */\n' + sql
