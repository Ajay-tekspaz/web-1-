"""
Load generator output into Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX: PUT to an internal stage, then COPY INTO.

  * COPY ... MATCH_BY_COLUMN_NAME with ENABLE_SCHEMA_EVOLUTION, so a renamed upstream
    column becomes a new column (same behaviour as the DuckDB stand-in)
  * Snowflake's load metadata skips files already loaded, so re-runs don't duplicate rows
  * Dry run by default: prints the plan. Pass --apply to execute.

Since K-15 the default ingestion is Airbyte; this loader is the documented fallback
(docs/ingestion.md). In Airflow it runs through TrackedSnowflakeHook (see
airflow/include/lakehouse_tasks.py); standalone it uses key-pair auth from the environment.

Usage:
    python -m loaders.snowflake_loader --data_dir data/generated            # dry run
    python -m loaders.snowflake_loader --data_dir data/generated --apply    # load
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from lakehouse.raw_tables import RAW_SCHEMA, RAW_TABLES, RawTable

STAGE = f"{RAW_SCHEMA}.RAW_STAGE"
FILE_FORMAT = f"{RAW_SCHEMA}.CSV_WITH_HEADER"


def _stage_dir(table: RawTable) -> str:
    return table.file_glob.split("/")[0] if "/" in table.file_glob else table.file_glob.rsplit(".", 1)[0]


def plan(data_dir: Path, full_refresh: bool = False) -> list[str]:
    """Every statement the load runs, in order. full_refresh replaces each raw table with exactly the files given
    (a source extracted in full each run): it empties the table and its staged files first, and forces the COPY."""
    data_dir = Path(data_dir).resolve()
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}",
        f"CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT} TYPE = CSV PARSE_HEADER = TRUE "
        "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' EMPTY_FIELD_AS_NULL = TRUE "
        # files have fewer columns than the table (_LOADED_AT, _SOURCE_FILE, evolved columns)
        "ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE",
        f"CREATE STAGE IF NOT EXISTS {STAGE} FILE_FORMAT = {FILE_FORMAT}",
    ]
    for table in RAW_TABLES:
        cols = ",\n  ".join(f"{c} {t}" for c, t in table.columns.items())
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{table.name} (\n  {cols},\n"
            "  _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),\n  _SOURCE_FILE VARCHAR\n"
            ") ENABLE_SCHEMA_EVOLUTION = TRUE"
        )
        local = (data_dir / table.file_glob).as_posix()
        if full_refresh:
            statements.append(f"REMOVE @{STAGE}/{_stage_dir(table)}/")
            statements.append(f"TRUNCATE TABLE {RAW_SCHEMA}.{table.name}")
        statements.append(f"PUT 'file://{local}' @{STAGE}/{_stage_dir(table)}/ AUTO_COMPRESS = TRUE OVERWRITE = FALSE")
        statements.append(
            f"COPY INTO {RAW_SCHEMA}.{table.name} FROM @{STAGE}/{_stage_dir(table)}/ "
            f"FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT}) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
            # COPY doesn't apply column defaults to unmatched columns, so set the load time explicitly
            "INCLUDE_METADATA = (_SOURCE_FILE = METADATA$FILENAME, _LOADED_AT = METADATA$START_SCAN_TIME) "
            "ON_ERROR = ABORT_STATEMENT" + (" FORCE = TRUE" if full_refresh else "")
        )
    return statements


def row_count_sql() -> list[tuple[str, str]]:
    return [(t.name, f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{t.name}") for t in RAW_TABLES]


def load(
    data_dir: Path, execute: Callable[[str], Any], fetch_one: Callable[[str], Any], full_refresh: bool = False,
) -> dict[str, int]:
    """Run the plan with the given executor (a cursor, or TrackedSnowflakeHook.run). Returns row counts."""
    for stmt in plan(data_dir, full_refresh):
        execute(stmt)
    return {name: int(fetch_one(sql)[0]) for name, sql in row_count_sql()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load generated CSVs into Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX")
    parser.add_argument("--data_dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--apply", action="store_true", help="execute against Snowflake (default: dry run)")
    parser.add_argument("--full_refresh", action="store_true",
                        help="replace each raw table with these files (data extracted in full from a source)")
    args = parser.parse_args(argv)

    if not args.apply:
        print("-- DRY RUN: nothing executed. Re-run with --apply to load DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX.\n")
        for stmt in plan(args.data_dir, args.full_refresh):
            print(stmt + ";\n")
        return 0

    import snowflake.connector

    from lakehouse.snowflake_auth import connection_kwargs

    with snowflake.connector.connect(**connection_kwargs(), session_parameters={
        "QUERY_TAG": '{"project":"daily-expense-tracker-fintech-application","environment":"dev","tool":"python-loader"}',
    }) as conn:
        cur = conn.cursor()

        def fetch_one(sql: str) -> Any:
            cur.execute(sql)
            return cur.fetchone()

        counts = load(args.data_dir, cur.execute, fetch_one, args.full_refresh)
    for name, n in counts.items():
        print(f"{name:<16} {n:>9,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
