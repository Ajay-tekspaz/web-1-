"""
Seed the Airbyte source: a Postgres database playing the retail chain's operational system (K-15).

Generator files -> Postgres schema `retail`, one table per raw table, written through DuckDB's
postgres extension with exactly the same file handling as the DuckDB loader (union by column
name, so the D4 renamed column arrives as its own column). Airbyte then replicates these tables
into Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX.

Usage (Postgres from ingestion/postgres/docker-compose.yml):
    python -m loaders.postgres_seeder --data_dir data/generated \
        --dsn "host=localhost port=5433 dbname=daily_expense_tracker_fintech_application_source user=retail password=retail"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

from lakehouse.raw_tables import RAW_TABLES
from loaders.duckdb_loader import read_csv_sql, table_files

SOURCE_SCHEMA = "retail"


def statements(schema: str = SOURCE_SCHEMA) -> list[tuple[str, str | None]]:
    """(sql, raw table name whose files bind to it) in execution order."""
    plan: list[tuple[str, str | None]] = [(f"create schema if not exists pg.{schema}", None)]
    for table in RAW_TABLES:
        name = table.name.lower()
        plan.append((f"drop table if exists pg.{schema}.{name}", None))
        plan.append((f"create table pg.{schema}.{name} as {read_csv_sql(table)}", table.name))
    return plan


def attach_sql(target: str, db_type: str = "postgres") -> str:
    """ATTACH can't take a bound parameter, so the target is inlined as an escaped SQL literal."""
    return f"attach '{target.replace(chr(39), chr(39) * 2)}' as pg (type {db_type})"


def seed(data_dir: Path, dsn: str, schema: str = SOURCE_SCHEMA, db_type: str = "postgres") -> dict[str, int]:
    """db_type='duckdb' attaches a DuckDB file instead (used by the tests)."""
    by_name = {t.name: t for t in RAW_TABLES}
    counts = {}
    with duckdb.connect() as con:
        if db_type == "postgres":
            con.execute("install postgres; load postgres;")
        con.execute(attach_sql(dsn, db_type))
        for sql, table_name in statements(schema):
            if table_name is None:
                con.execute(sql)
            else:
                con.execute(sql, [table_files(data_dir, by_name[table_name])])
                row = con.execute(f"select count(*) from pg.{schema}.{table_name.lower()}").fetchone()
                counts[table_name] = int(row[0]) if row else 0
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed the Airbyte source Postgres from generator files")
    parser.add_argument("--data_dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--dsn", default=os.environ.get(
        "SOURCE_POSTGRES_DSN",
        "host=localhost port=5433 dbname=daily_expense_tracker_fintech_application_source user=retail password=retail"))
    args = parser.parse_args(argv)
    for name, n in seed(args.data_dir, args.dsn).items():
        print(f"{SOURCE_SCHEMA}.{name.lower():<18} {n:>9,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
