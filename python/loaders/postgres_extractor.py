"""
Extract the raw tables from a customer's source Postgres into the generator's file layout.

The other direction of postgres_seeder: where the seeder plays the operational system with generated data,
this reads a real one. The source exposes schema `retail` with one table per raw table (docs/ingestion.md,
"Your own source"), and its rows are written as CSV files exactly where the loaders expect the generator's
files, so the Snowflake loader (or the DuckDB one) takes them without knowing the difference.

The platform that deploys a project writes the connection into the CI environment:
SOURCE_POSTGRES_HOST, SOURCE_POSTGRES_PORT, SOURCE_POSTGRES_DATABASE, SOURCE_POSTGRES_USER,
SOURCE_POSTGRES_PASSWORD, SOURCE_POSTGRES_SSLMODE. Use a read-only account.

Usage:
    python -m loaders.postgres_extractor --data_dir data/generated            # connection from the environment
    python -m loaders.postgres_extractor --data_dir data/generated --check    # only report what the source has
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping
from pathlib import Path

import duckdb

from lakehouse.raw_tables import RAW_TABLES, RawTable
from loaders.postgres_seeder import SOURCE_SCHEMA, attach_sql

# Columns the loaders add themselves; a source that carries them (the seeder's does) must not send them twice.
LOADER_COLUMNS = ("_loaded_at", "_source_file")


class SourceNotReady(Exception):
    """The source is missing tables or columns the raw contract needs; the message says which."""


def dsn_from_env(env: Mapping[str, str] | None = None) -> str | None:
    """A libpq connection string from SOURCE_POSTGRES_*, or None when no source is configured."""
    env = os.environ if env is None else env
    host = env.get("SOURCE_POSTGRES_HOST", "").strip()
    if not host:
        return None
    required = ("SOURCE_POSTGRES_DATABASE", "SOURCE_POSTGRES_USER", "SOURCE_POSTGRES_PASSWORD")
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise SourceNotReady(f"SOURCE_POSTGRES_HOST is set but {', '.join(missing)} is missing")

    def quote(value: str) -> str:  # libpq: single-quote values, backslash-escape quotes and backslashes
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    parts = {
        "host": env["SOURCE_POSTGRES_HOST"].strip(),
        "port": env.get("SOURCE_POSTGRES_PORT", "5432").strip() or "5432",
        "dbname": env["SOURCE_POSTGRES_DATABASE"],
        "user": env["SOURCE_POSTGRES_USER"],
        "password": env["SOURCE_POSTGRES_PASSWORD"],
        "sslmode": env.get("SOURCE_POSTGRES_SSLMODE", "require").strip() or "require",
    }
    return " ".join(f"{k}={quote(v)}" for k, v in parts.items())


def target_file(data_dir: Path, table: RawTable) -> Path:
    """Where the loaders look for this table: the glob's folder, or the file itself."""
    if "*" in table.file_glob:
        return Path(data_dir) / table.file_glob.replace("*", "extract")
    return Path(data_dir) / table.file_glob


def _columns(con: duckdb.DuckDBPyConnection, schema: str, table: RawTable) -> list[str]:
    name = table.name.lower()
    try:
        rows = con.execute(f"describe select * from pg.{schema}.{name}").fetchall()
    except duckdb.Error as error:
        raise SourceNotReady(f"the source has no table {schema}.{name} (needed for {table.name}): {error}") from error
    return [r[0] for r in rows]


def extract(
    dsn: str, data_dir: Path, schema: str = SOURCE_SCHEMA, db_type: str = "postgres", check_only: bool = False,
) -> dict[str, int]:
    """Writes one CSV per raw table and returns row counts. db_type='duckdb' attaches a DuckDB file (the tests)."""
    counts: dict[str, int] = {}
    problems: list[str] = []
    with duckdb.connect() as con:
        if db_type == "postgres":
            con.execute("install postgres; load postgres;")
        con.execute(attach_sql(dsn, db_type))
        plan: list[tuple[RawTable, list[str]]] = []
        for table in RAW_TABLES:
            try:
                have = _columns(con, schema, table)
            except SourceNotReady as error:
                problems.append(str(error))
                continue
            lower = {c.lower() for c in have}
            absent = [c for c in table.columns if c.lower() not in lower]
            if absent:
                problems.append(f"{schema}.{table.name.lower()} lacks {', '.join(absent)}")
                continue
            plan.append((table, [c for c in have if c.lower() not in LOADER_COLUMNS]))
        if problems:
            raise SourceNotReady("; ".join(problems))
        for table, columns in plan:
            select = ", ".join(f'"{c}"' for c in columns)
            source = f"select {select} from pg.{schema}.{table.name.lower()}"
            row = con.execute(f"select count(*) from ({source})").fetchone()
            counts[table.name] = int(row[0]) if row else 0
            if not check_only:
                out = target_file(data_dir, table)
                out.parent.mkdir(parents=True, exist_ok=True)
                con.execute(f"copy ({source}) to '{out.as_posix()}' (format csv, header true)")
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract the raw tables from the customer's source Postgres")
    parser.add_argument("--data_dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--schema", default=SOURCE_SCHEMA)
    parser.add_argument("--check", action="store_true", help="report what the source has; write nothing")
    args = parser.parse_args(argv)

    try:
        dsn = dsn_from_env()
        if dsn is None:
            print("[skip] SOURCE_POSTGRES_HOST is not set: no source to extract from", file=sys.stderr)
            return 2
        counts = extract(dsn, args.data_dir, args.schema, check_only=args.check)
    except SourceNotReady as error:
        print(f"[fail] {error}", file=sys.stderr)
        return 1
    print(f"[ok] {'checked' if args.check else 'extracted'} the source -> {args.data_dir}")
    for name, n in counts.items():
        print(f"     {name:<16} {n:>9,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
