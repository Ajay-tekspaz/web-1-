"""
Load generator output into a local DuckDB file: the stand-in for Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX.

Same raw contract as the Snowflake loader (lakehouse.raw_tables): files are unioned by
column name, unknown columns are kept, and every row gets _LOADED_AT and _SOURCE_FILE.
A full reload each run, so it is idempotent.

Usage:
    python -m loaders.duckdb_loader --data_dir data/generated --db_path data/lakehouse.duckdb
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import duckdb

from lakehouse.raw_tables import DUCKDB_TYPES, RAW_SCHEMA, RAW_TABLES, RawTable


def _types_literal(columns: dict[str, str]) -> str:
    pairs = ", ".join(f"'{c.lower()}': '{DUCKDB_TYPES[t]}'" for c, t in columns.items())
    return "{" + pairs + "}"


def _files(data_dir: Path, file_glob: str, through: date | None) -> list[str]:
    """Files for one table; daily files (name_YYYY-MM-DD.csv) after `through` are skipped."""
    files = sorted(data_dir.glob(file_glob))
    if through is not None:
        files = [f for f in files if "_" not in f.stem or date.fromisoformat(f.stem.rsplit("_", 1)[1]) <= through]
    return [f.as_posix() for f in files]


def read_csv_sql(table: RawTable) -> str:
    """SELECT over one table's files (bound as parameter 1): union by name, plus _loaded_at/_source_file."""
    return f"""
        select * exclude (filename),
               current_timestamp::timestamp as _loaded_at,
               regexp_extract(filename, '[^/\\\\]+$') as _source_file
        from read_csv(?, header = true, union_by_name = true, filename = true,
                      types = {_types_literal(table.columns)})
    """


def table_files(data_dir: Path, table: RawTable, through: date | None = None) -> list[str]:
    files = _files(Path(data_dir), table.file_glob, through)
    if not files:
        raise FileNotFoundError(f"no files for {table.name}: {(Path(data_dir) / table.file_glob).as_posix()}")
    return files


def load(data_dir: Path, db_path: Path, schema: str = RAW_SCHEMA, through: date | None = None) -> dict[str, int]:
    """(Re)load every raw table, optionally only the daily files up to `through`. Returns row counts."""
    data_dir = Path(data_dir)
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    with duckdb.connect(str(db_path)) as con:
        con.execute(f"create schema if not exists {schema}")
        for table in RAW_TABLES:
            files = table_files(data_dir, table, through)
            con.execute(f"create or replace table {schema}.{table.name} as {read_csv_sql(table)}", [files])
            row = con.execute(f"select count(*) from {schema}.{table.name}").fetchone()
            counts[table.name] = int(row[0]) if row else 0
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load generated CSVs into local DuckDB (DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX stand-in)")
    parser.add_argument("--data_dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--db_path", type=Path, default=Path("data/lakehouse.duckdb"))
    parser.add_argument("--through", type=date.fromisoformat, default=None,
                        help="only load daily files ingested on or before this date (simulates daily runs)")
    args = parser.parse_args(argv)

    counts = load(args.data_dir, args.db_path, through=args.through)
    print(f"[ok] loaded {args.data_dir} -> {args.db_path} ({RAW_SCHEMA})"
          + (f" through {args.through}" if args.through else ""))
    for name, n in counts.items():
        print(f"     {name:<16} {n:>9,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
