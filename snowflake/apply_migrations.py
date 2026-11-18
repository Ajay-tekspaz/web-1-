"""
Apply snowflake/migrations/*.sql in order. Dry run by default: prints what would run.

    python snowflake/apply_migrations.py            # dry run
    python snowflake/apply_migrations.py --apply    # execute (key-pair auth from the environment)

--apply runs as SNOWFLAKE_MIGRATION_ROLE (or --role): migrations create roles, users, warehouses
and tags, which the DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE pipeline role is not allowed to do.

Every migration is idempotent (IF NOT EXISTS / ALTER ... SET), so re-running is safe.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

MIGRATIONS = Path(__file__).resolve().parent / "migrations"
QUERY_TAG = '{"project":"daily-expense-tracker-fintech-application","environment":"dev","tool":"migrations"}'


def statements(sql: str) -> list[str]:
    """Split a migration into statements (no semicolons inside strings in these files)."""
    body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--role", default=os.environ.get("SNOWFLAKE_MIGRATION_ROLE"),
                        help="admin role to run as (default: $SNOWFLAKE_MIGRATION_ROLE)")
    args = parser.parse_args(argv)
    files = sorted(MIGRATIONS.glob("*.sql"))

    if not args.apply:
        print("-- DRY RUN: nothing executed. Re-run with --apply.")
        for f in files:
            print(f"\n-- {f.name}: {len(statements(f.read_text(encoding='utf-8')))} statements")
        return 0

    if not args.role:
        raise SystemExit("set SNOWFLAKE_MIGRATION_ROLE (or --role): "
                         "migrations need an admin role, not DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
    import snowflake.connector

    from lakehouse.snowflake_auth import connection_kwargs

    kwargs = {**connection_kwargs(), "role": args.role}
    with snowflake.connector.connect(**kwargs, session_parameters={"QUERY_TAG": QUERY_TAG}) as conn:
        cur = conn.cursor()
        for f in files:
            for stmt in statements(f.read_text(encoding="utf-8")):
                cur.execute(stmt)
            print(f"[ok] {f.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
