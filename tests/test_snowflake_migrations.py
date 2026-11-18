import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("apply_migrations", ROOT / "snowflake" / "apply_migrations.py")
apply_migrations = importlib.util.module_from_spec(spec)
spec.loader.exec_module(apply_migrations)


def _all_sql() -> str:
    return "\n".join(f.read_text() for f in sorted((ROOT / "snowflake" / "migrations").glob("*.sql")))


def _warehouse_blocks(sql: str) -> dict[str, str]:
    return dict(re.findall(r"CREATE WAREHOUSE IF NOT EXISTS (\w+)(.*?);", sql, re.S))


def test_every_warehouse_is_xs_with_short_auto_suspend():
    blocks = _warehouse_blocks(_all_sql())
    assert "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS" in blocks
    for name, body in blocks.items():
        assert "WAREHOUSE_SIZE = XSMALL" in body, name
        assert int(re.search(r"AUTO_SUSPEND = (\d+)", body).group(1)) <= 60, name


def test_every_warehouse_is_tagged_with_project_and_environment():
    sql = _all_sql()
    for name in _warehouse_blocks(sql):
        tag_stmt = re.search(rf"ALTER WAREHOUSE {name} SET TAG(.*?);", sql, re.S)
        assert tag_stmt, f"{name} has no tags"
        assert "PROJECT = 'daily-expense-tracker-fintech-application'" in tag_stmt.group(1)
        assert "ENVIRONMENT = 'dev'" in tag_stmt.group(1)


def test_nothing_targets_production():
    assert "prod" not in _all_sql().lower().replace("'dev', 'staging', 'prod'", "")


def test_dry_run_is_the_default(capsys):
    assert apply_migrations.main([]) == 0
    assert "DRY RUN" in capsys.readouterr().out


def test_pipeline_role_is_created_before_it_is_granted_anything():
    sql = _all_sql()
    created = sql.index("CREATE ROLE IF NOT EXISTS DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE")
    assert created < sql.index("TO ROLE DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE")


def test_apply_refuses_to_run_without_an_admin_role(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_MIGRATION_ROLE", raising=False)
    with pytest.raises(SystemExit, match="SNOWFLAKE_MIGRATION_ROLE"):
        apply_migrations.main(["--apply"])
