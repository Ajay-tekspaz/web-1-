import re

from tests.test_snowflake_migrations import ROOT, _warehouse_blocks, apply_migrations


def test_bi_role_reads_gold_only():
    sql = (ROOT / "snowflake" / "migrations" / "003_bi_readonly_role.sql").read_text()
    grants = [s for s in apply_migrations.statements(sql)
              if s.startswith("GRANT") and "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_READER" in s]
    schema_grants = [g for g in grants if " SCHEMA " in g]
    assert schema_grants and all(".GOLD " in g for g in schema_grants)
    assert not any(re.search(r"\b(INSERT|UPDATE|DELETE|OWNERSHIP|ALL PRIVILEGES|CREATE)\b", g) for g in grants)


def test_bi_has_its_own_xs_warehouse():
    sql = (ROOT / "snowflake" / "migrations" / "003_bi_readonly_role.sql").read_text()
    assert "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_XS" in _warehouse_blocks(sql)
