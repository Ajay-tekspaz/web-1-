"""
Staging casts every typed raw column to the contract's type.

Loaders type the raw tables differently: Airbyte's Snowflake destination writes every NUMBER (ids, quantities,
money) as FLOAT. A staging model that passes a column through unchanged lets that reach the incremental marts,
whose on_schema_change='fail' then stops the build (and FLOAT money rounds). So each staging model must cast each
non-text contract column of its raw table.
"""

import re
from pathlib import Path

import pytest

from lakehouse.raw_tables import RAW_TABLES

STAGING = Path(__file__).resolve().parents[1] / "dbt" / "models" / "staging"


def staging_model(raw_table: str) -> Path:
    return STAGING / f"stg_{raw_table.removesuffix('_RAW').lower()}.sql"


@pytest.mark.parametrize("table", RAW_TABLES, ids=lambda t: t.name)
def test_staging_casts_each_typed_contract_column(table):
    sql = staging_model(table.name).read_text(encoding="utf-8").lower()
    typed = [column.lower() for column, kind in table.columns.items() if kind != "VARCHAR"]
    uncast = [c for c in typed if not re.search(rf"cast\(\s*(coalesce\(\s*)?{c}\b", sql)]
    assert not uncast, f"{staging_model(table.name).name} passes these through uncast: {uncast}"
