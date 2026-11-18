from datetime import date

import duckdb

from lakehouse.raw_tables import RAW_SCHEMA, RAW_TABLES
from loaders import duckdb_loader, snowflake_loader


def test_duckdb_loader_loads_every_row_and_keeps_renamed_column(tmp_path, small_data):
    folder, truth = small_data
    db = tmp_path / "lakehouse.duckdb"
    counts = duckdb_loader.load(folder, db)
    assert counts["ORDERS_RAW"] == truth.row_counts["orders"]
    assert counts["ORDER_LINES_RAW"] == truth.row_counts["order_lines"]
    assert counts["INVENTORY_RAW"] == truth.row_counts["inventory"]
    with duckdb.connect(str(db), read_only=True) as con:
        renamed = con.execute(f"select count(order_ts) from {RAW_SCHEMA}.orders_raw").fetchone()[0]
        missing = con.execute(
            f"select count(*) from {RAW_SCHEMA}.orders_raw where order_timestamp is null").fetchone()[0]
        files = con.execute(f"select count(distinct _source_file) from {RAW_SCHEMA}.orders_raw").fetchone()[0]
    assert renamed == missing == truth.defects["renamed_column_rows"]
    assert files == 98


def test_duckdb_loader_through_date_only_loads_earlier_files(tmp_path, small_data):
    folder, truth = small_data
    counts = duckdb_loader.load(folder, tmp_path / "partial.duckdb", through=date(2026, 5, 31))
    assert 0 < counts["ORDERS_RAW"] < truth.row_counts["orders"]
    assert counts["STORES_RAW"] == truth.row_counts["stores"]  # undated files always load


def test_snowflake_plan_is_idempotent_and_evolves_schema(small_data):
    folder, _ = small_data
    statements = snowflake_loader.plan(folder)
    creates = [s for s in statements if s.startswith("CREATE TABLE")]
    copies = [s for s in statements if s.startswith("COPY INTO")]
    assert len(creates) == len(copies) == len(RAW_TABLES)
    assert all("IF NOT EXISTS" in s and "ENABLE_SCHEMA_EVOLUTION = TRUE" in s for s in creates)
    assert all("MATCH_BY_COLUMN_NAME" in s for s in copies)
    assert all("_LOADED_AT = METADATA$START_SCAN_TIME" in s for s in copies)  # freshness needs it
    assert any("ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE" in s for s in statements)
    assert not any("PASSWORD" in s.upper() for s in statements)


def test_snowflake_loader_dry_run_by_default(small_data, capsys, monkeypatch):
    folder, _ = small_data
    monkeypatch.setitem(__import__("sys").modules, "snowflake.connector", None)  # importing it would fail
    assert snowflake_loader.main(["--data_dir", str(folder)]) == 0
    assert "DRY RUN" in capsys.readouterr().out


def test_snowflake_load_uses_the_given_executor(small_data):
    folder, _ = small_data
    executed = []
    counts = snowflake_loader.load(folder, executed.append, lambda sql: (42,))
    assert executed == snowflake_loader.plan(folder)
    assert set(counts) == {t.name for t in RAW_TABLES} and set(counts.values()) == {42}
