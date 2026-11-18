"""The seeder, run end to end against a DuckDB file attached as `pg` (same statements the postgres extension gets)."""

import duckdb

from lakehouse.raw_tables import RAW_TABLES
from loaders import postgres_seeder


def test_seed_creates_every_source_table(tmp_path, small_data):
    folder, truth = small_data
    target = tmp_path / "pg.duckdb"
    counts = postgres_seeder.seed(folder, target.as_posix(), db_type="duckdb")
    assert counts["ORDERS_RAW"] == truth.row_counts["orders"]
    with duckdb.connect(str(target), read_only=True) as con:
        tables = {r[0] for r in con.execute(
            "select table_name from duckdb_tables() where schema_name = 'retail'").fetchall()}
        renamed = con.execute("select count(order_ts) from retail.orders_raw").fetchone()[0]
        cursor_nulls = con.execute("select count(*) from retail.orders_raw where ingested_at is null").fetchone()[0]
    assert tables == {t.name.lower() for t in RAW_TABLES}
    assert renamed == truth.defects["renamed_column_rows"]
    assert cursor_nulls == 0  # Airbyte's incremental cursor


def test_seed_is_rerunnable(tmp_path, small_data):
    folder, truth = small_data
    target = (tmp_path / "pg.duckdb").as_posix()
    postgres_seeder.seed(folder, target, db_type="duckdb")
    assert postgres_seeder.seed(folder, target, db_type="duckdb")["ORDERS_RAW"] == truth.row_counts["orders"]


def test_attach_escapes_quotes_in_the_dsn():
    assert postgres_seeder.attach_sql("host=x password=it's") == "attach 'host=x password=it''s' as pg (type postgres)"
