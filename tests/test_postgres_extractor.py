"""The extractor, run against a DuckDB file attached as `pg` (the same statements the postgres extension gets)."""

import duckdb
import pytest

from lakehouse.raw_tables import RAW_TABLES
from loaders import duckdb_loader, postgres_extractor, postgres_seeder, snowflake_loader
from loaders.postgres_extractor import SourceNotReady


@pytest.fixture()
def source(tmp_path, small_data):
    """A seeded source database standing in for the customer's Postgres."""
    folder, truth = small_data
    target = (tmp_path / "customer.duckdb").as_posix()
    postgres_seeder.seed(folder, target, db_type="duckdb")
    return target, truth


def test_extracted_files_load_exactly_like_the_generators(tmp_path, source):
    target, truth = source
    out = tmp_path / "extracted"
    counts = postgres_extractor.extract(target, out, db_type="duckdb")
    assert counts["ORDERS_RAW"] == truth.row_counts["orders"]
    loaded = duckdb_loader.load(out, tmp_path / "lake.duckdb")
    assert loaded == counts


def test_the_loaders_own_columns_are_not_sent_twice(tmp_path, source):
    target, _ = source
    out = tmp_path / "extracted"
    postgres_extractor.extract(target, out, db_type="duckdb")
    header = (out / "stores.csv").read_text(encoding="utf-8").splitlines()[0].lower()
    assert "_loaded_at" not in header and "_source_file" not in header


def test_an_evolved_column_in_the_source_is_kept(tmp_path, source):
    target, truth = source
    out = tmp_path / "extracted"
    postgres_extractor.extract(target, out, db_type="duckdb")
    header = next((out / "orders").glob("*.csv")).read_text(encoding="utf-8").splitlines()[0].lower()
    assert "order_ts" in header  # the D4 rename arrives as its own column, like the generator's files


def test_check_only_writes_nothing(tmp_path, source):
    target, _ = source
    out = tmp_path / "extracted"
    counts = postgres_extractor.extract(target, out, db_type="duckdb", check_only=True)
    assert counts and not out.exists()


def test_a_missing_table_or_column_is_named(tmp_path, source):
    target, _ = source
    with duckdb.connect(target) as con:
        con.execute("drop table retail.stores_raw")
        con.execute("alter table retail.products_raw drop column category")
    with pytest.raises(SourceNotReady) as raised:
        postgres_extractor.extract(target, tmp_path / "out", db_type="duckdb")
    message = str(raised.value)
    assert "stores_raw" in message and "CATEGORY" in message


def test_no_source_configured_means_no_dsn():
    assert postgres_extractor.dsn_from_env({}) is None
    assert postgres_extractor.dsn_from_env({"SOURCE_POSTGRES_HOST": "  "}) is None


def test_connection_string_quotes_awkward_passwords_and_defaults_to_ssl():
    dsn = postgres_extractor.dsn_from_env({
        "SOURCE_POSTGRES_HOST": "db.internal", "SOURCE_POSTGRES_DATABASE": "ops",
        "SOURCE_POSTGRES_USER": "reader", "SOURCE_POSTGRES_PASSWORD": "it's a \\ pass",
    })
    assert "host='db.internal'" in dsn and "port='5432'" in dsn and "sslmode='require'" in dsn
    assert "password='it\\'s a \\\\ pass'" in dsn


def test_a_half_configured_source_fails_clearly():
    with pytest.raises(SourceNotReady, match="SOURCE_POSTGRES_PASSWORD"):
        postgres_extractor.dsn_from_env(
            {"SOURCE_POSTGRES_HOST": "h", "SOURCE_POSTGRES_DATABASE": "d", "SOURCE_POSTGRES_USER": "u"})


def test_every_extract_target_is_where_the_loaders_look(tmp_path):
    for table in RAW_TABLES:
        target = postgres_extractor.target_file(tmp_path, table)
        folder = table.file_glob.split("/")[0] if "/" in table.file_glob else None
        assert (target.parent.name == folder) if folder else (target.name == table.file_glob)
        assert target.match(table.file_glob) or target == tmp_path / table.file_glob


def test_snowflake_full_refresh_replaces_the_table_and_forces_the_copy(tmp_path):
    normal = snowflake_loader.plan(tmp_path)
    refresh = snowflake_loader.plan(tmp_path, full_refresh=True)
    assert not any(s.startswith(("TRUNCATE", "REMOVE")) for s in normal)
    assert not any("FORCE = TRUE" in s for s in normal)
    truncates = [s for s in refresh if s.startswith("TRUNCATE TABLE")]
    assert len(truncates) == len(RAW_TABLES)
    assert sum("FORCE = TRUE" in s for s in refresh) == len(RAW_TABLES)
    # emptied first, then loaded: TRUNCATE comes before its PUT
    first = next(i for i, s in enumerate(refresh) if s.startswith("TRUNCATE"))
    assert refresh[first + 1].startswith("PUT")
