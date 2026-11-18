"""
Raw-layer contract shared by every loader (DuckDB, Snowflake, Airbyte fallback).

Each raw table maps to the generator's files. Loaders union files *by column name*
and keep columns they don't know about (schema evolution), so an upstream rename
shows up as a new nullable column instead of being silently dropped. dbt staging
decides what to do with it.
"""

from __future__ import annotations

from dataclasses import dataclass

RAW_SCHEMA = "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX"


@dataclass(frozen=True)
class RawTable:
    name: str
    file_glob: str          # relative to the generator output directory
    columns: dict[str, str]  # contract columns -> Snowflake type


RAW_TABLES: tuple[RawTable, ...] = (
    RawTable("STORES_RAW", "stores.csv", {
        "STORE_ID": "INTEGER", "STORE_NAME": "VARCHAR", "REGION": "VARCHAR", "OPENED_DATE": "DATE",
    }),
    RawTable("PRODUCTS_RAW", "products.csv", {
        "SKU": "VARCHAR", "PRODUCT_NAME": "VARCHAR", "CATEGORY": "VARCHAR",
        "UNIT_PRICE": "NUMBER(10,2)", "COST_PRICE": "NUMBER(10,2)", "ACTIVE": "INTEGER",
    }),
    RawTable("PROMOTIONS_RAW", "promotions.csv", {
        "PROMO_ID": "VARCHAR", "SCOPE": "VARCHAR", "TARGET": "VARCHAR",
        "START_DATE": "DATE", "END_DATE": "DATE", "DISCOUNT_PCT": "INTEGER",
    }),
    RawTable("ORDERS_RAW", "orders/*.csv", {
        "ORDER_ID": "VARCHAR", "STORE_ID": "INTEGER", "ORDER_TIMESTAMP": "TIMESTAMP_NTZ",
        "CUSTOMER_ID": "VARCHAR", "GROSS_AMOUNT": "NUMBER(12,2)", "DISCOUNT_AMOUNT": "NUMBER(12,2)",
        "TOTAL_AMOUNT": "NUMBER(12,2)", "INGESTED_AT": "TIMESTAMP_NTZ",
    }),
    RawTable("ORDER_LINES_RAW", "order_lines/*.csv", {
        "ORDER_ID": "VARCHAR", "LINE_NUMBER": "INTEGER", "SKU": "VARCHAR", "QUANTITY": "INTEGER",
        "UNIT_PRICE": "NUMBER(10,2)", "DISCOUNT_AMOUNT": "NUMBER(12,2)", "LINE_TOTAL": "NUMBER(12,2)",
        "INGESTED_AT": "TIMESTAMP_NTZ",
    }),
    RawTable("INVENTORY_RAW", "inventory/*.csv", {
        "SNAPSHOT_DATE": "DATE", "STORE_ID": "INTEGER", "SKU": "VARCHAR", "QUANTITY_ON_HAND": "INTEGER",
        "REORDER_LEVEL": "INTEGER", "UNITS_RECEIVED": "INTEGER", "UNITS_SOLD": "INTEGER",
        "LAST_UPDATED": "TIMESTAMP_NTZ", "INGESTED_AT": "TIMESTAMP_NTZ",
    }),
)

# Snowflake type -> DuckDB type for the local stand-in
DUCKDB_TYPES = {
    "INTEGER": "INTEGER", "VARCHAR": "VARCHAR", "DATE": "DATE", "TIMESTAMP_NTZ": "TIMESTAMP",
    "NUMBER(10,2)": "DECIMAL(10,2)", "NUMBER(12,2)": "DECIMAL(12,2)",
}
