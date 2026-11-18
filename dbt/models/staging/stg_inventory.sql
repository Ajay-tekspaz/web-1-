-- Every column is cast to the raw contract's type (python/lakehouse/raw_tables.py): loaders differ in how they
-- type numbers (Airbyte's Snowflake destination writes every NUMBER as FLOAT), and the marts must not follow them.
select
    cast(snapshot_date as date) as snapshot_date,
    cast(store_id as integer) as store_id,
    sku,
    cast(quantity_on_hand as integer) as quantity_on_hand,
    cast(reorder_level as integer) as reorder_level,
    cast(units_received as integer) as units_received,
    cast(units_sold as integer) as units_sold,
    cast(last_updated as {{ dbt.type_timestamp() }}) as last_updated,
    cast(ingested_at as {{ dbt.type_timestamp() }}) as ingested_at
from {{ source('raw', 'INVENTORY_RAW') }}
-- D5: negative stock counts are recording errors; drop them (the source test reports how many)
where quantity_on_hand >= 0
