-- Every column is cast to the raw contract's type (python/lakehouse/raw_tables.py): loaders differ in how they
-- type numbers (Airbyte's Snowflake destination writes every NUMBER as FLOAT), and the marts must not follow them.
select
    cast(store_id as integer) as store_id,
    store_name,
    region,
    cast(opened_date as date) as opened_date
from {{ source('raw', 'STORES_RAW') }}
