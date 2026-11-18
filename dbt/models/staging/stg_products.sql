-- Every column is cast to the raw contract's type (python/lakehouse/raw_tables.py): loaders differ in how they
-- type numbers (Airbyte's Snowflake destination writes every NUMBER as FLOAT), and the marts must not follow them.
select
    sku,
    product_name,
    category,
    cast(unit_price as decimal(10, 2)) as unit_price,
    cast(cost_price as decimal(10, 2)) as cost_price,
    cast(active as integer) = 1 as is_active
from {{ source('raw', 'PRODUCTS_RAW') }}
where unit_price > 0
