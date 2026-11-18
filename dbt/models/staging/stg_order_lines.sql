-- Every column is cast to the raw contract's type (python/lakehouse/raw_tables.py): loaders differ in how they
-- type numbers (Airbyte's Snowflake destination writes every NUMBER as FLOAT), and the marts must not follow them.
select
    order_id,
    cast(line_number as integer) as line_number,
    sku,
    cast(quantity as integer) as quantity,
    cast(unit_price as decimal(10, 2)) as unit_price,
    cast(discount_amount as decimal(12, 2)) as discount_amount,
    cast(line_total as decimal(12, 2)) as line_total,
    cast(ingested_at as {{ dbt.type_timestamp() }}) as ingested_at
from {{ source('raw', 'ORDER_LINES_RAW') }}
-- D1: lines without a quantity can't be valued; they are counted by the source test and dropped
where quantity is not null
