-- Every column is cast to the raw contract's type (python/lakehouse/raw_tables.py): loaders differ in how they
-- type numbers (Airbyte's Snowflake destination writes every NUMBER as FLOAT), and the marts must not follow them.
with source as (
    select * from {{ source('raw', 'ORDERS_RAW') }}
)

select
    order_id,
    cast(store_id as integer) as store_id,
    -- D4: one day's file arrived with order_timestamp renamed to order_ts
    cast(coalesce(
        order_timestamp,
        {{ optional_column(source('raw', 'ORDERS_RAW'), 'order_ts', dbt.type_timestamp()) }}
    ) as {{ dbt.type_timestamp() }}) as order_timestamp,
    customer_id,
    cast(gross_amount as decimal(12, 2)) as gross_amount,
    cast(discount_amount as decimal(12, 2)) as discount_amount,
    cast(total_amount as decimal(12, 2)) as total_amount,
    cast(ingested_at as {{ dbt.type_timestamp() }}) as ingested_at,
    cast(_loaded_at as {{ dbt.type_timestamp() }}) as loaded_at
from source
-- D2: the POS feed occasionally re-sends an order; keep the first copy
qualify row_number() over (partition by order_id order by ingested_at, _loaded_at) = 1
