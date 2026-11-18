-- Every column is cast to the raw contract's type (python/lakehouse/raw_tables.py): loaders differ in how they
-- type numbers (Airbyte's Snowflake destination writes every NUMBER as FLOAT), and the marts must not follow them.
select
    promo_id,
    scope as promo_scope,
    target as promo_target,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    cast(discount_pct as integer) as discount_pct
from {{ source('raw', 'PROMOTIONS_RAW') }}
