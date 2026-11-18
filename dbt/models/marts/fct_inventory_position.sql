{#
  End-of-day stock position per store and SKU, with days of cover from the trailing
  7-day sales rate. Incremental by snapshot_date: new days are appended, reading back
  6 days so the trailing window is complete.
#}
{{ config(
    materialized='incremental',
    unique_key='snapshot_date',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{% set watermark = none %}
{% if is_incremental() and execute %}
    {% set watermark = run_query('select max(snapshot_date) from ' ~ this).columns[0].values()[0] %}
{% endif %}

with inventory as (
    select * from {{ ref('stg_inventory') }}
    {% if watermark %}
        where snapshot_date > {{ dbt.dateadd('day', -7, "cast('" ~ watermark ~ "' as date)") }}
    {% endif %}
),

products as (
    select
        sku,
        unit_price,
        cost_price
    from {{ ref('stg_products') }}
),

with_rate as (
    select
        i.*,
        avg(i.units_sold) over (
            partition by i.store_id, i.sku
            order by i.snapshot_date
            rows between 6 preceding and current row
        ) as avg_daily_units_7d
    from inventory as i
)

select
    {{ dbt_utils.generate_surrogate_key(['r.snapshot_date', 'r.store_id', 'r.sku']) }} as inventory_key,
    r.snapshot_date,
    r.store_id,
    r.sku,
    r.quantity_on_hand,
    r.reorder_level,
    r.units_received,
    r.units_sold,
    round(r.avg_daily_units_7d, 2) as avg_daily_units_7d,
    case
        when r.avg_daily_units_7d > 0 then round(r.quantity_on_hand / r.avg_daily_units_7d, 1)
    end as days_of_cover,
    r.quantity_on_hand = 0 as is_stockout,
    case
        when r.quantity_on_hand = 0 then 'OUT_OF_STOCK'
        when r.quantity_on_hand <= r.reorder_level then 'LOW'
        when r.quantity_on_hand <= r.reorder_level * 2 then 'MEDIUM'
        else 'SUFFICIENT'
    end as stock_status,
    r.quantity_on_hand * p.unit_price as inventory_value_retail,
    r.quantity_on_hand * p.cost_price as inventory_value_cost,
    r.ingested_at
from with_rate as r
left join products as p on r.sku = p.sku
{% if watermark %}
    where r.snapshot_date > cast('{{ watermark }}' as date)
{% endif %}
