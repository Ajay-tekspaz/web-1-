{#
  Daily sales per store and SKU.

  Incremental by sale_date with delete+insert: each run finds every sale_date touched by rows
  ingested since the last run (late arrivals, D3, land up to 3 days after the sale) and rebuilds
  those dates completely, so a late order is never double-counted or lost.
#}
{{ config(
    materialized='incremental',
    unique_key='sale_date',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

lines as (
    select * from {{ ref('stg_order_lines') }}
),

products as (
    select
        sku,
        cost_price
    from {{ ref('stg_products') }}
),

{% if is_incremental() %}
    affected_dates as (
        select distinct cast(orders.order_timestamp as date) as sale_date
        from orders
        where orders.ingested_at > (select max(t.max_ingested_at) from {{ this }} as t)
    ),
{% endif %}

order_lines as (
    select
        cast(o.order_timestamp as date) as sale_date,
        o.store_id,
        l.sku,
        o.order_id,
        l.quantity,
        l.quantity * l.unit_price as gross_revenue,
        l.discount_amount,
        l.line_total as net_revenue,
        l.quantity * p.cost_price as cost_of_goods,
        o.ingested_at
    from lines as l
    inner join orders as o on l.order_id = o.order_id
    left join products as p on l.sku = p.sku
    {% if is_incremental() %}
        where cast(o.order_timestamp as date) in (select a.sale_date from affected_dates as a)
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['sale_date', 'store_id', 'sku']) }} as sales_key,
    sale_date,
    store_id,
    sku,
    count(distinct order_id) as num_orders,
    sum(quantity) as units_sold,
    sum(gross_revenue) as gross_revenue,
    sum(discount_amount) as discount_amount,
    sum(net_revenue) as net_revenue,
    sum(cost_of_goods) as cost_of_goods,
    sum(net_revenue) - sum(cost_of_goods) as gross_margin,
    max(ingested_at) as max_ingested_at
from order_lines
group by sale_date, store_id, sku
