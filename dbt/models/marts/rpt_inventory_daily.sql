-- Wide, BI-ready view of stock positions: the "Inventory health" dashboard reads this.
{{ config(materialized='view') }}

select
    f.snapshot_date,
    f.snapshot_date = max(f.snapshot_date) over () as is_latest_snapshot,
    f.store_id,
    s.store_name,
    s.region,
    f.sku,
    p.product_name,
    p.category,
    f.quantity_on_hand,
    f.reorder_level,
    f.units_sold,
    f.units_received,
    f.avg_daily_units_7d,
    f.days_of_cover,
    case when f.is_stockout then 1 else 0 end as is_stockout,
    f.stock_status,
    f.inventory_value_retail,
    f.inventory_value_cost
from {{ ref('fct_inventory_position') }} as f
inner join {{ ref('dim_stores') }} as s on f.store_id = s.store_id
inner join {{ ref('dim_products') }} as p on f.sku = p.sku
