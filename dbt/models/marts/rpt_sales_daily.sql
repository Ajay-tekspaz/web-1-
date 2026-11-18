-- Wide, BI-ready view of daily sales: the "Sales performance" dashboard reads this.
{{ config(materialized='view') }}

select
    f.sale_date,
    f.store_id,
    s.store_name,
    s.region,
    f.sku,
    p.product_name,
    p.category,
    f.num_orders,
    f.units_sold,
    f.gross_revenue,
    f.discount_amount,
    f.net_revenue,
    f.cost_of_goods,
    f.gross_margin
from {{ ref('fct_sales_daily') }} as f
inner join {{ ref('dim_stores') }} as s on f.store_id = s.store_id
inner join {{ ref('dim_products') }} as p on f.sku = p.sku
