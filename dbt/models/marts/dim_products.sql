select
    sku,
    product_name,
    category,
    unit_price,
    cost_price,
    round((unit_price - cost_price) / unit_price, 4) as list_margin_pct,
    is_active
from {{ ref('stg_products') }}
