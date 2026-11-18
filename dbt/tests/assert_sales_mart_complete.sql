-- The incremental sales mart must hold every staged order line, including late arrivals (D3):
-- per sale_date, mart revenue equals staged revenue.
with staged as (
    select
        cast(o.order_timestamp as date) as sale_date,
        sum(l.line_total) as net_revenue
    from {{ ref('stg_order_lines') }} as l
    inner join {{ ref('stg_orders') }} as o on l.order_id = o.order_id
    group by 1
),

mart as (
    select
        sale_date,
        sum(net_revenue) as net_revenue
    from {{ ref('fct_sales_daily') }}
    group by 1
)

select
    coalesce(s.sale_date, m.sale_date) as sale_date,
    s.net_revenue as staged,
    m.net_revenue as mart
from staged as s
full outer join mart as m on s.sale_date = m.sale_date
where s.net_revenue is null or m.net_revenue is null or abs(s.net_revenue - m.net_revenue) > 0.01
