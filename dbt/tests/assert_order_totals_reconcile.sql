-- K-10: an order's total must equal the sum of its lines (quantity x list price - promotion discount).
-- Orders that lost a line to D1 (empty quantity) are excluded: their lines are incomplete by design.
with line_totals as (
    select
        order_id,
        sum(line_total) as lines_total,
        count(*) as n_lines
    from {{ source('raw', 'ORDER_LINES_RAW') }}
    group by order_id
    having count(quantity) = count(*)
)

select
    o.order_id,
    o.total_amount,
    l.lines_total
from {{ ref('stg_orders') }} as o
inner join line_totals as l on o.order_id = l.order_id
where abs(o.total_amount - l.lines_total) > 0.01 * l.n_lines
