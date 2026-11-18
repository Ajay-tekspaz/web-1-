{#
  Weekly demand forecast per store x SKU for the next forecast_horizon_weeks weeks, with an
  80% band. The numbers come from python/forecasts/demand_forecast.py (Holt-Winters, K-16),
  which writes ML.DEMAND_FORECAST_OUTPUT; this model adds names and revenue for BI.

  Build order: dbt build --exclude tag:forecast -> forecast script -> dbt build --select tag:forecast
#}

with forecast as (
    select * from {{ source('ml', 'DEMAND_FORECAST_OUTPUT') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['f.forecast_week', 'f.store_id', 'f.sku']) }} as forecast_key,
    cast(f.forecast_week as date) as forecast_week,
    f.store_id,
    s.store_name,
    s.region,
    f.sku,
    p.product_name,
    p.category,
    f.forecast_units,
    f.lower_80,
    f.upper_80,
    round(f.forecast_units * p.unit_price, 2) as forecast_revenue,
    f.model_name,
    f.holdout_mape,
    cast(f.trained_through as date) as trained_through,
    f.generated_at
from forecast as f
inner join {{ ref('dim_stores') }} as s on f.store_id = s.store_id
inner join {{ ref('dim_products') }} as p on f.sku = p.sku
where f.forecast_week <= {{ dbt.dateadd('week', var('forecast_horizon_weeks'), 'f.trained_through') }}
