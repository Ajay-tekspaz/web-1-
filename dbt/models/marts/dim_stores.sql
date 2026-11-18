select
    store_id,
    store_name,
    region,
    opened_date
from {{ ref('stg_stores') }}
