-- [Phase 3] Generic dbt Test - No Duplicates
-- Ensures tables have no duplicate rows based on a unique key.
-- Status: Planned for Phase 3

-- Example usage in schema.yml:
-- tests:
--   - no_duplicates:
--       column_names: ['sale_id', 'sale_date']

{% test no_duplicates(model, column_names) %}

with duplicates as (
  select
    {% for column in column_names -%}
      {{ column }}{{ "," if not loop.last else "" }}
    {%- endfor %},
    count(*) as duplicate_count
  from {{ model }}
  group by {% for column in column_names -%}
      {{ column }}{{ "," if not loop.last else "" }}
    {%- endfor %}
  having count(*) > 1
)

select *
from duplicates
where true

{% endtest %}
