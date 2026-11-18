{#
  Use the configured schema name as-is (STAGING, GOLD) instead of dbt's default
  "<target_schema>_<custom>" so the BI role can be granted one predictable schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
