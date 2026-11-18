{#
  Returns `column_name` if the relation has it, otherwise a typed NULL.

  Raw tables evolve their schema (loaders keep unknown columns), so a column that only
  exists after an upstream rename can be referenced safely.
#}
{% macro optional_column(relation, column_name, data_type) -%}
  {%- set null_value = "cast(null as " ~ data_type ~ ")" -%}
  {%- if not execute -%}
    {{ null_value }}
  {%- else -%}
    {%- set existing = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- if column_name | lower in existing -%}
      {{ column_name }}
    {%- else -%}
      {{ null_value }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
