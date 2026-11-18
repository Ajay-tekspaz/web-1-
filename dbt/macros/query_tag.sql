{#
  Extends dbt-snowflake's native query_tag handling (K-11).

  The static part of the tag comes from `+query_tag` in dbt_project.yml. This override adds the
  run- and node-specific keys that infraops' Path B lookup filters on (invocation_id, node_id),
  then sets the session tag exactly as the built-in macro does, returning the original tag so
  dbt resets it after the materialization.
#}
{% macro snowflake__set_query_tag() -%}
  {%- set configured = config.get('query_tag') -%}
  {%- if not configured -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set tag = fromjson(configured) -%}
  {%- do tag.update({'invocation_id': invocation_id, 'node_id': model.unique_id}) -%}
  {%- set new_query_tag = tojson(tag) -%}
  {%- set original_query_tag = get_current_query_tag() -%}
  {%- do run_query("alter session set query_tag = '{}'".format(new_query_tag)) -%}
  {{ return(original_query_tag) }}
{%- endmacro %}
