{#- Put models in the schema named in config (silver, gold, ref) rather than <target>_<schema>. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}

{#- Minutes between two timestamps, exact to the second. -#}
{% macro minutes_between(a, b) -%}
((epoch({{ b }}) - epoch({{ a }})) / 60.0)
{%- endmacro %}
