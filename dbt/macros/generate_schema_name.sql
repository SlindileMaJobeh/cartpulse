{#- Use "staging" / "marts" as-is rather than dbt's "<target>_<custom>" naming. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
