{#- PII never reaches the marts in clear text. -#}
{% macro mask_email(col) -%}
    regexp_replace({{ col }}, '^(.)[^@]*(@.*)$', '\1***\2')
{%- endmacro %}

{% macro mask_phone(col) -%}
    left({{ col }}, 5) || '*****' || right({{ col }}, 2)
{%- endmacro %}
