{#- Generic tests without external packages (no `dbt deps` needed). -#}
{% test value_between(model, column_name, min_value, max_value) %}
    select * from {{ model }}
    where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}

{% test unique_combination(model, columns) %}
    select {{ columns | join(', ') }}, count(*) from {{ model }}
    group by {{ columns | join(', ') }} having count(*) > 1
{% endtest %}
