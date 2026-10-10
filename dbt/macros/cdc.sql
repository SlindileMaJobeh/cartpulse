{#- Every Debezium change for one shop table, with the row image as JSONB.
    `after` for snapshot/insert/update, `before` for deletes. -#}
{% macro cdc_changes(table_name) %}
    select
        op,
        op = 'd'                               as is_deleted,
        ts_ms,
        kafka_partition,
        kafka_offset,
        to_timestamp(ts_ms / 1000.0)           as changed_at,
        coalesce(after_json, before_json)      as image,
        before_json
    from {{ source('raw', 'cdc_events') }}
    where table_name = '{{ table_name }}'
{% endmacro %}

{#- Turn a CDC history into SCD type 2 validity windows. -#}
{% macro scd2_window(key) %}
    row_number() over (partition by {{ key }} order by ts_ms, kafka_offset)  as version,
    lead(changed_at) over (partition by {{ key }} order by ts_ms, kafka_offset) as next_changed_at
{% endmacro %}
