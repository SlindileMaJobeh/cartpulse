with changes as ({{ cdc_changes('customers') }})
select
    (image ->> 'customer_id')::bigint        as customer_id,
    image ->> 'first_name'                   as first_name,
    image ->> 'last_name'                    as last_name,
    image ->> 'email'                        as email,
    image ->> 'phone'                        as phone,
    image ->> 'province'                     as province,
    image ->> 'city'                         as city,
    (image ->> 'marketing_opt_in')::boolean  as marketing_opt_in,
    (image ->> 'created_at')::timestamptz    as created_at,
    op, is_deleted, changed_at, ts_ms, kafka_offset
from changes
