with changes as ({{ cdc_changes('orders') }})
select
    (image ->> 'order_id')::bigint           as order_id,
    (image ->> 'customer_id')::bigint        as customer_id,
    image ->> 'session_id'                   as session_id,
    image ->> 'status'                       as status,
    before_json ->> 'status'                 as previous_status,
    image ->> 'payment_method'               as payment_method,
    (image ->> 'shipping_fee')::numeric(10, 2) as shipping_fee,
    (image ->> 'order_total')::numeric(12, 2)  as order_total,
    image ->> 'ship_province'                as ship_province,
    (image ->> 'placed_at')::timestamptz     as placed_at,
    op, is_deleted, changed_at, ts_ms, kafka_offset
from changes
