-- Order lines are insert-only in the shop, so the latest image per key is the line.
with changes as ({{ cdc_changes('order_items') }}),
latest as (
    select *, row_number() over (partition by image ->> 'order_id', image ->> 'product_id'
                                 order by ts_ms desc, kafka_offset desc) as rn
    from changes
)
select
    (image ->> 'order_id')::bigint           as order_id,
    (image ->> 'product_id')::bigint         as product_id,
    (image ->> 'quantity')::int              as quantity,
    (image ->> 'unit_price')::numeric(10, 2) as unit_price
from latest
where rn = 1 and not is_deleted
