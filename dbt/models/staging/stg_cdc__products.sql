with changes as ({{ cdc_changes('products') }})
select
    (image ->> 'product_id')::bigint         as product_id,
    image ->> 'sku'                          as sku,
    image ->> 'name'                         as product_name,
    image ->> 'category'                     as category,
    (image ->> 'price')::numeric(10, 2)      as price,
    (image ->> 'cost')::numeric(10, 2)       as cost,
    (image ->> 'is_active')::boolean         as is_active,
    (image ->> 'created_at')::timestamptz    as created_at,
    op, is_deleted, changed_at, ts_ms, kafka_offset
from changes
