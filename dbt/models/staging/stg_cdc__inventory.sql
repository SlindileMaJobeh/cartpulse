with changes as ({{ cdc_changes('inventory') }})
select
    (image ->> 'product_id')::bigint         as product_id,
    (image ->> 'stock_on_hand')::int         as stock_on_hand,
    (image ->> 'reorder_level')::int         as reorder_level,
    op, is_deleted, changed_at, ts_ms, kafka_offset
from changes
