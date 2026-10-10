-- One row per order: the courier's most recent report wins.
with ranked as (
    select
        *,
        row_number() over (partition by order_id
                           order by coalesce(delivered_at, collected_at) desc, attempts desc, loaded_at desc) as rn
    from {{ source('raw', 'deliveries') }}
)
select
    order_id,
    waybill,
    courier,
    province,
    collected_at,
    delivered_at,
    status          as delivery_status,
    attempts,
    source_file
from ranked
where rn = 1
