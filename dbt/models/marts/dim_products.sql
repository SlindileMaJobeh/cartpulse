-- SCD type 2: the full price/cost history of every product, plus current stock.
with versions as (
    select *, {{ scd2_window('product_id') }}
    from {{ ref('stg_cdc__products') }}
),
stock as (
    select distinct on (product_id) product_id, stock_on_hand, reorder_level
    from {{ ref('stg_cdc__inventory') }}
    order by product_id, ts_ms desc, kafka_offset desc
)
select
    md5(v.product_id::text || '-' || v.version::text)                 as product_sk,
    v.product_id,
    v.version,
    v.sku,
    v.product_name,
    v.category,
    v.price,
    v.cost,
    round((v.price - v.cost) / v.price, 4)                            as margin_pct,
    v.is_active,
    -- first version is open-ended to the past, so point-in-time joins never miss a
    -- row created a few microseconds after the fact that references it
    case when v.version = 1 then timestamptz '1900-01-01 00:00:00+00' else v.changed_at end as valid_from,
    coalesce(v.next_changed_at, timestamptz '9999-12-31 00:00:00+00') as valid_to,
    v.next_changed_at is null and not v.is_deleted                    as is_current,
    case when v.next_changed_at is null then s.stock_on_hand end      as stock_on_hand,
    case when v.next_changed_at is null then s.stock_on_hand <= s.reorder_level end as needs_reorder
from versions v
left join stock s using (product_id)
