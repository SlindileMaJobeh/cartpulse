-- One row per order: current status, how long each step took (from the CDC history),
-- the customer as they were when they ordered, and what the courier reported.
with history as (
    select * from {{ ref('stg_cdc__orders') }}
),
latest as (
    select distinct on (order_id) *
    from history
    order by order_id, ts_ms desc, kafka_offset desc
),
milestones as (
    select
        order_id,
        min(changed_at) filter (where status = 'PAID')       as paid_at,
        min(changed_at) filter (where status = 'SHIPPED')    as shipped_at,
        min(changed_at) filter (where status = 'DELIVERED')  as delivered_at_shop,
        min(changed_at) filter (where status = 'CANCELLED')  as cancelled_at,
        count(*) filter (where op = 'u')                     as status_changes
    from history
    group by order_id
),
items as (
    select order_id, count(*) as lines, sum(quantity) as units, sum(quantity * unit_price) as items_value
    from {{ ref('stg_cdc__order_items') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    c.customer_sk,
    o.session_id,
    o.status,
    o.status not in ('CANCELLED', 'RETURNED')                         as is_revenue,
    o.payment_method,
    o.ship_province,
    o.placed_at,
    (o.placed_at at time zone 'Africa/Johannesburg')::date            as order_date,
    m.paid_at,
    m.shipped_at,
    m.cancelled_at,
    m.status_changes,
    coalesce(i.lines, 0)                                              as lines,
    coalesce(i.units, 0)                                              as units,
    coalesce(i.items_value, 0)                                        as items_value,
    o.shipping_fee,
    o.order_total,
    d.courier,
    d.waybill,
    d.delivery_status,
    d.attempts                                                        as delivery_attempts,
    d.collected_at,
    coalesce(d.delivered_at, m.delivered_at_shop)                     as delivered_at,
    round(extract(epoch from (d.delivered_at - d.collected_at)) / 3600.0, 1) as delivery_hours
from latest o
left join milestones m using (order_id)
left join items i using (order_id)
left join {{ ref('stg_deliveries') }} d using (order_id)
left join {{ ref('dim_customers') }} c
       on c.customer_id = o.customer_id
      and o.placed_at >= c.valid_from and o.placed_at < c.valid_to
where not o.is_deleted
