-- Courier performance by province. SLA: 72 hours from collection (96 for remote provinces).
with delivered as (
    select
        courier,
        ship_province,
        delivery_hours,
        delivery_attempts,
        delivery_hours <= case when ship_province in ('Northern Cape', 'Limpopo', 'North West', 'Eastern Cape')
                               then 96 else 72 end as within_sla
    from {{ ref('fct_orders') }}
    where delivery_status = 'DELIVERED' and delivery_hours is not null
)
select
    courier,
    ship_province,
    count(*)                                                             as deliveries,
    round(avg(delivery_hours), 1)                                        as avg_hours,
    round(percentile_cont(0.9) within group (order by delivery_hours)::numeric, 1) as p90_hours,
    round(avg(within_sla::int), 4)                                       as sla_rate,
    round(avg((delivery_attempts > 1)::int), 4)                          as repeat_attempt_rate
from delivered
group by 1, 2
