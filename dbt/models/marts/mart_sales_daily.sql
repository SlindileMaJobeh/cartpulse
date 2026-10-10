select
    d.date_day                                                      as order_date,
    d.day_name,
    d.is_weekend,
    d.is_public_holiday,
    d.holiday_name,
    d.is_payday_period,
    count(o.order_id) filter (where o.is_revenue)                   as orders,
    count(o.order_id) filter (where o.status = 'CANCELLED')         as cancelled_orders,
    coalesce(sum(o.order_total) filter (where o.is_revenue), 0)     as revenue,
    coalesce(sum(o.units) filter (where o.is_revenue), 0)           as units,
    round(avg(o.order_total) filter (where o.is_revenue), 2)        as avg_order_value,
    count(distinct o.customer_id) filter (where o.is_revenue)       as customers
from {{ ref('dim_date') }} d
left join {{ ref('fct_orders') }} o on o.order_date = d.date_day
where d.date_day <= current_date
group by 1, 2, 3, 4, 5, 6
