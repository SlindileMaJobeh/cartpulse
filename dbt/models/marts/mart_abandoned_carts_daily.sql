-- How much money is left in carts, and how much of it comes back within 24 hours.
with carts as (
    select
        a.*,
        (a.last_activity_at at time zone 'Africa/Johannesburg')::date as cart_date,
        exists (
            select 1 from {{ ref('fct_orders') }} o
            where o.customer_id = a.customer_id
              and o.placed_at between a.last_activity_at and a.last_activity_at + interval '24 hours'
        ) as recovered
    from {{ ref('stg_abandoned_carts') }} a
)
select
    cart_date,
    count(*)                                         as abandoned_carts,
    round(sum(cart_value), 2)                        as abandoned_value,
    count(*) filter (where customer_id is null)      as guest_carts,
    count(*) filter (where recovered)                as recovered_carts,
    round(coalesce(sum(cart_value) filter (where recovered), 0), 2) as recovered_value
from carts
group by 1
