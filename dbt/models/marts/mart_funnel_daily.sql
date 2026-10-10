-- Session funnel by day and traffic source: who views, who adds, who checks out, who buys.
select
    session_date,
    traffic_source,
    count(*)                                          as sessions,
    count(*) filter (where product_views > 0)         as viewed_product,
    count(*) filter (where add_to_carts > 0)          as added_to_cart,
    count(*) filter (where checkouts > 0)             as reached_checkout,
    count(*) filter (where purchases > 0)             as purchased,
    count(*) filter (where cart_abandoned)            as abandoned_carts,
    round(count(*) filter (where purchases > 0)::numeric / nullif(count(*), 0), 4) as conversion_rate,
    round(avg(duration_s))                            as avg_duration_s
from {{ ref('fct_sessions') }}
group by 1, 2
