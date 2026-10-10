-- Recency, Frequency, Monetary segmentation of customers who have bought.
with base as (
    select
        customer_id,
        current_date - max(order_date)          as recency_days,
        count(*)                                as frequency,
        sum(order_total)                        as monetary
    from {{ ref('fct_orders') }}
    where is_revenue
    group by customer_id
),
scored as (
    select
        *,
        6 - ntile(5) over (order by recency_days)  as r_score,     -- recent buyers score 5
        ntile(5) over (order by frequency)         as f_score,
        ntile(5) over (order by monetary)          as m_score
    from base
)
select
    s.customer_id,
    c.first_name,
    c.province,
    s.recency_days,
    s.frequency,
    round(s.monetary, 2)                        as monetary,
    s.r_score, s.f_score, s.m_score,
    case
        when s.r_score >= 4 and s.f_score >= 4                then 'Champions'
        when s.r_score >= 3 and s.f_score >= 3                then 'Loyal'
        when s.r_score >= 4 and s.f_score <= 2                then 'New / Promising'
        when s.r_score <= 2 and s.f_score >= 3                then 'At Risk'
        when s.r_score <= 2                                   then 'Hibernating'
        else 'Needs Attention'
    end                                         as segment
from scored s
left join {{ ref('dim_customers') }} c on c.customer_id = s.customer_id and c.is_current
