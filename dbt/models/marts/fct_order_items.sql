-- Order lines with the product's cost *at the time of the sale* (point-in-time join on the SCD2).
select
    i.order_id,
    i.product_id,
    p.product_sk,
    p.product_name,
    p.category,
    o.order_date,
    o.is_revenue,
    i.quantity,
    i.unit_price,
    i.quantity * i.unit_price                          as line_revenue,
    p.cost                                             as unit_cost_at_sale,
    i.quantity * (i.unit_price - p.cost)               as line_margin
from {{ ref('stg_cdc__order_items') }} i
join {{ ref('fct_orders') }} o using (order_id)
left join {{ ref('dim_products') }} p
       on p.product_id = i.product_id
      and o.placed_at >= p.valid_from and o.placed_at < p.valid_to
