select
    s.*,
    s.purchases > 0                     as converted,
    s.add_to_carts > 0                  as added_to_cart,
    a.session_id is not null            as cart_abandoned,
    a.cart_value                        as abandoned_value,
    o.order_id,
    o.order_total
from {{ ref('stg_sessions') }} s
left join {{ ref('stg_abandoned_carts') }} a using (session_id)
left join {{ ref('fct_orders') }} o on o.session_id = s.session_id
