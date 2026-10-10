-- Consistency across two CDC streams: order header total = sum of lines + shipping.
select order_id, order_total, items_value, shipping_fee
from {{ ref('fct_orders') }}
where lines > 0
  and abs(order_total - (items_value + shipping_fee)) > 0.01
