-- SCD2 integrity: never two "current" rows for the same customer or product.
select 'customer' as dim, customer_id as id from {{ ref('dim_customers') }} where is_current
group by customer_id having count(*) > 1
union all
select 'product', product_id from {{ ref('dim_products') }} where is_current
group by product_id having count(*) > 1
