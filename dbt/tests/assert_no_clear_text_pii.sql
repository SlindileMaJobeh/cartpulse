select customer_sk from {{ ref('dim_customers') }}
where email_masked not like '%***@%' or phone_masked !~ '\*{5}'
