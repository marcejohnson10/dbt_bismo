{{
    config(
        materialized = 'materialized_table'
    )
}}


select 
    order_id 
from  {{ ref('stg_jaffle_orders') }} orders
inner join {{ ref('stg_jaffle_customers') }} customers on orders.customer_id = customers.customer_id
where status = 'completed'


