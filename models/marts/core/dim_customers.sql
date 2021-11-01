
with customers as (
    select * from {{ref('stg_customers')}}    
),
orders as (
    select * from {{ref('stg_orders')}}
)
,
payments as (
  select * from {{ref('stg_payments')}}
)
,
customers_orders as (
select customer_id,
  min(order_date) as first_order_date,
  max(order_date) as most_recent_order_date,
  count(order_id) as number_of_orders
  from orders
  group by 1
)
,

customer_life_time as (
  select sum(amount) as amount , customer_id
  from payments
  left join orders using(order_id)
  group by customer_id
)

,
final as (
select
  customers.customer_id,
   customers.first_name,
   customers.last_name,
  customers_orders.first_order_date,
  customers_orders.most_recent_order_date,
  coalesce(customers_orders.number_of_orders,0) as number_of_orders,
  amount
  from customers
  left join customers_orders using (customer_id)
  left join customer_life_time using(customer_id)
 )

 select * from final
