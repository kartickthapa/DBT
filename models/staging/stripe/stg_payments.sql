with payments as (
select id,orderid as order_id,amount,paymentmethod,status 
from raw.stripe.payment
)
select * from payments
