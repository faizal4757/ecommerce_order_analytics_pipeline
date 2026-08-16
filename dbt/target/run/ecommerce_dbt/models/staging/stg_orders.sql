
  
  
  
  create or replace view `workspace`.`ecommerce`.`stg_orders`
  
  as (
    SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    shipping_address,
    payment_method,
    CAST(processing_date AS DATE) AS processing_date,
    year,
    month,
    day
FROM `workspace`.`ecommerce`.`orders`
  )
