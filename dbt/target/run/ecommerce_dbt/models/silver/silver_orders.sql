
  
    
        create or replace table `workspace`.`ecommerce`.`silver_orders`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      WITH ranked_orders AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_timestamp DESC
        ) AS rn

    FROM `workspace`.`ecommerce`.`stg_orders`

)

SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    order_timestamp,
    shipping_address,
    payment_method,
    processing_date,
    year,
    month,
    day

FROM ranked_orders

WHERE rn = 1
  