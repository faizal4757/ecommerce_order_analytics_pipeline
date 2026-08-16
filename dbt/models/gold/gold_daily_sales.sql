SELECT
    processing_date,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(quantity) AS units_sold,
    SUM(quantity * unit_price) AS revenue
FROM {{ ref('silver_orders') }}
GROUP BY processing_date