{{ config(materialized='view') }}

WITH raw_flower_orders AS (
    SELECT * FROM {{ ref('raw_flower_orders') }}
)

SELECT 
    order_id,
    customer_id,
    TO_TIMESTAMP(order_time/1000) AS order_time,
    order_value,
    flowers_amount,
    vase_amount,
    chocolate_amount,
    delivery_id,
    platform
FROM raw_flower_orders
WHERE order_time IS NOT NULL
  AND customer_id IS NOT NULL
  AND order_value IS NOT NULL
  AND order_time > 0

