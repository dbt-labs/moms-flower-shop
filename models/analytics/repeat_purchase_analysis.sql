{{ config(materialized='table') }}

WITH customer_purchases AS (
    SELECT 
        customer_id,
        event_id AS purchase_id,
        event_time AS purchase_time,
        event_value AS purchase_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time) AS purchase_number,
        LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time) AS prev_purchase_time,
        LAG(event_value) OVER (PARTITION BY customer_id ORDER BY event_time) AS prev_purchase_amount
    FROM {{ ref('stg_website_events') }}
    WHERE event_name = 'purchase'
),

purchase_intervals AS (
    SELECT 
        customer_id,
        purchase_id,
        purchase_time,
        purchase_amount,
        purchase_number,
        prev_purchase_time,
        prev_purchase_amount,
        DATEDIFF(day, prev_purchase_time, purchase_time) AS days_since_last_purchase,
        purchase_amount - COALESCE(prev_purchase_amount, 0) AS purchase_amount_change
    FROM customer_purchases
),

customer_purchase_summary AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT purchase_id) AS total_purchases,
        MIN(purchase_time) AS first_purchase_date,
        MAX(purchase_time) AS last_purchase_date,
        SUM(purchase_amount) AS total_lifetime_value,
        AVG(purchase_amount) AS avg_purchase_value,
        AVG(days_since_last_purchase) AS avg_days_between_purchases,
        STDDEV(days_since_last_purchase) AS purchase_frequency_stddev
    FROM purchase_intervals
    GROUP BY customer_id
),

repeat_customer_classification AS (
    SELECT 
        cps.customer_id,
        cps.total_purchases,
        cps.first_purchase_date,
        cps.last_purchase_date,
        cps.total_lifetime_value,
        cps.avg_purchase_value,
        cps.avg_days_between_purchases,
        cps.purchase_frequency_stddev,
        DATEDIFF(day, cps.first_purchase_date, cps.last_purchase_date) AS customer_purchase_lifespan,
        CASE 
            WHEN cps.total_purchases >= 10 THEN 'Super Frequent'
            WHEN cps.total_purchases >= 5 THEN 'Frequent'
            WHEN cps.total_purchases >= 3 THEN 'Repeat'
            WHEN cps.total_purchases = 2 THEN 'Second Time'
            ELSE 'One Time'
        END AS purchase_frequency_segment
    FROM customer_purchase_summary cps
),

final AS (
    SELECT 
        rcc.*,
        c.platform,
        c.campaign_type,
        c.state,
        rcc.total_lifetime_value / NULLIF(rcc.total_purchases, 0) AS actual_avg_order_value,
        CASE 
            WHEN rcc.avg_days_between_purchases <= 7 THEN 'Weekly'
            WHEN rcc.avg_days_between_purchases <= 30 THEN 'Monthly'
            WHEN rcc.avg_days_between_purchases <= 90 THEN 'Quarterly'
            ELSE 'Infrequent'
        END AS purchase_cadence
    FROM repeat_customer_classification rcc
    LEFT JOIN {{ ref('stg_customers') }} c ON rcc.customer_id = c.customer_id
)

SELECT * FROM final
