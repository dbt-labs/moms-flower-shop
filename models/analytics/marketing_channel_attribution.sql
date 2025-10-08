{{ config(materialized='table') }}

WITH customer_touchpoints AS (
    SELECT 
        i.customer_id,
        i.campaign_id,
        i.campaign_name,
        i.campaign_type,
        i.event_time AS touchpoint_time,
        'page_hit' AS touchpoint_type,
        1 AS touchpoint_sequence
    FROM {{ ref('stg_website_hits') }} i
    WHERE i.campaign_id != -1
),

purchase_events AS (
    SELECT 
        customer_id,
        event_id,
        event_time AS purchase_time,
        event_value AS purchase_value
    FROM {{ ref('stg_website_events') }}
    WHERE event_name = 'purchase'
),

attributed_purchases AS (
    SELECT 
        ct.customer_id,
        ct.campaign_id,
        ct.campaign_name,
        ct.campaign_type,
        ct.touchpoint_time,
        pe.event_id AS purchase_event_id,
        pe.purchase_time,
        pe.purchase_value,
        DATEDIFF(day, ct.touchpoint_time, pe.purchase_time) AS days_to_purchase
    FROM customer_touchpoints ct
    INNER JOIN purchase_events pe 
        ON ct.customer_id = pe.customer_id
        AND pe.purchase_time >= ct.touchpoint_time
),

first_touch_attribution AS (
    SELECT 
        campaign_id,
        campaign_name,
        campaign_type,
        COUNT(DISTINCT purchase_event_id) AS first_touch_purchases,
        COUNT(DISTINCT customer_id) AS first_touch_customers,
        SUM(purchase_value) AS first_touch_revenue
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id, purchase_event_id ORDER BY touchpoint_time) AS touch_rank
        FROM attributed_purchases
    ) ranked
    WHERE touch_rank = 1
    GROUP BY campaign_id, campaign_name, campaign_type
),

last_touch_attribution AS (
    SELECT 
        campaign_id,
        campaign_name,
        campaign_type,
        COUNT(DISTINCT purchase_event_id) AS last_touch_purchases,
        COUNT(DISTINCT customer_id) AS last_touch_customers,
        SUM(purchase_value) AS last_touch_revenue
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id, purchase_event_id ORDER BY touchpoint_time DESC) AS touch_rank
        FROM attributed_purchases
    ) ranked
    WHERE touch_rank = 1
    GROUP BY campaign_id, campaign_name, campaign_type
),

final AS (
    SELECT 
        COALESCE(fta.campaign_id, lta.campaign_id) AS campaign_id,
        COALESCE(fta.campaign_name, lta.campaign_name) AS campaign_name,
        COALESCE(fta.campaign_type, lta.campaign_type) AS campaign_type,
        fta.first_touch_purchases,
        fta.first_touch_customers,
        fta.first_touch_revenue,
        lta.last_touch_purchases,
        lta.last_touch_customers,
        lta.last_touch_revenue,
        (COALESCE(fta.first_touch_revenue, 0) + COALESCE(lta.last_touch_revenue, 0)) / 2 AS blended_attribution_revenue
    FROM first_touch_attribution fta
    FULL OUTER JOIN last_touch_attribution lta 
        ON fta.campaign_id = lta.campaign_id
)

SELECT * FROM final
