{{ config(materialized='table') }}

WITH campaign_visits AS (
    SELECT 
        campaign_id,
        campaign_name,
        campaign_type,
        COUNT(DISTINCT customer_id) AS total_visits,
        COUNT(DISTINCT order_id) AS total_visit_events,
        MIN(event_time) AS first_visit_date,
        MAX(event_time) AS last_visit_date
    FROM {{ ref('stg_website_hits') }}
    WHERE campaign_id != -1
    GROUP BY campaign_id, campaign_name, campaign_type
),

campaign_costs AS (
    SELECT 
        campaign_id,
        SUM(cost) AS total_spend,
        COUNT(DISTINCT event_id) AS total_ad_events,
        AVG(cost) AS avg_cost_per_event
    FROM {{ ref('stg_marketing_campaigns') }}
    GROUP BY campaign_id
),

post_visit_purchases AS (
    SELECT 
        i.campaign_id,
        COUNT(DISTINCT e.customer_id) AS purchasers,
        SUM(e.event_value) AS total_revenue,
        COUNT(DISTINCT e.event_id) AS total_purchases
    FROM {{ ref('stg_website_hits') }} i
    INNER JOIN {{ ref('stg_website_events') }} e 
        ON i.customer_id = e.customer_id
        AND e.event_time > i.event_time
        AND e.event_name = 'purchase'
    WHERE i.campaign_id != -1
    GROUP BY i.campaign_id
),

final AS (
    SELECT 
        cv.campaign_id,
        cv.campaign_name,
        cv.campaign_type,
        cv.total_visits,
        cc.total_spend,
        cc.avg_cost_per_event,
        pip.purchasers,
        pip.total_revenue,
        pip.total_purchases,
        {{ calculate_conversion_rate('pip.purchasers', 'cv.total_visits') }} AS visit_to_purchase_rate,
        cc.total_spend / NULLIF(cv.total_visits, 0) AS cost_per_visit,
        pip.total_revenue / NULLIF(cc.total_spend, 0) AS return_on_ad_spend,
        DATEDIFF(day, cv.first_visit_date, cv.last_visit_date) AS campaign_duration_days
    FROM campaign_visits cv
    LEFT JOIN campaign_costs cc ON cv.campaign_id = cc.campaign_id
    LEFT JOIN post_visit_purchases pip ON cv.campaign_id = pip.campaign_id
)

SELECT * FROM final
