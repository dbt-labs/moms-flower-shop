{{ config(materialized='table') }}

WITH platform_visits AS (
    SELECT 
        platform,
        COUNT(DISTINCT customer_id) AS total_visits,
        COUNT(DISTINCT campaign_id) AS campaigns_used,
        MIN(event_time) AS first_visit,
        MAX(event_time) AS last_visit
    FROM {{ ref('stg_website_hits') }}
    GROUP BY platform
),

platform_engagement AS (
    SELECT 
        platform,
        COUNT(DISTINCT customer_id) AS engaged_users,
        COUNT(DISTINCT event_id) AS total_events,
        COUNT(DISTINCT DATE(event_time)) AS active_days,
        AVG(event_value) AS avg_event_value
    FROM {{ ref('stg_website_events') }}
    GROUP BY platform
),

platform_revenue AS (
    SELECT 
        platform,
        COUNT(DISTINCT customer_id) AS paying_customers,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN event_id END) AS total_purchases,
        SUM(CASE WHEN event_name = 'purchase' THEN event_value ELSE 0 END) AS total_revenue
    FROM {{ ref('stg_website_events') }}
    GROUP BY platform
),

final AS (
    SELECT 
        pv.platform,
        pv.total_visits,
        pv.campaigns_used,
        pe.engaged_users,
        pe.total_events,
        pe.active_days,
        pr.paying_customers,
        pr.total_purchases,
        pr.total_revenue,
        {{ calculate_conversion_rate('pr.paying_customers', 'pv.total_visits') }} AS visit_to_paying_rate,
        pe.total_events / NULLIF(pv.total_visits, 0) AS avg_events_per_visit,
        pr.total_revenue / NULLIF(pr.paying_customers, 0) AS avg_revenue_per_paying_customer,
        DATEDIFF(day, pv.first_visit, pv.last_visit) AS platform_active_days
    FROM platform_visits pv
    LEFT JOIN platform_engagement pe ON pv.platform = pe.platform
    LEFT JOIN platform_revenue pr ON pv.platform = pr.platform
)

SELECT * FROM final
