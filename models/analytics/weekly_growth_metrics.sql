{{ config(materialized='table') }}

WITH weekly_visits AS (
    SELECT 
        DATE_TRUNC('week', event_time) AS week_start,
        platform,
        campaign_type,
        COUNT(DISTINCT customer_id) AS new_visits,
        COUNT(DISTINCT order_id) AS total_visit_events
    FROM {{ ref('stg_website_hits') }}
    GROUP BY DATE_TRUNC('week', event_time), platform, campaign_type
),

weekly_active_users AS (
    SELECT 
        DATE_TRUNC('week', event_time) AS week_start,
        platform,
        COUNT(DISTINCT customer_id) AS active_users,
        COUNT(DISTINCT event_id) AS total_events
    FROM {{ ref('stg_website_events') }}
    GROUP BY DATE_TRUNC('week', event_time), platform
),

weekly_revenue AS (
    SELECT 
        DATE_TRUNC('week', event_time) AS week_start,
        platform,
        SUM(CASE WHEN event_name = 'purchase' THEN event_value ELSE 0 END) AS weekly_revenue,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN customer_id END) AS paying_customers
    FROM {{ ref('stg_website_events') }}
    GROUP BY DATE_TRUNC('week', event_time), platform
),

aggregated_weekly AS (
    SELECT 
        wv.week_start,
        wv.platform,
        wv.campaign_type,
        wv.new_visits,
        wau.active_users,
        wau.total_events,
        wr.weekly_revenue,
        wr.paying_customers
    FROM weekly_visits wv
    LEFT JOIN weekly_active_users wau 
        ON wv.week_start = wau.week_start 
        AND wv.platform = wau.platform
    LEFT JOIN weekly_revenue wr 
        ON wv.week_start = wr.week_start 
        AND wv.platform = wr.platform
),

final AS (
    SELECT 
        week_start,
        platform,
        campaign_type,
        new_visits,
        active_users,
        total_events,
        weekly_revenue,
        paying_customers,
        LAG(new_visits) OVER (PARTITION BY platform, campaign_type ORDER BY week_start) AS prev_week_visits,
        LAG(weekly_revenue) OVER (PARTITION BY platform ORDER BY week_start) AS prev_week_revenue,
        ((new_visits - LAG(new_visits) OVER (PARTITION BY platform, campaign_type ORDER BY week_start))::FLOAT 
            / NULLIF(LAG(new_visits) OVER (PARTITION BY platform, campaign_type ORDER BY week_start), 0)) * 100 AS visit_growth_rate,
        weekly_revenue / NULLIF(new_visits, 0) AS revenue_per_visit
    FROM aggregated_weekly
)

SELECT * FROM final
ORDER BY week_start DESC, platform
