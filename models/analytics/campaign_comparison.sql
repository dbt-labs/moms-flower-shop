{{ config(materialized='table') }}

WITH campaign_metrics AS (
    SELECT 
        campaign_id,
        campaign_name,
        campaign_type,
        COUNT(DISTINCT customer_id) AS total_customers,
        MIN(event_time) AS campaign_start,
        MAX(event_time) AS campaign_end,
        COUNT(DISTINCT DATE(event_time)) AS active_days
    FROM {{ ref('stg_website_hits') }}
    WHERE campaign_id != -1
    GROUP BY campaign_id, campaign_name, campaign_type
),

campaign_engagement AS (
    SELECT 
        i.campaign_id,
        COUNT(DISTINCT e.event_id) AS total_post_visit_events,
        COUNT(DISTINCT e.customer_id) AS engaged_customers,
        AVG(DATEDIFF(hour, i.event_time, e.event_time)) AS avg_hours_to_first_event
    FROM {{ ref('stg_website_hits') }} i
    INNER JOIN {{ ref('stg_website_events') }} e 
        ON i.customer_id = e.customer_id
        AND e.event_time > i.event_time
    WHERE i.campaign_id != -1
    GROUP BY i.campaign_id
),

campaign_retention AS (
    SELECT 
        i.campaign_id,
        COUNT(DISTINCT CASE 
            WHEN DATEDIFF(day, i.event_time, e.event_time) BETWEEN 1 AND 7 
            THEN e.customer_id 
        END) AS day_7_retained,
        COUNT(DISTINCT CASE 
            WHEN DATEDIFF(day, i.event_time, e.event_time) BETWEEN 1 AND 30 
            THEN e.customer_id 
        END) AS day_30_retained
    FROM {{ ref('stg_website_hits') }} i
    LEFT JOIN {{ ref('stg_website_events') }} e 
        ON i.customer_id = e.customer_id
        AND e.event_time > i.event_time
    WHERE i.campaign_id != -1
    GROUP BY i.campaign_id
),

final AS (
    SELECT 
        cm.campaign_id,
        cm.campaign_name,
        cm.campaign_type,
        cm.total_customers,
        cm.campaign_start,
        cm.campaign_end,
        cm.active_days,
        ce.total_post_visit_events,
        ce.engaged_customers,
        ce.avg_hours_to_first_event,
        cr.day_7_retained,
        cr.day_30_retained,
        {{ calculate_conversion_rate('ce.engaged_customers', 'cm.total_customers') }} AS engagement_rate,
        {{ calculate_conversion_rate('cr.day_7_retained', 'cm.total_customers') }} AS day_7_retention_rate,
        {{ calculate_conversion_rate('cr.day_30_retained', 'cm.total_customers') }} AS day_30_retention_rate,
        ce.total_post_visit_events / NULLIF(ce.engaged_customers, 0) AS avg_events_per_engaged_user
    FROM campaign_metrics cm
    LEFT JOIN campaign_engagement ce ON cm.campaign_id = ce.campaign_id
    LEFT JOIN campaign_retention cr ON cm.campaign_id = cr.campaign_id
)

SELECT * FROM final
ORDER BY total_customers DESC
