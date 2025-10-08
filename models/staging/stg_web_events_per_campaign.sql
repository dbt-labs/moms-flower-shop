{{ config(materialized='view') }}

WITH website_events AS (
    SELECT 
        e.event_id,
        e.customer_id,
        e.event_time,
        e.platform,
        COALESCE(m.campaign_id, -1) AS campaign_id, 
        COALESCE(m.campaign_name, 'organic') AS campaign_name,
        COALESCE(m.c_name, 'organic') AS campaign_type
    FROM {{ ref('stg_website_events') }} e
        LEFT JOIN {{ source('moms_flower_shop', 'raw_marketing_campaign_events') }} m
            ON (e.campaign_id = m.campaign_id) 
    WHERE e.event_name = 'page_hit'
)

SELECT 
    campaign_id,
    campaign_name,
    campaign_type,
    platform,
    event_time,
    COUNT(event_id) AS total_num_events
FROM website_events
GROUP BY 1,2,3,4,5
