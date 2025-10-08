{{ config(materialized='table') }}

SELECT 
    -- event data
    campaign_name,
    platform,
    SUM(total_num_events) AS distinct_events
FROM {{ ref('stg_web_events_per_campaign') }}
GROUP BY 1,2

