{{ config(materialized='view') }}

SELECT 
    DISTINCT
    -- website events data
    e.event_id as order_id,
    e.customer_id,
    e.event_time AS event_time,
    e.platform,

    -- marketing campaigns data - if doesn't exist than organic
    COALESCE(m.campaign_id, -1) AS campaign_id, 
    COALESCE(m.campaign_name, 'organic') AS campaign_name,
    COALESCE(m.c_name, 'organic') AS campaign_type
FROM {{ ref('stg_website_events') }} e 
    LEFT JOIN {{ source('moms_flower_shop', 'raw_marketing_campaign_events') }} m
        ON (e.campaign_id = m.campaign_id) 
WHERE e.event_name = 'page_hit'

