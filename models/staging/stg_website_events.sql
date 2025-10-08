{{ config(materialized='view') }}

WITH raw_website_events AS (
    SELECT * FROM {{ ref('raw_website_events') }}
)

SELECT 
    event_id,
    customer_id,
    TO_TIMESTAMP(event_time/1000) AS event_time,  
    event_name,
    event_value,
    additional_details,
    platform,
    campaign_id
FROM raw_website_events
WHERE event_time IS NOT NULL
  AND customer_id IS NOT NULL
  AND event_name IS NOT NULL
  AND platform IS NOT NULL
  AND event_time > 0