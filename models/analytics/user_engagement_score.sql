{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

WITH base_activity AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT DATE(event_time)) AS days_active_last_30,
        COUNT(DISTINCT event_id) AS events_last_30,
        COUNT(DISTINCT event_name) AS unique_event_types,
        MAX(event_time) AS last_event_time
    FROM {{ ref('stg_website_events') }}
    WHERE event_time >= DATEADD(day, -30, CURRENT_DATE())
    {% if is_incremental() %}
        AND event_time > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
    GROUP BY customer_id
),

purchase_behavior AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN event_id END) AS purchases_last_30,
        SUM(CASE WHEN event_name = 'purchase' THEN event_value ELSE 0 END) AS revenue_last_30
    FROM {{ ref('stg_website_events') }}
    WHERE event_time >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY customer_id
),

engagement_scores AS (
    SELECT 
        ba.customer_id,
        ba.days_active_last_30,
        ba.events_last_30,
        ba.unique_event_types,
        ba.last_event_time,
        pb.purchases_last_30,
        pb.revenue_last_30,
        (ba.days_active_last_30 * 2) + 
        (ba.events_last_30 / 10) + 
        (ba.unique_event_types * 5) + 
        (COALESCE(pb.purchases_last_30, 0) * 20) AS engagement_score
    FROM base_activity ba
    LEFT JOIN purchase_behavior pb ON ba.customer_id = pb.customer_id
),

final AS (
    SELECT 
        customer_id,
        days_active_last_30,
        events_last_30,
        unique_event_types,
        purchases_last_30,
        revenue_last_30,
        engagement_score,
        NTILE(10) OVER (ORDER BY engagement_score) AS engagement_decile,
        CASE 
            WHEN engagement_score >= 100 THEN 'Highly Engaged'
            WHEN engagement_score >= 50 THEN 'Moderately Engaged'
            WHEN engagement_score >= 20 THEN 'Lightly Engaged'
            ELSE 'Minimally Engaged'
        END AS engagement_tier,
        CURRENT_TIMESTAMP() AS last_updated
    FROM engagement_scores
)

SELECT * FROM final
