{{ config(materialized='table') }}

WITH event_sequences AS (
    SELECT 
        customer_id,
        event_id,
        event_name,
        event_time,
        event_value,
        additional_details,
        LAG(event_name) OVER (PARTITION BY customer_id ORDER BY event_time) AS prev_event,
        LEAD(event_name) OVER (PARTITION BY customer_id ORDER BY event_time) AS next_event,
        LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time) AS prev_event_time
    FROM {{ ref('stg_website_events') }}
),

event_transitions AS (
    SELECT 
        prev_event,
        event_name AS current_event,
        next_event,
        COUNT(*) AS transition_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        AVG(DATEDIFF(second, prev_event_time, event_time)) AS avg_seconds_between_events
    FROM event_sequences
    WHERE prev_event IS NOT NULL
    GROUP BY prev_event, current_event, next_event
),

event_popularity AS (
    SELECT 
        event_name,
        COUNT(DISTINCT customer_id) AS customers_with_event,
        COUNT(event_id) AS total_occurrences,
        AVG(event_value) AS avg_value
    FROM {{ ref('stg_website_events') }}
    GROUP BY event_name
),

final AS (
    SELECT 
        et.prev_event,
        et.current_event,
        et.next_event,
        et.transition_count,
        et.unique_customers,
        et.avg_seconds_between_events,
        ep1.total_occurrences AS prev_event_total,
        ep2.total_occurrences AS current_event_total,
        {{ calculate_conversion_rate('et.transition_count', 'ep1.total_occurrences') }} AS transition_probability
    FROM event_transitions et
    LEFT JOIN event_popularity ep1 ON et.prev_event = ep1.event_name
    LEFT JOIN event_popularity ep2 ON et.current_event = ep2.event_name
    WHERE et.transition_count > 5
)

SELECT * FROM final
ORDER BY transition_count DESC
