{{ config(materialized='table') }}

WITH campaign_events AS (
    -- Get base event metrics per campaign
    SELECT 
        campaign_id,
        total_num_events,
        CURRENT_TIMESTAMP AS analysis_timestamp
    FROM {{ ref('stg_web_events_per_campaign') }}
),

event_rankings AS (
    -- Rank campaigns by event volume
    SELECT 
        campaign_id,
        total_num_events,
        RANK() OVER (ORDER BY total_num_events DESC) AS event_rank,
        PERCENT_RANK() OVER (ORDER BY total_num_events) AS event_percentile
    FROM campaign_events
),

campaign_summary as (
    select 
        count(distinct campaign_id) as total_campaigns,
        sum(total_num_events) as total_events
    from campaign_events
),

performance_segments AS (
    -- Segment campaigns into performance tiers
    SELECT 
        campaign_id,
        total_num_events,
        event_rank,
        event_percentile,
        CASE 
            WHEN event_percentile >= 0.75 THEN 'Top Performer'
            WHEN event_percentile >= 0.50 THEN 'Above Average'
            WHEN event_percentile >= 0.25 THEN 'Below Average'
            ELSE 'Low Performer'
        END AS performance_tier
    FROM event_rankings
),

aggregate_metrics AS (
    -- Calculate aggregate statistics
    SELECT 
        performance_tier,
        COUNT(DISTINCT campaign_id) AS campaigns_in_tier,
        SUM(total_num_events) AS tier_total_events,
        AVG(total_num_events) AS tier_avg_events,
        MIN(total_num_events) AS tier_min_events,
        MAX(total_num_events) AS tier_max_events
    FROM performance_segments
    GROUP BY performance_tier
),

final_enriched AS (
    -- Join campaign-level and aggregate metrics
    SELECT 
        ps.campaign_id,
        ps.total_num_events,
        ps.event_rank,
        ps.event_percentile,
        ps.performance_tier,
        am.campaigns_in_tier,
        am.tier_total_events,
        am.tier_avg_events,
        am.tier_min_events,
        am.tier_max_events,
        ROUND(
            (ps.total_num_events::NUMERIC / NULLIF(am.tier_avg_events, 0)) * 100, 
            2
        ) AS pct_of_tier_avg,
        ROUND(
            (ps.total_num_events::NUMERIC / NULLIF(
                SUM(ps.total_num_events) OVER (), 0
            )) * 100, 
            2
        ) AS pct_of_total_events
    FROM performance_segments ps
    LEFT JOIN aggregate_metrics am 
        ON ps.performance_tier = am.performance_tier
)

SELECT * FROM final_enriched
ORDER BY event_rank

