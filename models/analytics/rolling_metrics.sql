{{ config(
    materialized='table',
    tags=['metrics', 'snapshot']
) }}

WITH ds AS (
    {{ generate_date_spine('2020-01-01', '2025-12-31') }}
),

daily_orders AS (
    SELECT 
        DATE(event_time) AS metric_date,
        COUNT(DISTINCT customer_id) AS orders
    FROM {{ ref('stg_website_hits') }}
    GROUP BY DATE(event_time)
),

daily_revenue AS (
    SELECT 
        DATE(event_time) AS metric_date,
        SUM(CASE WHEN event_name = 'purchase' THEN event_value ELSE 0 END) AS revenue
    FROM {{ ref('stg_website_events') }}
    GROUP BY DATE(event_time)
),

daily_active_users AS (
    SELECT 
        DATE(event_time) AS metric_date,
        COUNT(DISTINCT customer_id) AS active_users
    FROM {{ ref('stg_website_events') }}
    GROUP BY DATE(event_time)
),

-- TODO (Elias): Calculate 7d rolling orders
-- daily_7d_orders AS (
    -- SELECT 
    --    *
    -- FROM
    --     daily_orders
-- ),

combined_daily AS (
    SELECT 
        ds.date_day AS metric_date,
        COALESCE(di.orders, 0) AS daily_orders,
        COALESCE(dr.revenue, 0) AS daily_revenue,
        COALESCE(dau.active_users, 0) AS daily_active_users
    FROM ds
    LEFT JOIN daily_orders di ON ds.date_day = di.metric_date
    LEFT JOIN daily_revenue dr ON ds.date_day = dr.metric_date
    LEFT JOIN daily_active_users dau ON ds.date_day = dau.metric_date
),

rolling_calculations AS (
    SELECT 
        metric_date,
        daily_orders,
        daily_revenue,
        daily_active_users,
        SUM(daily_revenue) OVER (ORDER BY metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_revenue,
        AVG(daily_active_users) OVER (ORDER BY metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg_dau,
        SUM(daily_orders) OVER (ORDER BY metric_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_orders,
        SUM(daily_revenue) OVER (ORDER BY metric_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_revenue,
        AVG(daily_active_users) OVER (ORDER BY metric_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_avg_dau
    FROM combined_daily
),

final AS (
    SELECT 
        *,
        rolling_30d_revenue / NULLIF(rolling_30d_orders, 0) AS rolling_30d_revenue_per_order
    FROM rolling_calculations
    WHERE metric_date >= '2020-01-01'
)

SELECT * FROM final
ORDER BY metric_date DESC
