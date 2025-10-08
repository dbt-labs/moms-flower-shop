{{ config(materialized='table') }}

WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', ORDER_TIME) AS revenue_month,
        platform,
        SUM( flowers_amount + vase_amount + chocolate_amount) AS total_revenue,
        COUNT(DISTINCT customer_id) AS paying_customers,
        COUNT(DISTINCT order_id) AS total_transactions
    FROM {{ ref('stg_flower_orders') }}
    GROUP BY DATE_TRUNC('month', ORDER_TIME), platform
),

monthly_visits AS (
    SELECT 
        DATE_TRUNC('month', event_time) AS visit_month,
        platform,
        COUNT(DISTINCT customer_id) AS new_visits
    FROM {{ ref('stg_website_hits') }}
    GROUP BY DATE_TRUNC('month', event_time), platform
),

cumulative_metrics AS (
    SELECT 
        mr.revenue_month,
        mr.platform,
        mr.total_revenue,
        mr.paying_customers,
        mr.total_transactions,
        mv.new_visits,
        SUM(mr.total_revenue) OVER (PARTITION BY mr.platform ORDER BY mr.revenue_month) AS cumulative_revenue,
        SUM(mv.new_visits) OVER (PARTITION BY mr.platform ORDER BY mr.revenue_month) AS cumulative_visits
    FROM monthly_revenue mr
    LEFT JOIN monthly_visits mv 
        ON mr.revenue_month = mv.visit_month 
        AND mr.platform = mv.platform
),

final AS (
    SELECT 
        revenue_month,
        platform,
        total_revenue,
        paying_customers,
        total_transactions,
        new_visits,
        cumulative_revenue,
        cumulative_visits,
        LAG(total_revenue) OVER (PARTITION BY platform ORDER BY revenue_month) AS prev_month_revenue,
        total_revenue / NULLIF(paying_customers, 0) AS arpu,
        total_revenue / NULLIF(total_transactions, 0) AS avg_transaction_value,
        ((total_revenue - LAG(total_revenue) OVER (PARTITION BY platform ORDER BY revenue_month))::FLOAT 
            / NULLIF(LAG(total_revenue) OVER (PARTITION BY platform ORDER BY revenue_month), 0)) * 100 AS revenue_growth_rate
    FROM cumulative_metrics
)

SELECT * FROM final
ORDER BY revenue_month DESC, platform
