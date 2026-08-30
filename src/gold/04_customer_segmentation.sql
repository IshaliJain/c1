-- Gold: Customer segmentation with business metrics
-- Segments (mutually exclusive):
--   Inactive   : no valid orders
--   High-Value : total_revenue >= 10000
--   Repeat     : total_orders > 1 (and not High-Value)
--   One-Time   : exactly 1 valid order

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        c.country,
        CAST(c.lifetime_value AS DOUBLE) AS lifetime_value_expected,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2), 0) AS total_revenue,
        COALESCE(ROUND(AVG(CAST(o.total_amount AS DOUBLE)), 2), 0) AS avg_order_value,
        MAX(CAST(o.order_date AS DATE)) AS last_order_date
    FROM silver_customers c
    LEFT JOIN silver_orders o
        ON c.customer_id = o.customer_id
       AND o.is_valid = true
    WHERE c.is_valid = true
    GROUP BY
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        c.country,
        c.lifetime_value
),
segmented AS (
    SELECT
        *,
        CASE
            WHEN total_orders = 0 THEN 'Inactive'
            WHEN total_revenue >= 10000 THEN 'High-Value'
            WHEN total_orders > 1 THEN 'Repeat'
            ELSE 'One-Time'
        END AS customer_type
    FROM customer_orders
)
SELECT
    customer_id,
    customer_name,
    customer_segment,
    country,
    customer_type,
    total_orders,
    total_revenue,
    avg_order_value,
    lifetime_value_expected,
    total_revenue AS lifetime_value_actual,
    last_order_date
FROM segmented
ORDER BY total_revenue DESC, customer_id
