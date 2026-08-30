-- Gold: Daily and weekly revenue trends
-- Source: valid Silver orders

SELECT
    'daily' AS period_type,
    CAST(o.order_date AS DATE) AS period_start,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) AS total_revenue,
    ROUND(AVG(CAST(o.total_amount AS DOUBLE)), 2) AS avg_order_value
FROM silver_orders o
WHERE o.is_valid = true
  AND o.order_date IS NOT NULL
  AND TRIM(o.order_date) != ''
GROUP BY CAST(o.order_date AS DATE)

UNION ALL

SELECT
    'weekly' AS period_type,
    DATE_TRUNC('week', CAST(o.order_date AS DATE)) AS period_start,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) AS total_revenue,
    ROUND(AVG(CAST(o.total_amount AS DOUBLE)), 2) AS avg_order_value
FROM silver_orders o
WHERE o.is_valid = true
  AND o.order_date IS NOT NULL
  AND TRIM(o.order_date) != ''
GROUP BY DATE_TRUNC('week', CAST(o.order_date AS DATE))

ORDER BY period_type, period_start
