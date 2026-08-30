-- Gold: Revenue aggregated by customer
-- Source: valid Silver orders joined to valid Silver customers

SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) AS total_revenue,
    ROUND(AVG(CAST(o.total_amount AS DOUBLE)), 2) AS avg_order_value,
    ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) AS lifetime_value_actual
FROM silver_orders o
INNER JOIN silver_customers c
    ON o.customer_id = c.customer_id
WHERE o.is_valid = true
  AND c.is_valid = true
GROUP BY
    c.customer_id,
    c.customer_name,
    c.customer_segment
ORDER BY total_revenue DESC
