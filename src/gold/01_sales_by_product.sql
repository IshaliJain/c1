-- Gold: Sales aggregated by product
-- Source: valid Silver orders joined to valid Silver products

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(CAST(o.total_amount AS DOUBLE)), 2) AS total_revenue,
    ROUND(AVG(CAST(o.total_amount AS DOUBLE)), 2) AS avg_order_value
FROM silver_orders o
INNER JOIN silver_products p
    ON o.product_id = p.product_id
WHERE o.is_valid = true
  AND p.is_valid = true
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY total_revenue DESC
