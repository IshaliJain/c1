-- =============================================================================
-- Databricks SQL Dashboard Queries
-- Project: Databricks Medallion E-Commerce Pipeline
-- =============================================================================
--
-- Prerequisites (run once per session on Databricks):
--
--   CREATE OR REPLACE TEMP VIEW gold_sales_by_product AS
--     SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/sales_by_product`;
--
--   CREATE OR REPLACE TEMP VIEW gold_revenue_by_customer AS
--     SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/revenue_by_customer`;
--
--   CREATE OR REPLACE TEMP VIEW gold_daily_weekly_trends AS
--     SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/daily_weekly_trends`;
--
--   CREATE OR REPLACE TEMP VIEW gold_customer_segmentation AS
--     SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/customer_segmentation`;
--
-- Dashboard visualization recommendations:
--   Query 1 → Bar chart (horizontal)
--   Query 2 → Bar chart (histogram buckets)
--   Query 3 → Pie / donut chart
--   Query 4 → Line chart (daily revenue trend)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Query 1: Top 10 Products by Revenue
-- Visualization: Horizontal bar chart
-- X-axis: total_revenue | Y-axis: product_name
-- -----------------------------------------------------------------------------
SELECT
    product_id,
    product_name,
    category,
    total_orders,
    total_revenue,
    avg_order_value
FROM gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- Query 2: Customer Revenue Distribution (Histogram)
-- Visualization: Bar chart
-- X-axis: revenue_bucket | Y-axis: customer_count
-- -----------------------------------------------------------------------------
SELECT
    revenue_bucket,
    COUNT(*) AS customer_count
FROM (
    SELECT
        customer_id,
        CASE
            WHEN total_revenue = 0 THEN '0'
            WHEN total_revenue < 100 THEN '1-99'
            WHEN total_revenue < 500 THEN '100-499'
            WHEN total_revenue < 1000 THEN '500-999'
            WHEN total_revenue < 5000 THEN '1000-4999'
            WHEN total_revenue < 10000 THEN '5000-9999'
            ELSE '10000+'
        END AS revenue_bucket,
        CASE
            WHEN total_revenue = 0 THEN 1
            WHEN total_revenue < 100 THEN 2
            WHEN total_revenue < 500 THEN 3
            WHEN total_revenue < 1000 THEN 4
            WHEN total_revenue < 5000 THEN 5
            WHEN total_revenue < 10000 THEN 6
            ELSE 7
        END AS bucket_order
    FROM gold_revenue_by_customer
) buckets
GROUP BY revenue_bucket, bucket_order
ORDER BY bucket_order;


-- -----------------------------------------------------------------------------
-- Query 3: Customer Segmentation Pie Chart
-- Visualization: Pie / donut chart
-- Slices: customer_type | Values: customer_count
-- -----------------------------------------------------------------------------
SELECT
    customer_type,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_revenue), 2) AS segment_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_revenue_per_customer
FROM gold_customer_segmentation
GROUP BY customer_type
ORDER BY customer_count DESC;


-- -----------------------------------------------------------------------------
-- Query 4 (Bonus): Daily Revenue Trend
-- Visualization: Line chart
-- X-axis: period_start | Y-axis: total_revenue
-- -----------------------------------------------------------------------------
SELECT
    period_start,
    total_orders,
    total_revenue,
    avg_order_value
FROM gold_daily_weekly_trends
WHERE period_type = 'daily'
ORDER BY period_start;


-- -----------------------------------------------------------------------------
-- Query 5 (Bonus): Data Quality KPI — Silver Pass Rate by Entity
-- Visualization: Counter / table
-- Requires Silver quality summary table
-- -----------------------------------------------------------------------------
-- CREATE OR REPLACE TEMP VIEW silver_quality_summary AS
--   SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/silver/quality_summary`;
--
-- SELECT
--     entity,
--     check_category,
--     total_rows,
--     passed_count,
--     failed_count,
--     pass_percentage
-- FROM silver_quality_summary
-- ORDER BY entity, check_category;
