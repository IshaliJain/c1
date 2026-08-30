# Gold Layer — AI Prompt History

**Date:** 2026-08-30  
**Tool:** Cursor Agent Mode  
**Branch:** `feature/databricks-medallion-pipeline`

## Prompt

```
Create SQL/PySpark aggregation scripts in src/gold/:
01_sales_by_product.sql: Aggregates product_id, product_name, category, total_orders, total_revenue, avg_order_value.
02_revenue_by_customer.sql: Aggregates customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual.
03_daily_weekly_trends.sql: Aggregates revenue over time.
04_customer_segmentation.sql: Categorizes customers into High-Value, Repeat, One-Time, or Inactive and computes metrics.
Create create_gold_tables.py to execute and persist these Gold Delta tables.
Append prompt details to ai-prompts/gold-layer.md.
```

## Accepted Suggestions

| Suggestion | Rationale |
|------------|-----------|
| SQL files for business logic + `create_gold_tables.py` orchestrator | Separates analytics definitions from PySpark I/O |
| `gold_common.py` for path resolution and Silver view registration | Matches Bronze/Silver module patterns |
| Filter `is_valid = true` on Silver sources | Gold uses only quality-passing rows per design-notes |
| `CAST(... AS DOUBLE)` / `CAST(... AS DATE)` in SQL | Silver stores string columns from Bronze; explicit casts in Gold |
| Overwrite Gold Delta on each run | Idempotent batch refresh for v1 |
| Daily + weekly trends in one table with `period_type` | Single trend table powers multiple dashboard time grains |

## Modifications Made

| Modification | Reason |
|--------------|--------|
| `lifetime_value_actual` = sum of valid order revenue | Distinct from `lifetime_value` on customer record (expected vs actual) |
| Segmentation threshold: High-Value ≥ $10,000 revenue | Documented business rule; adjustable constant |
| `04_customer_segmentation` includes all valid customers via LEFT JOIN | Captures Inactive customers with zero orders |
| Gold output paths under `data/delta/gold/{table_name}` | Consistent with Bronze/Silver local layout |

## Artifacts Produced

| File | Gold Table | Description |
|------|------------|-------------|
| `01_sales_by_product.sql` | `sales_by_product` | Product-level sales metrics |
| `02_revenue_by_customer.sql` | `revenue_by_customer` | Customer-level revenue metrics |
| `03_daily_weekly_trends.sql` | `daily_weekly_trends` | Daily and weekly revenue trends |
| `04_customer_segmentation.sql` | `customer_segmentation` | Customer type segmentation + metrics |
| `create_gold_tables.py` | — | Executes SQL and writes all Gold Delta tables |
| `gold_common.py` | — | Shared path and view utilities |

## Customer Segmentation Rules

| Segment | Rule |
|---------|------|
| Inactive | `total_orders = 0` |
| High-Value | `total_revenue >= 10000` |
| Repeat | `total_orders > 1` (and not High-Value) |
| One-Time | exactly 1 valid order |

## Gold Table Schemas (Output)

### sales_by_product
`product_id`, `product_name`, `category`, `total_orders`, `total_revenue`, `avg_order_value`

### revenue_by_customer
`customer_id`, `customer_name`, `customer_segment`, `total_orders`, `total_revenue`, `avg_order_value`, `lifetime_value_actual`

### daily_weekly_trends
`period_type`, `period_start`, `total_orders`, `total_revenue`, `avg_order_value`

### customer_segmentation
`customer_id`, `customer_name`, `customer_segment`, `country`, `customer_type`, `total_orders`, `total_revenue`, `avg_order_value`, `lifetime_value_expected`, `lifetime_value_actual`, `last_order_date`

## Usage

```bash
# Prerequisite: Silver tables must exist
python src/silver/transform_all.py

# Build all Gold tables
python src/gold/create_gold_tables.py
```

## Path Resolution

| Environment | Silver Source | Gold Target |
|-------------|---------------|-------------|
| Local | `data/delta/silver/{entity}` | `data/delta/gold/{table_name}` |
| Databricks | `/dbfs/tmp/databricks-medallion-pipeline/silver/{entity}` | `/dbfs/tmp/databricks-medallion-pipeline/gold/{table_name}` |
