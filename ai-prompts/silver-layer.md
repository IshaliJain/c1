# Silver Layer — AI Prompt History

**Date:** 2026-08-30  
**Tool:** Cursor Agent Mode  
**Branch:** `feature/databricks-medallion-pipeline`

## Prompt

```
Build PySpark scripts in src/silver/ implementing the 4 required data quality checks:
Completeness: Flag NULLs in critical fields (email, customer_id, product_id).
Uniqueness: Identify duplicate keys (order_id, customer_id) using window functions.
Referential Integrity: Validate customer_id and product_id presence via left anti-joins.
Logic & Type Check: Validate dates and positive numeric values.

Strategy:
Do not delete bad rows. Add a column quality_check_result (e.g., 'PASSED' or 'FAILED_COMPLETENESS').
Create a quality summary metrics table/report showing the total row count, passed count, failed count, and pass percentage for each check.
Save clean/flagged data to Silver Delta tables.
Append prompt history details to ai-prompts/silver-layer.md.
```

## Accepted Suggestions

| Suggestion | Rationale |
|------------|-----------|
| Shared `silver_common.py` + `quality_checks.py` | DRY path resolution, result building, and reusable check functions |
| `quality_check_result` pipe-delimited for multiple failures | e.g. `FAILED_COMPLETENESS\|FAILED_REFERENTIAL` when row fails multiple checks |
| `is_valid` boolean alongside `quality_check_result` | Simplifies Gold filtering per data-quality-strategy.md |
| Window `count(*) over (partitionBy key)` for uniqueness | Explicit requirement; flags all rows in duplicate groups |
| Left join for referential checks (equivalent to anti-join orphan detection) | Identifies FK values not present in parent Silver tables |
| `transform_all.py` runs customers → products → orders | Parent Silver tables required before order FK validation |
| Overwrite Silver Delta on each run | Idempotent full-batch refresh for v1 |

## Modifications Made

| Modification | Reason |
|--------------|--------|
| Products uniqueness on `product_id` (not in prompt) | Required for catalog integrity; prompt only listed order/customer keys explicitly |
| Customers completeness checks `customer_id` + `email` | Both are critical fields per data-quality-strategy.md |
| Orders referential checks use Silver parent tables | Validates against cleansed parent keys already in Silver |
| Internal `_failed_*` boolean columns retained in Silver output | Enables per-check summary metrics; can be dropped in Gold |
| Quality summary at `silver/quality_summary` Delta table | Centralized metrics report with `.show()` logging |

## Artifacts Produced

| File | Description |
|------|-------------|
| `src/silver/silver_common.py` | Paths, quality result builder, summary metrics, Delta I/O |
| `src/silver/quality_checks.py` | Four check category implementations |
| `src/silver/01_transform_customers.py` | Customers Bronze → Silver |
| `src/silver/03_transform_products.py` | Products Bronze → Silver |
| `src/silver/02_transform_orders.py` | Orders Bronze → Silver with FK checks |
| `src/silver/transform_all.py` | Orchestrator + combined quality summary |

## Quality Check Mapping

| Category | Customers | Products | Orders |
|----------|-----------|----------|--------|
| Completeness | `customer_id`, `email` | `product_id` | `customer_id`, `product_id` |
| Uniqueness | `customer_id` | `product_id` | `order_id` |
| Referential | — | — | `customer_id` → customers, `product_id` → products |
| Logic & Type | `signup_date`, `lifetime_value` | `price`, `cost`, `stock_quantity`, `reorder_level` | dates, `quantity`, `unit_price`, `total_amount` |

## quality_check_result Values

| Value | Meaning |
|-------|---------|
| `PASSED` | All four checks passed |
| `FAILED_COMPLETENESS` | NULL/empty critical field |
| `FAILED_UNIQUENESS` | Duplicate key detected |
| `FAILED_REFERENTIAL` | Orphan foreign key |
| `FAILED_LOGIC_TYPE` | Invalid date or non-positive numeric |

Multiple failures are pipe-delimited: `FAILED_COMPLETENESS|FAILED_REFERENTIAL`

## Quality Summary Schema

| Column | Description |
|--------|-------------|
| `entity` | customers, products, orders |
| `check_category` | Completeness, Uniqueness, Referential Integrity, Logic & Type |
| `total_rows` | Total rows in entity |
| `passed_count` | Rows passing that specific check |
| `failed_count` | Rows failing that specific check |
| `pass_percentage` | `(passed / total) * 100` |

## Usage

```bash
# Run individual entity
python src/silver/01_transform_customers.py

# Run full Silver pipeline + quality summary
python src/silver/transform_all.py
```

Prerequisite: Bronze Delta tables must exist (run `src/bronze/ingest_all.py` first).
