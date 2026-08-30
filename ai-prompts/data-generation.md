# Data Generation — AI Prompt History

**Date:** 2026-08-30  
**Tool:** Cursor Agent Mode  
**Branch:** `feature/databricks-medallion-pipeline`

## Prompt

```
Write src/data_generation/generate_sample_data.py using Python's Faker and pandas/PySpark.
Requirements:
Generate 3 CSV files into the data/ directory:
customers.csv (10,000 rows): customer_id, customer_name, email, country, signup_date, customer_segment, lifetime_value.
orders.csv (100,000 rows): order_id, customer_id, order_date, product_id, quantity, unit_price, total_amount, order_status, payment_date.
products.csv (500 rows): product_id, product_name, category, price, cost, stock_quantity, reorder_level.
Introduce the following intentional data quality issues (exactly ~700 defective rows total):
customers.csv: 50 NULL emails, 10 duplicate customer_ids.
orders.csv: 100 NULL customer_ids, 200 NULL product_ids, 50 orphan customer_ids (not in customers), 30 orphan product_ids (not in products), 20 duplicate order_ids.
Generate src/data_generation/DATA_GENERATION_NOTES.md explaining how the data was generated and documenting the seed parameters.
Log the output to ai-prompts/data-generation.md tracking prompt details, accepted suggestions, and modifications made.
```

## Accepted Suggestions

| Suggestion | Rationale |
|------------|-----------|
| Fixed seeds (`RANDOM_SEED=42`, `FAKER_SEED=42`) | Reproducible defect injection across runs |
| Sequential ID formats (`CUST-`, `PROD-`, `ORD-`) | Aligns with `data-quality-strategy.md` type checks |
| Non-overlapping defect row indices in orders | Each defective row maps to one primary defect category |
| Orphan IDs use `ORPHAN-CUST-*` / `ORPHAN-PROD-*` prefixes | Easy to identify and validate referential integrity failures |
| NULLs written as empty CSV fields | Standard CSV convention; Bronze preserves as-is |
| PySpark validation after pandas write | Confirms Databricks-readable output without messy Spark part-files |
| `requirements.txt` with pinned versions | `faker`, `pandas`, `pyspark` for local generation and validation |

## Modifications Made

| Modification | Reason |
|--------------|--------|
| Defect total documented as **460**, not 700 | User-specified defect counts sum to 460; 700 is the broader project target across all entities and check types |
| Products file has **no defects** | Not listed in prompt; kept clean for FK baseline |
| `payment_date` NULL for Pending/Cancelled orders | Realistic business logic (not counted as intentional defects) |
| `--output-dir` CLI flag | Supports `data/` (prompt) or `data/raw/` (design-notes) without code changes |
| `--no-validate` flag | Allows generation without Java/Spark installed locally |

## Artifacts Produced

| File | Description |
|------|-------------|
| `src/data_generation/generate_sample_data.py` | Main generator script |
| `src/data_generation/DATA_GENERATION_NOTES.md` | Seed params, schemas, defect manifest |
| `requirements.txt` | Python dependencies |
| `data/customers.csv` | Generated at runtime (gitignored) |
| `data/orders.csv` | Generated at runtime (gitignored) |
| `data/products.csv` | Generated at runtime (gitignored) |

## Defect Count Verification

| Defect | Expected | Implementation |
|--------|----------|----------------|
| NULL emails | 50 | `random.sample` on 50 customer indices |
| Duplicate customer_ids | 10 | 10 rows assigned IDs from first 10 customers |
| NULL customer_ids (orders) | 100 | Non-overlapping index injection |
| NULL product_ids (orders) | 200 | Non-overlapping index injection |
| Orphan customer_ids | 50 | `ORPHAN-CUST-001` through `ORPHAN-CUST-050` |
| Orphan product_ids | 30 | `ORPHAN-PROD-001` through `ORPHAN-PROD-030` |
| Duplicate order_ids | 20 | 20 rows assigned IDs from first 20 orders |
| **Total** | **460** | Logged by `log_defect_summary()` at runtime |
