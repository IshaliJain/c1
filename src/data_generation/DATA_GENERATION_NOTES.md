# Data Generation Notes

## Overview

Synthetic e-commerce datasets are produced by `generate_sample_data.py` using **Faker** and **pandas**, with **PySpark** used post-generation to validate that CSVs are readable in a Spark environment (Databricks-compatible).

## Output Files

| File | Rows | Columns |
|------|------|---------|
| `data/customers.csv` | 10,000 | `customer_id`, `customer_name`, `email`, `country`, `signup_date`, `customer_segment`, `lifetime_value` |
| `data/orders.csv` | 100,000 | `order_id`, `customer_id`, `order_date`, `product_id`, `quantity`, `unit_price`, `total_amount`, `order_status`, `payment_date` |
| `data/products.csv` | 500 | `product_id`, `product_name`, `category`, `price`, `cost`, `stock_quantity`, `reorder_level` |

## Seed Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `RANDOM_SEED` | `42` | Reproducible Python `random` sampling |
| `FAKER_SEED` | `42` | Reproducible Faker names, emails, dates |
| `NUM_CUSTOMERS` | `10,000` | Customer volume |
| `NUM_ORDERS` | `100,000` | Order volume |
| `NUM_PRODUCTS` | `500` | Product catalog size |

## ID Formats

| Entity | Pattern | Example |
|--------|---------|---------|
| Customer | `CUST-{5-digit}` | `CUST-00042` |
| Product | `PROD-{5-digit}` | `PROD-00123` |
| Order | `ORD-{6-digit}` | `ORD-000789` |
| Orphan customer | `ORPHAN-CUST-{3-digit}` | `ORPHAN-CUST-001` |
| Orphan product | `ORPHAN-PROD-{3-digit}` | `ORPHAN-PROD-001` |

## Generation Logic

### Products (clean)

- 500 products with random category, price, cost, and stock levels.
- No intentional defects injected in this phase.

### Customers

- 10,000 unique `customer_id` values generated sequentially.
- `signup_date` range: 2018-01-01 to 2025-12-31.
- `customer_segment`: random from Bronze / Silver / Gold / Platinum.
- `lifetime_value`: uniform random 0–25,000 (2 decimal places).

### Orders

- 100,000 unique `order_id` values generated sequentially.
- `customer_id` and `product_id` sampled from valid parent IDs.
- `total_amount` = `quantity` × `unit_price`.
- `payment_date` is NULL when `order_status` is Pending or Cancelled.

## Intentional Data Quality Defects

Defects are injected **after** clean data generation on non-overlapping row indices (except duplicate keys which reuse existing values).

| # | File | Defect Type | Category | Count | Description |
|---|------|-------------|----------|-------|-------------|
| 1 | customers | NULL `email` | Completeness | 50 | Email field left empty |
| 2 | customers | Duplicate `customer_id` | Uniqueness | 10 | 10 rows reuse IDs from first 10 customers |
| 3 | orders | NULL `customer_id` | Completeness | 100 | Customer FK missing |
| 4 | orders | NULL `product_id` | Completeness | 200 | Product FK missing |
| 5 | orders | Orphan `customer_id` | Referential Integrity | 50 | `ORPHAN-CUST-*` IDs not in customers |
| 6 | orders | Orphan `product_id` | Referential Integrity | 30 | `ORPHAN-PROD-*` IDs not in products |
| 7 | orders | Duplicate `order_id` | Uniqueness | 20 | 20 rows reuse IDs from first 20 orders |

**Total defective row injections: 460**

> Note: The broader project targets 700 defects across all entities and check types. This phase covers the 460 defects specified for the initial three-file dataset. Additional defects (type/business rule violations) will be added in subsequent phases.

## NULL Representation

NULL values are written as **empty CSV fields** (`na_rep=""` in pandas). Bronze ingestion should treat empty strings as NULL during Silver casting.

## Running the Generator

```bash
# Install dependencies
pip install -r requirements.txt

# Generate CSVs into data/
python src/data_generation/generate_sample_data.py

# Custom output directory, skip PySpark validation
python src/data_generation/generate_sample_data.py --output-dir data/raw --no-validate
```

## PySpark Validation

After writing CSVs, the script optionally:

1. Starts a local Spark session.
2. Reads each CSV with `header=true`, `inferSchema=false`.
3. Asserts row counts match expected volumes.

This confirms files are consumable before Bronze ingestion on Databricks.

## Validation Checklist

After running, confirm:

- [ ] `customers.csv` has 10,000 rows
- [ ] `products.csv` has 500 rows
- [ ] `orders.csv` has 100,000 rows
- [ ] 50 customers have empty `email`
- [ ] 10 customer rows share duplicate `customer_id` values
- [ ] 100 orders have empty `customer_id`
- [ ] 200 orders have empty `product_id`
- [ ] 50 orders reference `ORPHAN-CUST-*` customer IDs
- [ ] 30 orders reference `ORPHAN-PROD-*` product IDs
- [ ] 20 order rows share duplicate `order_id` values
