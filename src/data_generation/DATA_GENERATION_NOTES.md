# Data Generation Notes

## Overview

Synthetic e-commerce datasets are produced by `generate_sample_data.py` using **Faker** and **pandas**, with **PySpark** used post-generation to validate that CSVs are readable in a Spark environment (Databricks-compatible).

**Total intentional defects: 700** (recorded in `data/manifest/defect_manifest.csv`)

## Output Files

| File | Rows | Columns |
|------|------|---------|
| `data/customers.csv` | 10,000 | `customer_id`, `customer_name`, `email`, `country`, `signup_date`, `customer_segment`, `lifetime_value` |
| `data/orders.csv` | 100,000 | `order_id`, `customer_id`, `order_date`, `product_id`, `quantity`, `unit_price`, `total_amount`, `order_status`, `payment_date` |
| `data/products.csv` | 500 | `product_id`, `product_name`, `category`, `price`, `cost`, `stock_quantity`, `reorder_level` |
| `data/manifest/defect_manifest.csv` | 700 | Defect registry (one row per injected defect) |

## Seed Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `RANDOM_SEED` | `42` | Reproducible Python `random` sampling |
| `FAKER_SEED` | `42` | Reproducible Faker names, emails, dates |
| `TARGET_DEFECT_COUNT` | `700` | Total intentional defect instances |
| `NUM_CUSTOMERS` | `10,000` | Customer volume |
| `NUM_ORDERS` | `100,000` | Order volume |
| `NUM_PRODUCTS` | `500` | Product catalog size |

## Defect Distribution (700 Total)

| Category | Count | Details |
|----------|-------|---------|
| **Completeness** | 370 | 50 NULL emails, 100 NULL customer_ids, 200 NULL product_ids, 20 empty product names |
| **Uniqueness** | 50 | 10 duplicate customer_ids, 20 duplicate order_ids, 20 duplicate product_ids |
| **Referential Integrity** | 80 | 50 orphan customer_ids, 30 orphan product_ids |
| **Logic & Type** | 200 | 45 invalid emails, 25 invalid signup dates, 20 negative lifetime values, 25 negative prices, 35 invalid order dates, 25 negative quantities, 25 negative unit prices |
| **Total** | **700** | |

## Defect Manifest Schema

| Column | Description |
|--------|-------------|
| `defect_id` | Sequential ID (1–700) |
| `entity` | customers, products, orders |
| `primary_key_value` | Row identifier affected |
| `category` | Completeness, Uniqueness, Referential Integrity, Logic & Type |
| `error_code` | e.g., `COMP_002`, `UNIQ_001`, `REF_001`, `TYPE_002` |
| `field_name` | Column with defect |
| `defect_description` | Human-readable description |
| `injected_value` | The bad value inserted |

## Running the Generator

```bash
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
python src/data_generation/generate_sample_data.py --no-validate  # skip PySpark if no Java
```

## Validation Checklist

- [ ] `customers.csv` has 10,000 rows
- [ ] `products.csv` has 500 rows
- [ ] `orders.csv` has 100,000 rows
- [ ] `defect_manifest.csv` has exactly 700 rows
- [ ] Defect category totals match table above
