# Bronze Layer — AI Prompt History

**Date:** 2026-08-30  
**Tool:** Cursor Agent Mode  
**Branch:** `feature/databricks-medallion-pipeline`

## Prompt

```
Create PySpark scripts in src/bronze/ (01_ingest_customers.py, 02_ingest_orders.py, 03_ingest_products.py, and ingest_all.py).
Requirements:
Read raw CSV files from DBFS or local path into Delta tables.
Define explicit PySpark schemas (StructType) for each source.
Do not apply cleaning or transformations. Add metadata columns: _ingestion_timestamp and _source_file.
Log ingestion counts and target Delta table creation.
Append prompt history details to ai-prompts/bronze-layer.md.
```

## Accepted Suggestions

| Suggestion | Rationale |
|------------|-----------|
| Shared `bronze_common.py` module | DRY path resolution, schemas, and ingest logic across all scripts |
| All source columns as `StringType` | Preserves raw CSV values and intentional defects without silent casting |
| Auto-detect Databricks via `DATABRICKS_RUNTIME_VERSION` | No manual env flag needed on cluster |
| Append-only Delta writes (`mode("append")`) | Matches medallion Bronze design — preserves ingestion history |
| `mergeSchema=true` on write | Tolerates metadata column addition on first run |
| `ingest_all.py` runs customers → products → orders | Parent entities ingested before child orders |

## Modifications Made

| Modification | Reason |
|--------------|--------|
| Metadata column named `_ingestion_timestamp` (not `_ingested_at`) | Per explicit prompt requirement; design-notes uses `_ingested_at` — Silver can alias if needed |
| Local source path: `data/{entity}.csv` | Matches data generator output location |
| Local Delta target: `data/delta/bronze/{entity}` | Keeps Delta tables under gitignored `data/` |
| Databricks paths under `/dbfs/tmp/databricks-medallion-pipeline/` | Portable DBFS location for Community Edition |
| `ingest_all` order: customers, products, then orders | FK parent tables available before orders (no FK enforcement at Bronze) |

## Artifacts Produced

| File | Description |
|------|-------------|
| `src/bronze/bronze_common.py` | Shared schemas, path resolution, ingest function |
| `src/bronze/01_ingest_customers.py` | Ingest `customers.csv` → Delta |
| `src/bronze/02_ingest_orders.py` | Ingest `orders.csv` → Delta |
| `src/bronze/03_ingest_products.py` | Ingest `products.csv` → Delta |
| `src/bronze/ingest_all.py` | Orchestrates all three ingestions |

## Schemas Defined

### customers (7 source columns + 2 metadata)

`customer_id`, `customer_name`, `email`, `country`, `signup_date`, `customer_segment`, `lifetime_value`

### orders (9 source columns + 2 metadata)

`order_id`, `customer_id`, `order_date`, `product_id`, `quantity`, `unit_price`, `total_amount`, `order_status`, `payment_date`

### products (7 source columns + 2 metadata)

`product_id`, `product_name`, `category`, `price`, `cost`, `stock_quantity`, `reorder_level`

## Path Resolution

| Environment | Source | Target |
|-------------|--------|--------|
| Local | `<project>/data/{entity}.csv` | `<project>/data/delta/bronze/{entity}` |
| Databricks | `/dbfs/tmp/databricks-medallion-pipeline/data/{entity}.csv` | `/dbfs/tmp/databricks-medallion-pipeline/bronze/{entity}` |

## Usage

```bash
# Run individual entity
python src/bronze/01_ingest_customers.py

# Run all Bronze ingestions
python src/bronze/ingest_all.py
```

On Databricks, upload CSVs to DBFS and run the same scripts as notebook cells or a Job.
