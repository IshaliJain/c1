# Design Notes — Bronze, Silver, Gold & Dashboard Architecture

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION                              │
│  src/data_generation/  →  data/raw/{customers,products,orders,...}  │
│  (700 intentional defects injected + defect manifest)             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ CSV files
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (src/bronze/)                                        │
│  Raw ingestion · append-only · schema preserved                     │
│  + _ingested_at, _source_file                                       │
│  Delta: bronze.customers | bronze.products | bronze.orders | ...  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (src/silver/)                                        │
│  Schema enforcement · cleansing · 4 quality checks                │
│  quality_check_result per row · deduplication of valid records      │
│  Delta: silver.customers | silver.products | silver.orders | ...    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (src/gold/)                                            │
│  Business aggregations · star-schema-friendly facts & summaries     │
│  Delta: gold.daily_revenue | gold.product_performance | ...         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DASHBOARD  (src/dashboard/)                                      │
│  SQL queries + visualizations over Gold tables                      │
│  Revenue · orders · product rankings · data quality KPIs            │
└─────────────────────────────────────────────────────────────────────┘
```

## 2. Repository Layout

```
databricks-medallion-pipeline/
├── candidate-info.md
├── requirements-analysis.md
├── design-notes.md                 ← this file
├── data-quality-strategy.md
├── data/
│   ├── raw/                        # Generated CSV source files
│   ├── manifest/                   # Defect manifest (700 errors)
│   └── .gitkeep
├── src/
│   ├── data_generation/            # Synthetic data + defect injection
│   ├── bronze/                       # Raw → Delta Bronze
│   ├── silver/                       # Bronze → cleansed Silver
│   ├── gold/                         # Silver → analytics Gold
│   └── dashboard/                    # Dashboard SQL / notebook exports
├── ai-prompts/                       # Cursor prompt history
└── .cursor/rules/
    └── databricks-medallion.mdc
```

## 3. Bronze Layer Design

### Purpose

Land raw data exactly as received. Bronze is the **system of record for source fidelity** and supports reprocessing if Silver/Gold logic changes.

### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Append-only writes | Preserves full ingestion history for audit and replay |
| No cleansing | Defects must survive to Silver for quality testing |
| Metadata columns | `_ingested_at` and `_source_file` enable lineage and incremental debugging |
| Explicit read schema optional | Bronze may read with `mergeSchema` for drift tolerance; Silver enforces strict schema |
| Delta format | ACID, time travel, and consistent Databricks CE support |

### Bronze Tables

| Table | Source Path | Notes |
|-------|-------------|-------|
| `bronze.customers` | `data/raw/customers/` | All columns as string or inferred; defects preserved |
| `bronze.products` | `data/raw/products/` | |
| `bronze.orders` | `data/raw/orders/` | |
| `bronze.order_items` | `data/raw/order_items/` | |

### Bronze Processing Flow

1. Discover new files in source directory.
2. Read with Spark (`spark.read.option("header", true)`).
3. Add `_ingested_at = current_timestamp()`, `_source_file = input_file_name()`.
4. Append to Delta table at configured path.

## 4. Silver Layer Design

### Purpose

Transform Bronze into **cleansed, schema-enforced** tables while **flagging** (not dropping) every data quality issue.

### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Explicit `StructType` schemas | Prevents silent type coercion; required by project standards |
| `quality_check_result` column | Single auditable field (JSON array or pipe-delimited codes) per row |
| Row-level flagging | Meets "do not drop invalid rows" requirement |
| Deduplication window | Keep latest valid record per business key; flag duplicates |
| Separate `is_valid` boolean | Simplifies Gold filtering while retaining full Silver history |

### Silver Schema Additions (all entities)

| Column | Type | Description |
|--------|------|-------------|
| `quality_check_result` | `STRING` | Error codes (e.g., `COMP_001\|REF_003`) or `PASS` |
| `is_valid` | `BOOLEAN` | `true` only when all checks pass |
| `_silver_processed_at` | `TIMESTAMP` | Processing timestamp |

### Silver Processing Flow

1. Read Bronze Delta table.
2. Cast columns to enforced schema (nullable fields explicit).
3. Run four check modules (see `data-quality-strategy.md`).
4. Merge check results into `quality_check_result`.
5. Apply deduplication logic for uniqueness violations on valid keys.
6. Write to Silver Delta table (overwrite partition or merge by key — TBD in implementation).

## 5. Gold Layer Design

### Purpose

Deliver **business-ready analytics** for dashboard consumption. Gold reads from Silver and applies business logic on clean data.

### Gold Tables (Planned)

| Table | Grain | Key Metrics / Columns |
|-------|-------|----------------------|
| `gold.daily_revenue` | Day | `order_date`, `total_revenue`, `order_count`, `avg_order_value` |
| `gold.product_performance` | Product | `product_id`, `product_name`, `units_sold`, `revenue`, `rank` |
| `gold.customer_summary` | Customer | `customer_id`, `total_orders`, `total_spend`, `first_order_date`, `last_order_date` |
| `gold.order_status_summary` | Status × Day | `order_status`, `order_date`, `count`, `revenue` |
| `gold.data_quality_summary` | Check × Entity | `entity`, `check_category`, `error_code`, `flagged_count`, `detection_rate` |

### Gold Design Principles

- **Filter**: `WHERE is_valid = true` for revenue/BI metrics (documented explicitly).
- **Quality table**: Includes ALL flagged rows aggregated — proves 700/700 detection.
- **Denormalize for BI**: Pre-join product and customer attributes where it reduces dashboard query complexity.
- **Partition** by `order_date` (or `year`, `month`) if volume warrants it.

## 6. Dashboard Design

### Purpose

Provide stakeholders a visual summary of e-commerce performance and pipeline data quality.

### Planned Panels

| Panel | Source Table | Visualization |
|-------|--------------|---------------|
| Daily Revenue Trend | `gold.daily_revenue` | Line chart |
| Orders by Status | `gold.order_status_summary` | Bar / pie chart |
| Top 10 Products | `gold.product_performance` | Horizontal bar chart |
| Data Quality Scorecard | `gold.data_quality_summary` | KPI counters + table |
| Defect Detection Rate | `gold.data_quality_summary` | Single stat: `detected / 700` |

### Implementation Options (Databricks CE)

1. **Databricks SQL Dashboard** — queries saved in `src/dashboard/queries/`
2. **Notebook visualizations** — `src/dashboard/dashboard_notebook.py` using `display()` and `matplotlib` if needed

## 7. Configuration & Path Strategy

```python
# Conceptual — implemented in shared config module
ENV = "local" | "databricks"

PATHS = {
    "local": {
        "raw": "data/raw/",
        "bronze": "data/delta/bronze/",
        "silver": "data/delta/silver/",
        "gold":   "data/delta/gold/",
    },
    "databricks": {
        "raw":    "/dbfs/tmp/databricks-medallion-pipeline/raw/",
        "bronze": "/dbfs/tmp/databricks-medallion-pipeline/bronze/",
        "silver": "/dbfs/tmp/databricks-medallion-pipeline/silver/",
        "gold":   "/dbfs/tmp/databricks-medallion-pipeline/gold/",
    },
}
```

No absolute local paths (e.g., `/Users/...`) in committed code.

## 8. Execution Order

```
1. data_generation/generate_all.py
2. bronze/ingest_bronze.py
3. silver/transform_silver.py
4. gold/build_gold.py
5. dashboard/ (manual or scheduled refresh)
```

Each stage logs: input row count → output row count → defect count.

## 9. Error Handling

| Scenario | Behavior |
|----------|----------|
| Missing source directory | Fail fast with clear log message |
| Bronze read failure | Abort stage; do not partially write Silver/Gold |
| Silver quality check module error | Log and raise; do not produce misleading Gold |
| Zero valid Gold rows | Dashboard shows empty state + quality alert |

## 10. Future Enhancements (Post-v1)

- Auto Loader for incremental file ingestion
- Delta Live Tables with expectations
- Unity Catalog migration
- Scheduled Databricks Job with email alerts on quality regression
