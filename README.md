# Databricks Medallion E-Commerce Data Pipeline

End-to-end PySpark data engineering pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for synthetic e-commerce data on **Databricks Community Edition**.

**Candidate:** Ishali Jain  
**Repository:** https://github.com/IshaliJain/c1  
**Branch:** `feature/databricks-medallion-pipeline`

---

## Architecture

```
data/ (CSV)  →  Bronze (raw Delta)  →  Silver (quality-flagged Delta)  →  Gold (analytics Delta)  →  Dashboard
```

| Layer | Purpose | Key Output |
|-------|---------|------------|
| **Data Generation** | Synthetic CSVs with 460 intentional defects | `data/*.csv` |
| **Bronze** | Raw ingestion, append-only, schema preserved | `bronze/{customers,orders,products}` |
| **Silver** | 4 quality checks, row-level flagging (no drops) | `silver/{entity}` + `quality_summary` |
| **Gold** | Business aggregations for BI | `gold/{sales_by_product,...}` |
| **Dashboard** | SQL queries for Databricks SQL Dashboards | `src/dashboard/dashboard_queries.sql` |

---

## Project Structure

```
databricks-medallion-pipeline/
├── README.md                          ← this file
├── candidate-info.md
├── requirements-analysis.md
├── design-notes.md
├── data-quality-strategy.md
├── tool-workflow.md
├── debugging-notes.md
├── reflection.md
├── final-ai-usage-summary.md
├── requirements.txt
├── data/                              # Generated CSVs + Delta tables (gitignored)
├── ai-prompts/                        # AI prompt history per phase
├── .cursor/rules/
│   └── databricks-medallion.mdc
└── src/
    ├── data_generation/
    │   ├── generate_sample_data.py
    │   └── DATA_GENERATION_NOTES.md
    ├── bronze/
    │   ├── bronze_common.py
    │   ├── 01_ingest_customers.py
    │   ├── 02_ingest_orders.py
    │   ├── 03_ingest_products.py
    │   └── ingest_all.py
    ├── silver/
    │   ├── silver_common.py
    │   ├── quality_checks.py
    │   ├── 01_transform_customers.py
    │   ├── 02_transform_orders.py
    │   ├── 03_transform_products.py
    │   └── transform_all.py
    ├── gold/
    │   ├── gold_common.py
    │   ├── 01_sales_by_product.sql
    │   ├── 02_revenue_by_customer.sql
    │   ├── 03_daily_weekly_trends.sql
    │   ├── 04_customer_segmentation.sql
    │   └── create_gold_tables.py
    └── dashboard/
        └── dashboard_queries.sql
```

---

## Prerequisites

### Local Development (optional)

- Python 3.10+
- `pip install -r requirements.txt`
- Java 8+ (required for local PySpark; optional with `--no-validate`)

### Databricks Community Edition

- Active Databricks CE account
- Single-node cluster (Runtime 13.3+ or 14.3 LTS recommended)
- Repo cloned or files uploaded to workspace

---

## Running on Databricks Community Edition

### Step 1: Clone the Repository

In Databricks Repos:

1. Go to **Repos** → **Add Repo**
2. URL: `https://github.com/IshaliJain/c1`
3. Branch: `feature/databricks-medallion-pipeline`

Or upload the project folder to your workspace.

### Step 2: Create a Cluster

1. Go to **Compute** → **Create Cluster**
2. Select **Single Node** (Community Edition)
3. Runtime: **14.3 LTS** (or latest available)
4. Start the cluster

### Step 3: Generate Sample Data

**Option A — Run locally, upload CSVs:**

```bash
pip install -r requirements.txt
python src/data_generation/generate_sample_data.py
```

Upload `data/customers.csv`, `data/orders.csv`, `data/products.csv` to:
```
/dbfs/tmp/databricks-medallion-pipeline/data/
```

**Option B — Run on cluster** (attach repo to cluster, open a notebook):

```python
%run /Repos/<your-user>/c1/src/data_generation/generate_sample_data
```

Or in a notebook cell:

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<your-user>/c1/src/data_generation")
from generate_sample_data import generate_all
from pathlib import Path

generate_all(Path("/dbfs/tmp/databricks-medallion-pipeline/data"), validate=True)
```

### Step 4: Run Bronze Ingestion

```python
%run /Repos/<your-user>/c1/src/bronze/ingest_all
```

Or from a terminal on the cluster driver:

```bash
python src/bronze/ingest_all.py
```

**Expected output:** ~10,000 customers, ~500 products, ~100,000 orders appended to Bronze Delta.

### Step 5: Run Silver Transformations

```python
%run /Repos/<your-user>/c1/src/silver/transform_all
```

**Expected output:** Quality-flagged Silver tables + `quality_summary` with pass/fail metrics per check category.

Verify failures were detected:

```python
spark.read.format("delta").load(
    "/dbfs/tmp/databricks-medallion-pipeline/silver/orders"
).groupBy("quality_check_result").count().show()
```

### Step 6: Run Gold Aggregations

```python
%run /Repos/<your-user>/c1/src/gold/create_gold_tables
```

**Expected Gold tables:**

| Table | Description |
|-------|-------------|
| `gold/sales_by_product` | Product-level revenue metrics |
| `gold/revenue_by_customer` | Customer-level revenue metrics |
| `gold/daily_weekly_trends` | Daily and weekly revenue trends |
| `gold/customer_segmentation` | High-Value / Repeat / One-Time / Inactive |

### Step 7: Build SQL Dashboard

1. Go to **SQL** → **SQL Editor**
2. Run the view setup from `src/dashboard/dashboard_queries.sql`:

```sql
CREATE OR REPLACE TEMP VIEW gold_sales_by_product AS
  SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/sales_by_product`;

CREATE OR REPLACE TEMP VIEW gold_revenue_by_customer AS
  SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/revenue_by_customer`;

CREATE OR REPLACE TEMP VIEW gold_daily_weekly_trends AS
  SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/daily_weekly_trends`;

CREATE OR REPLACE TEMP VIEW gold_customer_segmentation AS
  SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/customer_segmentation`;
```

3. Create a **SQL Dashboard** with these visualizations:

| Query | Chart Type |
|-------|-----------|
| Top 10 Products by Revenue | Horizontal bar chart |
| Customer Revenue Distribution | Histogram (bar chart) |
| Customer Segmentation | Pie / donut chart |
| Daily Revenue Trend | Line chart |

---

## Running Locally (Full Pipeline)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate data
python src/data_generation/generate_sample_data.py --no-validate

# 3. Bronze ingestion (requires Java for PySpark)
python src/bronze/ingest_all.py

# 4. Silver transformations
python src/silver/transform_all.py

# 5. Gold aggregations
python src/gold/create_gold_tables.py
```

> Local PySpark requires Java. Use `--no-validate` for data generation if Java is not installed.

---

## Data Quality Checks (Silver)

| Category | What's Checked |
|----------|---------------|
| **Completeness** | NULL/empty `email`, `customer_id`, `product_id` |
| **Uniqueness** | Duplicate `customer_id`, `order_id` (window functions) |
| **Referential Integrity** | Orphan `customer_id` / `product_id` in orders |
| **Logic & Type** | Valid dates, positive numeric values |

Invalid rows are **flagged, not deleted**. The `quality_check_result` column contains `PASSED` or `FAILED_*` codes.

---

## Intentional Data Defects

460 defects injected during data generation:

| Entity | Defects |
|--------|---------|
| customers | 50 NULL emails, 10 duplicate customer_ids |
| orders | 100 NULL customer_ids, 200 NULL product_ids, 50 orphan customers, 30 orphan products, 20 duplicate order_ids |

See `src/data_generation/DATA_GENERATION_NOTES.md` for full details.

---

## Documentation Index

| File | Description |
|------|-------------|
| `tool-workflow.md` | AI context-setting, validation, debugging, privacy |
| `debugging-notes.md` | Known issues and fixes |
| `reflection.md` | Project reflection and lessons learned |
| `final-ai-usage-summary.md` | What AI got right/wrong |
| `ai-prompts/` | Per-phase AI prompt history |
| `design-notes.md` | Architecture design decisions |
| `data-quality-strategy.md` | Quality check specifications |

---

## License

This project was created as a data engineering assessment submission. Synthetic data only — no real customer information.
