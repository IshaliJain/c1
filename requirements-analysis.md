# Requirements Analysis — E-Commerce Medallion Pipeline

## 1. Project Overview

Build an end-to-end **e-commerce data engineering pipeline** on **Databricks Community Edition** using the **Medallion Architecture** (Bronze → Silver → Gold). The pipeline ingests synthetic transactional data with **700 intentional defects**, applies data quality checks without silently dropping bad records, and produces analytics-ready Gold tables and a dashboard.

## 2. Functional Requirements

### FR-1: Data Generation

| ID | Requirement |
|----|-------------|
| FR-1.1 | Generate synthetic e-commerce datasets for **customers**, **products**, **orders**, and **order_items**. |
| FR-1.2 | Inject exactly **700 intentional data defects** distributed across four quality dimensions (see `data-quality-strategy.md`). |
| FR-1.3 | Write output as CSV (or JSON) to `data/raw/` for local development and `/dbfs/tmp/` for Databricks runs. |
| FR-1.4 | Use explicit schemas; do not rely on schema inference at generation time. |

### FR-2: Bronze Layer (Raw Ingestion)

| ID | Requirement |
|----|-------------|
| FR-2.1 | Ingest raw files in **append-only** mode into Delta Bronze tables. |
| FR-2.2 | Preserve source schema as-is (including defects). |
| FR-2.3 | Add metadata columns: `_ingested_at` (timestamp), `_source_file` (string). |
| FR-2.4 | Support re-runs without corrupting historical Bronze records. |

### FR-3: Silver Layer (Cleansing & Quality)

| ID | Requirement |
|----|-------------|
| FR-3.1 | Enforce explicit PySpark schemas per entity. |
| FR-3.2 | Apply four quality check categories: Completeness, Uniqueness, Referential Integrity, Type/Business. |
| FR-3.3 | **Do not drop** invalid rows; flag each row with `quality_check_result` (and supporting error metadata). |
| FR-3.4 | Deduplicate valid records where business rules require uniqueness (e.g., `customer_id`, `product_id`). |
| FR-3.5 | Standardize formats (trim strings, cast types, normalize enums) where possible without losing auditability. |

### FR-4: Gold Layer (Analytics)

| ID | Requirement |
|----|-------------|
| FR-4.1 | Produce business-level aggregations optimized for BI consumption. |
| FR-4.2 | Example outputs: daily revenue, orders by status, top products, customer lifetime value summary. |
| FR-4.3 | Gold tables must join only from Silver tables that passed applicable quality gates (or clearly separate clean vs. flagged metrics). |

### FR-5: Dashboard

| ID | Requirement |
|----|-------------|
| FR-5.1 | Visualize key Gold metrics (revenue trend, order volume, data quality summary). |
| FR-5.2 | Include a data quality KPI panel showing defect detection rate (target: **100% of 700 intentional errors flagged**). |

### FR-6: Documentation & Traceability

| ID | Requirement |
|----|-------------|
| FR-6.1 | Maintain planning artifacts at repo root (`candidate-info.md`, this file, `design-notes.md`, `data-quality-strategy.md`). |
| FR-6.2 | Capture AI prompt history under `ai-prompts/`. |
| FR-6.3 | Each pipeline script includes inline documentation of rationale and validation steps. |

## 3. Non-Functional Requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NFR-1 | Platform | Must run on Databricks Community Edition (single-node cluster). |
| NFR-2 | Language | Python 3.10+ with PySpark; Spark SQL for transformations. |
| NFR-3 | Storage | Delta Lake for all layered tables. |
| NFR-4 | Scalability | Design supports incremental file ingestion; initial scope is batch on synthetic data. |
| NFR-5 | Idempotency | Pipeline stages can be re-run without duplicating cleansed Silver/Gold outputs. |
| NFR-6 | Observability | Structured logging at each stage (row counts, defect counts, pass/fail summaries). |
| NFR-7 | Security | No hardcoded credentials; use environment variables or Databricks secrets. |
| NFR-8 | Portability | Paths configurable for local (`data/`) vs. Databricks (`/dbfs/tmp/`). |
| NFR-9 | Maintainability | Modular code under `src/` with one responsibility per module. |
| NFR-10 | Testability | Quality checks produce measurable counts verifiable against the 700-defect manifest. |

## 4. Data Entities (High-Level)

| Entity | Primary Key | Key Foreign Keys | Description |
|--------|-------------|------------------|-------------|
| `customers` | `customer_id` | — | Customer profile and contact info |
| `products` | `product_id` | — | Product catalog and pricing |
| `orders` | `order_id` | `customer_id` | Order header (date, status, total) |
| `order_items` | `order_item_id` | `order_id`, `product_id` | Line-item detail (quantity, unit price) |

## 5. Edge Cases

| # | Edge Case | Expected Behavior |
|---|-----------|-------------------|
| E-1 | Duplicate `customer_id` in source | Flag uniqueness violation; retain all rows in Silver with error code |
| E-2 | `order` references non-existent `customer_id` | Flag referential integrity error on order row |
| E-3 | `order_item` references missing `order_id` or `product_id` | Flag FK violation; do not silently join-drop |
| E-4 | NULL in required field (e.g., `customer_id`, `order_date`) | Flag completeness error |
| E-5 | Invalid type (string in numeric column, malformed date) | Flag type/business error; attempt safe cast where possible |
| E-6 | Negative `unit_price` or `quantity` | Flag business rule violation |
| E-7 | Future `order_date` or implausible `registration_date` | Flag business rule violation |
| E-8 | Empty source file | Log warning; ingest zero rows; do not fail entire pipeline |
| E-9 | Schema drift (new column in source) | Bronze preserves as-is; Silver schema enforcement flags unknown/mismatched fields |
| E-10 | Re-ingestion of same file | Bronze appends with new `_ingested_at`; Silver dedup logic uses business keys + ingestion metadata |
| E-11 | Row with multiple defects | Accumulate all applicable error codes in `quality_check_result` |
| E-12 | All rows in a batch are defective | Silver completes; Gold uses only `PASS` rows or reports zero clean metrics explicitly |

## 6. Assumptions

| # | Assumption |
|---|------------|
| A-1 | Databricks Community Edition single-node cluster is sufficient for synthetic dataset volume. |
| A-2 | Initial load is **batch** (not streaming); streaming is out of scope for v1. |
| A-3 | Synthetic data volume is modest (thousands of rows per entity, not billions). |
| A-4 | CSV is the canonical raw format for Bronze ingestion. |
| A-5 | Unity Catalog may not be available on CE; tables use hive_metastore or path-based Delta tables under `/dbfs/tmp/`. |
| A-6 | Dashboard is built with Databricks-native tooling (SQL Dashboard or notebook visualizations). |
| A-7 | PII in synthetic data is fake; no real customer data is used. |
| A-8 | The 700 defects are pre-planned and documented in a defect manifest during data generation. |
| A-9 | "Do not drop invalid rows" applies to Silver; Gold may filter to clean rows for analytics but must document the filter. |
| A-10 | One developer (candidate) owns the repo; no multi-tenant access control required. |

## 7. Out of Scope (v1)

- Real-time streaming ingestion (Kafka / Event Hubs)
- Production CI/CD and automated Databricks deployment
- Unity Catalog governance and fine-grained ACLs
- External orchestration (Airflow, ADF) — manual or simple Databricks Job only
- Machine learning models beyond basic analytics aggregations

## 8. Success Criteria

1. All four medallion layers execute end-to-end on Databricks CE.
2. All **700 intentional defects** are detected and flagged in Silver.
3. Gold tables power a dashboard with at least revenue, orders, and quality KPIs.
4. Repository contains complete documentation and AI prompt traceability.
5. No credentials or absolute local paths are committed to source control.
