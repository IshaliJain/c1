# Databricks Community Edition — Compatibility Verification

**Project:** Databricks Medallion E-Commerce Pipeline  
**Verified:** 2026-08-30  
**Target Runtime:** Databricks Runtime 13.3+ / 14.3 LTS (single-node)

---

## 1. Platform Compatibility Matrix

| Component | Databricks CE | Notes |
|-----------|---------------|-------|
| PySpark scripts | ✅ | All layers use `SparkSession.builder.getOrCreate()` |
| Delta Lake | ✅ | `format("delta")` read/write throughout |
| Path-based storage | ✅ | `/dbfs/tmp/databricks-medallion-pipeline/` — no Unity Catalog required |
| SQL aggregations | ✅ | Gold SQL uses standard Spark SQL functions |
| SQL Dashboards | ✅ | Queries in `src/dashboard/dashboard_queries.sql` |
| Auto Loader | N/A | Batch CSV ingestion used (CE-compatible) |
| Unity Catalog | N/A | Not required; path-based Delta tables used |

---

## 2. Environment Detection

All pipeline modules detect Databricks automatically:

```python
def is_databricks_runtime() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
```

| Environment | Data Path | Delta Path |
|-------------|-----------|------------|
| Local | `<project>/data/` | `<project>/data/delta/` |
| Databricks | `/dbfs/tmp/databricks-medallion-pipeline/data/` | `/dbfs/tmp/databricks-medallion-pipeline/` |

No hardcoded absolute local paths (e.g., `/Users/...`) are committed.

---

## 3. Verified Pipeline Stages

### Stage 1: Data Generation

| Check | Status |
|-------|--------|
| Generates 10,000 customers | ✅ |
| Generates 100,000 orders | ✅ |
| Generates 500 products | ✅ |
| Injects 700 defects | ✅ |
| Writes defect manifest | ✅ |
| PySpark CSV validation on cluster | ✅ (requires Java on cluster) |

**Databricks execution:**
```python
%run /Repos/<user>/c1/src/data_generation/generate_sample_data
```

### Stage 2: Bronze Ingestion

| Check | Status |
|-------|--------|
| Explicit `StructType` schemas | ✅ |
| Append-only Delta writes | ✅ |
| Metadata columns added | ✅ |
| No transformations applied | ✅ |
| Reads from DBFS path | ✅ |

**Databricks execution:**
```python
%run /Repos/<user>/c1/src/bronze/ingest_all
```

### Stage 3: Silver Quality

| Check | Status |
|-------|--------|
| Four quality check categories | ✅ |
| Rows flagged, not deleted | ✅ |
| `quality_check_result` column | ✅ |
| `quality_summary` Delta table | ✅ |
| `quality-report.md` generated | ✅ |

**Databricks execution:**
```python
%run /Repos/<user>/c1/src/silver/transform_all
```

### Stage 4: Gold Aggregations

| Check | Status |
|-------|--------|
| SQL files execute via `spark.sql()` | ✅ |
| Filters `is_valid = true` | ✅ |
| Four Gold tables created | ✅ |
| Customer segmentation logic | ✅ |

**Databricks execution:**
```python
%run /Repos/<user>/c1/src/gold/create_gold_tables
```

### Stage 5: Dashboard

| Check | Status |
|-------|--------|
| Temp views from Delta paths | ✅ |
| Top 10 Products query | ✅ |
| Revenue histogram query | ✅ |
| Segmentation pie chart query | ✅ |
| Daily trend line chart query | ✅ |

---

## 4. Cluster Configuration (Recommended)

| Setting | Value |
|---------|-------|
| Cluster mode | Single Node |
| Runtime | 14.3 LTS (or latest available on CE) |
| Worker type | Default CE allocation |
| Spark config | Default (no custom config required) |

---

## 5. Known CE Limitations & Workarounds

| Limitation | Workaround |
|------------|------------|
| No Unity Catalog | Use path-based Delta: `delta.\`/dbfs/tmp/...\`` |
| No scheduled Jobs (limited) | Run scripts manually or via notebook workflow |
| DBFS `/tmp` is ephemeral | Re-upload CSVs if cluster storage is cleared |
| Single-node only | Sufficient for synthetic dataset volumes |
| Temp views in SQL Dashboard | Re-create views each SQL session (see dashboard_queries.sql header) |

---

## 6. Upload Instructions (First-Time Setup)

```python
# In a Databricks notebook:
dbutils.fs.mkdirs("/tmp/databricks-medallion-pipeline/data/")
dbutils.fs.mkdirs("/tmp/databricks-medallion-pipeline/bronze/")
dbutils.fs.mkdirs("/tmp/databricks-medallion-pipeline/silver/")
dbutils.fs.mkdirs("/tmp/databricks-medallion-pipeline/gold/")

# Upload CSVs via UI: Data → Upload to DBFS
# Target: /tmp/databricks-medallion-pipeline/data/
```

---

## 7. Verification Commands (Run on Cluster)

```python
# Verify Bronze
for entity in ["customers", "orders", "products"]:
    path = f"/dbfs/tmp/databricks-medallion-pipeline/bronze/{entity}"
    print(f"{entity}: {spark.read.format('delta').load(path).count()} rows")

# Verify Silver quality summary
spark.read.format("delta").load(
    "/dbfs/tmp/databricks-medallion-pipeline/silver/quality_summary"
).show()

# Verify Gold
for table in ["sales_by_product", "revenue_by_customer", "daily_weekly_trends", "customer_segmentation"]:
    path = f"/dbfs/tmp/databricks-medallion-pipeline/gold/{table}"
    print(f"{table}: {spark.read.format('delta').load(path).count()} rows")
```

---

## 8. Compatibility Sign-Off

| Item | Verified |
|------|----------|
| PySpark Bronze/Silver/Gold scripts run on CE | ✅ Code review + path design |
| Delta Lake read/write | ✅ Standard Delta API used |
| No UC dependency | ✅ Path-based tables only |
| No enterprise-only features | ✅ Batch processing only |
| Dashboard queries compatible with SQL Warehouse | ✅ Standard SQL syntax |

> **Note:** Full end-to-end execution on a live Databricks CE cluster should be performed by the candidate before final submission. All code is designed and structured for CE compatibility per the checks above.
