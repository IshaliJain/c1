# Debugging Notes

**Project:** Databricks Medallion E-Commerce Pipeline  
**Last Updated:** 2026-08-30

---

## 1. Environment Issues

### 1.1 PySpark Requires Java (Local)

**Symptom:**
```
PySparkRuntimeError: [JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number.
The operation couldn't be completed. Unable to locate a Java Runtime.
```

**Context:** Running `generate_sample_data.py` with PySpark validation on macOS without Java installed.

**Fix:**
- Data generation script updated to catch the exception and log a warning instead of failing.
- CSV files are still written successfully via pandas.
- Full PySpark validation runs on Databricks where Java is available.

**Workaround for local dev:**
```bash
python src/data_generation/generate_sample_data.py --no-validate
```

---

### 1.2 Git Unrelated Histories

**Symptom:** GitHub PR shows "entirely different commit histories" when comparing `feature/databricks-medallion-pipeline` to `main`.

**Cause:**
- Local repo started with its own initial commit.
- Remote `main` had a separate GitHub-generated README commit.

**Fix:**
```bash
git fetch origin
git merge origin/main --allow-unrelated-histories
# Resolve any conflicts, then push
```

---

## 2. Data Generation Issues

### 2.1 Defect Count Mismatch (460 vs 700)

**Symptom:** Prompt referenced "~700 defective rows" but listed specific counts totaling 460.

**Resolution:** Documented actual injected defects in `DATA_GENERATION_NOTES.md` and `ai-prompts/data-generation.md`. The 700 target remains the broader project goal across all entities and check types in later phases.

**Verification:**
```
null_emails: 50
duplicate_customer_id_injections: 10
null_order_customer_ids: 100
null_order_product_ids: 200
orphan_customer_ids: 50
orphan_product_ids: 30
duplicate_order_id_injections: 20
total_defective_rows: 460
```

---

## 3. Silver Layer Issues

### 3.1 `quality_checks.py` Edit Corruption

**Symptom:** During a multi-edit session, `apply_logic_type_checks_customers` body was accidentally merged into `apply_no_referential_check`, producing invalid Python.

**Detection:** Unstaged diff showed malformed function structure after initial commit.

**Fix:** Rewrote `quality_checks.py` completely; follow-up commit (`b83b497`) applied corrections.

**Lesson:** Review unstaged changes after partial commits; don't commit broken intermediate states.

### 3.2 Product Numeric Validation Logic

**Symptom:** Initial `apply_logic_type_checks_products` used `lit(True)` as a PySpark Column seed, which does not chain correctly with `&`.

**Fix:** Changed to `condition = None` pattern with iterative `condition & field_valid` — same pattern as completeness checks.

---

## 4. Bronze Layer Notes

### 4.1 Metadata Column Naming

**Issue:** Design docs reference `_ingested_at`; Bronze implementation uses `_ingestion_timestamp` per explicit prompt requirement.

**Impact:** No functional issue; Silver and Gold do not depend on this column name.

**Future:** Standardize to one name in a cleanup pass if needed.

### 4.2 Append-Only Re-runs

**Behavior:** Re-running Bronze ingestion appends duplicate rows from the same CSV.

**Expected:** This is by design (append-only audit trail).

**Mitigation for dev:** Delete Delta directory before re-run:
```bash
rm -rf data/delta/bronze/customers
```

---

## 5. Gold Layer Notes

### 5.1 Dashboard SQL Requires View Registration

**Symptom:** Dashboard queries reference `gold_sales_by_product` etc., but CE may not have Unity Catalog tables registered.

**Fix:** Run view creation statements from `dashboard_queries.sql` header before building dashboards:
```sql
CREATE OR REPLACE TEMP VIEW gold_sales_by_product AS
  SELECT * FROM delta.`/dbfs/tmp/databricks-medallion-pipeline/gold/sales_by_product`;
```

### 5.2 String-to-Numeric Casts in Gold SQL

**Issue:** Silver stores all source columns as strings (from Bronze). Gold SQL must explicitly `CAST(... AS DOUBLE)` and `CAST(... AS DATE)`.

**Symptom if omitted:** Aggregations return NULL or fail silently.

---

## 6. Databricks Community Edition Tips

| Task | Command / Approach |
|------|-------------------|
| Upload CSVs | Databricks UI → Data → Upload to `/tmp/` or use `%fs cp` |
| Check Delta path | `dbutils.fs.ls("/tmp/databricks-medallion-pipeline/gold/")` |
| Inspect schema | `spark.read.format("delta").load(path).printSchema()` |
| Clear Delta table | `dbutils.fs.rm(path, recurse=True)` |
| Run pipeline | Clone repo → attach to cluster → run scripts as notebook cells |

---

## 7. Quick Diagnostic Queries

```sql
-- Bronze row counts
SELECT 'customers' AS entity, COUNT(*) FROM delta.`.../bronze/customers`
UNION ALL
SELECT 'orders', COUNT(*) FROM delta.`.../bronze/orders`
UNION ALL
SELECT 'products', COUNT(*) FROM delta.`.../bronze/products`;

-- Silver failure breakdown
SELECT quality_check_result, COUNT(*) AS cnt
FROM delta.`.../silver/orders`
GROUP BY quality_check_result
ORDER BY cnt DESC;

-- Gold sanity check
SELECT SUM(total_revenue) FROM delta.`.../gold/sales_by_product`;
```

Replace `...` with `/dbfs/tmp/databricks-medallion-pipeline` on Databricks.
