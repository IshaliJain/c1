# Data Quality Strategy

## 1. Objectives

1. Implement **four required quality check categories** across all e-commerce entities.
2. Inject exactly **700 intentional data defects** during synthetic data generation.
3. Detect and flag **100% of intentional defects** in the Silver layer without silently dropping rows.
4. Provide auditable evidence via `quality_check_result`, a defect manifest, and Gold quality summary tables.

## 2. Quality Check Categories

### Category 1: Completeness

**Definition:** Required fields must be present and non-null.

| Error Code | Rule | Applies To | Example Defect |
|------------|------|------------|----------------|
| `COMP_001` | `customer_id` must not be NULL | customers | NULL customer_id |
| `COMP_002` | `email` must not be NULL | customers | Missing email |
| `COMP_003` | `product_id` must not be NULL | products | NULL product_id |
| `COMP_004` | `product_name` must not be NULL | products | Empty product name |
| `COMP_005` | `order_id` must not be NULL | orders | NULL order_id |
| `COMP_006` | `order_date` must not be NULL | orders | Missing order date |
| `COMP_007` | `customer_id` must not be NULL | orders | NULL FK on order |
| `COMP_008` | `order_item_id` must not be NULL | order_items | NULL line item id |
| `COMP_009` | `quantity` must not be NULL | order_items | NULL quantity |
| `COMP_010` | `unit_price` must not be NULL | order_items | NULL unit price |

**Silver behavior:** Set `quality_check_result` with applicable `COMP_*` codes; `is_valid = false`.

---

### Category 2: Uniqueness

**Definition:** Primary key values must be unique within each entity.

| Error Code | Rule | Applies To | Example Defect |
|------------|------|------------|----------------|
| `UNIQ_001` | `customer_id` unique | customers | Duplicate customer_id |
| `UNIQ_002` | `email` unique | customers | Two rows, same email |
| `UNIQ_003` | `product_id` unique | products | Duplicate product_id |
| `UNIQ_004` | `order_id` unique | orders | Duplicate order_id |
| `UNIQ_005` | `order_item_id` unique | order_items | Duplicate line item id |

**Silver behavior:** Flag all rows participating in a duplicate key group. For deduplication, retain the row with the latest `_ingested_at` as `is_valid = true` only if all other checks pass; others remain flagged.

---

### Category 3: Referential Integrity

**Definition:** Foreign key values must exist in the referenced parent entity.

| Error Code | Rule | Applies To | Example Defect |
|------------|------|------------|----------------|
| `REF_001` | `orders.customer_id` → `customers.customer_id` | orders | Orphan customer reference |
| `REF_002` | `order_items.order_id` → `orders.order_id` | order_items | Orphan order reference |
| `REF_003` | `order_items.product_id` → `products.product_id` | order_items | Orphan product reference |

**Silver behavior:** Left-anti-join against parent Silver/Bronze lookup; flag orphan rows with `REF_*` codes.

---

### Category 4: Type & Business Rule Checks

**Definition:** Values must conform to expected data types and domain business rules.

| Error Code | Rule | Applies To | Example Defect |
|------------|------|------------|----------------|
| `TYPE_001` | `customer_id` must match `CUST-\d+` pattern | customers | `customer_id = "INVALID"` |
| `TYPE_002` | `email` must match valid email format | customers | `email = "not-an-email"` |
| `TYPE_003` | `unit_price` must be castable to DECIMAL | order_items | `unit_price = "N/A"` |
| `TYPE_004` | `quantity` must be castable to INT | order_items | `quantity = "two"` |
| `TYPE_005` | `order_date` must be castable to DATE | orders | `order_date = "2024-13-45"` |
| `BIZ_001` | `unit_price` must be > 0 | order_items | Negative price |
| `BIZ_002` | `quantity` must be > 0 | order_items | Zero or negative quantity |
| `BIZ_003` | `order_date` must not be in the future | orders | Future-dated order |
| `BIZ_004` | `registration_date` must not be after `order_date` | customers/orders | Impossible timeline |
| `BIZ_005` | `order_status` must be in allowed enum | orders | `status = "SHIPPED_PENDING"` (invalid) |
| `BIZ_006` | `list_price` must be ≥ `unit_price` (if both present) | products/order_items | Price logic violation |

**Silver behavior:** Attempt safe casting; on failure or rule violation, append `TYPE_*` or `BIZ_*` to `quality_check_result`.

---

## 3. Flagging Mechanism

### Row-Level Columns (Silver)

| Column | Format | Example |
|--------|--------|---------|
| `quality_check_result` | Pipe-delimited error codes, or `PASS` | `COMP_001\|REF_001` |
| `is_valid` | Boolean | `false` |
| `_silver_processed_at` | Timestamp | `2026-08-30T10:00:00Z` |

### Rules

- A row with **multiple defects** accumulates **all** applicable codes.
- Rows are **never deleted** from Silver due to quality failure.
- `PASS` is assigned only when zero error codes apply.
- Gold analytics tables filter `is_valid = true` unless the table is explicitly a quality report.

### Pseudocode

```python
df = df.withColumn(
    "quality_check_result",
    when(size(error_array) == 0, lit("PASS")).otherwise(concat_ws("|", error_array))
).withColumn(
    "is_valid",
    col("quality_check_result") == "PASS"
)
```

## 4. Distribution of 700 Intentional Defects

Defects are injected at data generation time and recorded in `data/manifest/defect_manifest.csv`.

| Category | Target Count | % of Total | Primary Entities |
|----------|-------------|------------|------------------|
| Completeness | 175 | 25% | customers, orders, order_items |
| Uniqueness | 175 | 25% | customers, products, orders, order_items |
| Referential Integrity | 175 | 25% | orders, order_items |
| Type / Business | 175 | 25% | all entities |
| **Total** | **700** | **100%** | |

### Per-Entity Allocation (Planned)

| Entity | Completeness | Uniqueness | Referential | Type/Business | Subtotal |
|--------|-------------|------------|-------------|---------------|----------|
| customers | 50 | 50 | — | 50 | 150 |
| products | 40 | 40 | — | 45 | 125 |
| orders | 45 | 40 | 90 | 50 | 225 |
| order_items | 40 | 45 | 85 | 30 | 200 |
| **Total** | **175** | **175** | **175** | **175** | **700** |

> Some rows may carry multiple defects (e.g., NULL `customer_id` **and** orphan reference). The manifest tracks **700 distinct injected defect instances**; multi-defect rows are expected and must flag all codes.

## 5. Defect Manifest

File: `data/manifest/defect_manifest.csv`

| Column | Description |
|--------|-------------|
| `defect_id` | Sequential ID (1–700) |
| `entity` | Source table name |
| `primary_key_value` | Row identifier affected |
| `category` | Completeness / Uniqueness / Referential / TypeBusiness |
| `error_code` | e.g., `COMP_001` |
| `field_name` | Column with defect |
| `defect_description` | Human-readable description |
| `injected_value` | The bad value inserted |

The manifest is the **source of truth** for validation: after Silver runs, every `defect_id` must have a matching flagged row in Silver.

## 6. Validation & Acceptance Criteria

### Silver-Level Validation

```sql
-- Example: count flagged rows by category
SELECT
  CASE
    WHEN quality_check_result LIKE '%COMP_%' THEN 'Completeness'
    WHEN quality_check_result LIKE '%UNIQ_%' THEN 'Uniqueness'
    WHEN quality_check_result LIKE '%REF_%'  THEN 'Referential'
    WHEN quality_check_result RLIKE 'TYPE_|BIZ_' THEN 'TypeBusiness'
    ELSE 'PASS'
  END AS category,
  COUNT(*) AS flagged_rows
FROM silver.orders
WHERE quality_check_result != 'PASS'
GROUP BY 1;
```

### Acceptance Tests

| Test | Expected Result |
|------|-----------------|
| Total manifest defects | 700 |
| Manifest defects detected in Silver | 700 (100%) |
| False negatives | 0 |
| Rows silently dropped | 0 |
| Gold `data_quality_summary.detection_rate` | 100% |

### Validation Script (Planned)

`src/silver/validate_quality.py` will:

1. Load `defect_manifest.csv`.
2. Join against Silver tables on `entity` + `primary_key_value`.
3. Assert each manifest row has `is_valid = false` and the expected `error_code` in `quality_check_result`.
4. Print PASS/FAIL report with category breakdown.

## 7. Gold Quality Reporting

`gold.data_quality_summary` aggregates:

| Column | Description |
|--------|-------------|
| `entity` | customers, products, orders, order_items |
| `check_category` | Completeness, Uniqueness, Referential, TypeBusiness |
| `error_code` | Specific code |
| `expected_count` | From manifest |
| `detected_count` | From Silver flags |
| `detection_rate` | `detected_count / expected_count` |

Dashboard KPI: **700 / 700 defects detected**.

## 8. Implementation Notes

- Quality check functions live in `src/silver/quality_checks/` (one module per category).
- Each function returns a DataFrame with an `errors` array column appended per row.
- Checks run sequentially; results merge into a single `quality_check_result`.
- Logging at end of Silver job:

```
[INFO] Silver quality summary:
  Completeness:       175/175 detected
  Uniqueness:         175/175 detected
  Referential:        175/175 detected
  Type/Business:      175/175 detected
  TOTAL:              700/700 detected (100.0%)
```
