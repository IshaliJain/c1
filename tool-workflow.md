# Tool Workflow — Part A

**Project:** Databricks Medallion E-Commerce Data Pipeline  
**Candidate:** Ishali Jain  
**AI Tool:** Cursor (Agent / Composer Mode)  
**Platform:** Databricks Community Edition

---

## 1. AI Context-Setting Strategy

Before generating code, context was established in layers so Cursor produced Databricks-compatible output:

### 1.1 Cursor Rules (`.cursor/rules/databricks-medallion.mdc`)

A persistent rule file was created with `alwaysApply: true` covering:

- Target stack (Databricks CE, PySpark, Delta Lake)
- Medallion layer responsibilities (Bronze / Silver / Gold)
- Prohibited patterns (no pandas for big data, no hardcoded credentials)
- Expected project layout and reference artifacts

This prevented common AI errors such as using pandas in pipeline stages, hardcoding local paths, or dropping invalid rows during Silver processing.

### 1.2 Planning Artifacts First

Phase 1 generated documentation before any pipeline code:

| Artifact | Purpose |
|----------|---------|
| `requirements-analysis.md` | Functional/non-functional requirements, edge cases |
| `design-notes.md` | Architecture and layer design decisions |
| `data-quality-strategy.md` | Four check categories and defect flagging rules |
| `candidate-info.md` | Submission metadata template |

Subsequent prompts referenced `@Codebase` so Cursor aligned with these documents.

### 1.3 Phased Prompt Execution

Work was executed in strict phases to control scope:

| Phase | Prompt Focus | Output |
|-------|-------------|--------|
| 1 | Repository structure + planning docs | Markdown artifacts |
| 2 | Data generation | `generate_sample_data.py` + CSVs |
| 3 | Bronze ingestion | Raw CSV → Delta (append-only) |
| 4 | Silver quality | Four checks + flagged rows |
| 5 | Gold aggregations | SQL analytics tables |
| 6 | Dashboard + submission | Queries + reflection docs |

Each phase prompt was logged in `ai-prompts/` for traceability.

### 1.4 Prompt History Discipline

Every major phase produced an `ai-prompts/{layer}.md` file documenting:

- Original prompt text
- Accepted AI suggestions
- Human modifications and rationale

---

## 2. Validation Strategies

### 2.1 Data Generation Validation

| Check | Method |
|-------|--------|
| Row counts | Assert 10,000 / 100,000 / 500 rows per entity |
| Defect injection | `log_defect_summary()` counts NULLs, duplicates, orphans |
| Reproducibility | Fixed seeds (`RANDOM_SEED=42`, `FAKER_SEED=42`) |
| PySpark readability | Optional post-write CSV validation via Spark read |

### 2.2 Bronze Validation

| Check | Method |
|-------|--------|
| Schema enforcement | Explicit `StructType` on read — no inference |
| Metadata columns | `_ingestion_timestamp` and `_source_file` present |
| Row preservation | Input CSV row count == Bronze row count |
| Append behavior | Re-run appends without overwriting history |

### 2.3 Silver Validation

| Check | Method |
|-------|--------|
| Completeness | NULL/empty critical fields flagged |
| Uniqueness | Window `count(*) OVER (PARTITION BY key) > 1` |
| Referential integrity | Left join orphan detection against parent Silver tables |
| Logic & type | Date parsing and positive numeric validation |
| No row loss | Silver row count == Bronze row count per entity |
| Quality summary | Per-check pass/fail counts written to `quality_summary` Delta table |

### 2.4 Gold Validation

| Check | Method |
|-------|--------|
| Source filter | Only `is_valid = true` Silver rows |
| Aggregation sanity | `total_revenue = SUM(total_amount)` cross-check on sample |
| Segmentation coverage | All four segments present (Inactive, High-Value, Repeat, One-Time) |
| SQL execution | `create_gold_tables.py` logs row counts per Gold table |

### 2.5 Human Review Checklist

Before each commit:

- [ ] No secrets or absolute local paths in code
- [ ] Prompt history updated in `ai-prompts/`
- [ ] Generated data files remain gitignored
- [ ] Commit contains only files from the current phase

---

## 3. Debugging Logs & Techniques

### 3.1 Structured Logging

All pipeline scripts use Python `logging` with timestamps:

```
2026-08-30 16:05:44 [INFO] Defect summary:
2026-08-30 16:05:44 [INFO]   null_emails: 50
2026-08-30 16:05:44 [INFO]   orphan_customer_ids: 50
```

### 3.2 Layer-Specific Debug Points

| Layer | What to Log | Where |
|-------|-------------|-------|
| Data generation | Defect counts per category | `generate_sample_data.py` stdout |
| Bronze | Source path, target path, row count | `bronze_common.py` |
| Silver | Passed/failed per entity | `01_transform_*.py` |
| Silver summary | Per-check pass percentage | `quality_summary` Delta table |
| Gold | SQL file name, output row count | `create_gold_tables.py` |

### 3.3 Known Issues Encountered

| Issue | Symptom | Resolution |
|-------|---------|------------|
| No Java locally | PySpark validation fails with `JAVA_GATEWAY_EXITED` | Graceful skip in data generator; full validation on Databricks |
| Unrelated Git histories | PR shows divergent branches | Merge `origin/main` with `--allow-unrelated-histories` |
| 460 vs 700 defects | Prompt ambiguity | Documented actual injected count; broader target deferred |
| Silver `quality_checks.py` corruption | Duplicate function definitions during edit | Rewrote file; fixed in follow-up commit |

### 3.4 Databricks Debugging

On the cluster:

```python
# Inspect Bronze row count
display(spark.read.format("delta").load("/dbfs/tmp/.../bronze/orders").groupBy("quality_check_result").count())

# Inspect failed rows
spark.sql("SELECT * FROM silver_orders WHERE is_valid = false LIMIT 20").show()
```

---

## 4. Privacy Handling

| Concern | Mitigation |
|---------|------------|
| Real PII | All data is **synthetic** via Faker — no real customer data |
| Credentials | `.gitignore` excludes `.env`, `.databrickscfg`, `credentials/` |
| Hardcoded secrets | Cursor rule prohibits credentials in code; paths use env detection |
| Email addresses | Fake emails generated by Faker; not linked to real individuals |
| Repository visibility | Public GitHub repo contains no secrets (verified before each push) |
| AI prompt content | Prompts logged locally; no API keys or tokens included |
| Databricks tokens | Configured in Databricks UI / CLI locally — never committed |

---

## 5. Lessons Learned

### What Worked Well

1. **Cursor rules first** — Reduced repeated corrections across phases (PySpark vs pandas, path handling).
2. **Phased prompts** — Smaller, focused prompts produced reviewable diffs.
3. **Prompt history** — `ai-prompts/` folder made it easy to trace AI decisions during submission prep.
4. **Shared `*_common.py` modules** — Bronze, Silver, and Gold all benefited from centralized path resolution.
5. **StringType in Bronze** — Preserved intentional defects without silent type coercion.

### What Could Be Improved

1. **Align naming early** — `_ingested_at` vs `_ingestion_timestamp` caused minor doc inconsistency.
2. **Run PySpark locally with Java** — Would have caught integration issues before Databricks deployment.
3. **Defect manifest file** — Planned in `data-quality-strategy.md` but not yet generated as CSV.
4. **Unity Catalog** — Path-based Delta tables work on CE but table registration would simplify dashboard SQL.
5. **Automated tests** — No pytest suite; validation relies on logging and manual row-count checks.

### Recommendations for Future Projects

- Create `.cursor/rules` before the first code prompt.
- Define exact defect counts and manifest format before data generation.
- Register Delta paths as views in a setup notebook for dashboard reuse.
- Add a single `run_pipeline.py` orchestrator for end-to-end execution.
