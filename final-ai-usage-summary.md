# Final AI Usage Summary

**Project:** Databricks Medallion E-Commerce Data Pipeline  
**AI Tool:** Cursor (Agent / Composer Mode)  
**Candidate:** Ishali Jain  
**Date:** 2026-08-30

---

## 1. How AI Was Used

| Activity | AI Role | Human Role |
|----------|---------|------------|
| Project scaffolding | Generated directory structure, `.gitignore`, Cursor rules | Specified requirements, approved structure |
| Planning docs | Drafted requirements, design, quality strategy | Reviewed, corrected defect counts and scope |
| Data generation | Wrote Faker/pandas script with defect injection | Validated defect counts, fixed PySpark/Java handling |
| Bronze layer | PySpark ingest scripts with StructType schemas | Reviewed path strategy, approved metadata column names |
| Silver layer | Quality check functions, transform scripts | Fixed corrupted file, corrected product validation logic |
| Gold layer | SQL aggregations and orchestrator | Reviewed segmentation rules and cast logic |
| Submission docs | Dashboard queries, README, reflection | Filled candidate details, verified accuracy |
| Git operations | Commit messages and push commands | Approved each commit scope |

---

## 2. What AI Got Right

| Area | Details |
|------|---------|
| **Architecture alignment** | Consistently followed Medallion patterns after Cursor rules were set |
| **PySpark conventions** | Used `StructType`, Delta format, `current_timestamp()`, window functions correctly |
| **Path portability** | Auto-detected Databricks vs local via `DATABRICKS_RUNTIME_VERSION` |
| **Quality check design** | Four check categories implemented without dropping invalid rows |
| **Documentation structure** | Produced well-organized markdown with tables, schemas, and usage instructions |
| **Modular code** | `*_common.py` pattern applied consistently across Bronze, Silver, Gold |
| **Prompt history** | Each phase logged in `ai-prompts/` with accepted/modified decisions |
| **Gitignore** | Correctly excluded data files, credentials, and IDE metadata |

---

## 3. What AI Got Wrong (and How It Was Fixed)

| Issue | AI Output | Problem | Fix |
|-------|-----------|---------|-----|
| Defect total | Referenced 700 defects | Listed counts summed to 460 | Documented actual 460; noted broader 700 target |
| PySpark local validation | Hard fail without Java | Script exited with error code 1 | Added try/except graceful skip |
| `quality_checks.py` | Corrupted during multi-edit | Duplicate/malformed functions | Rewrote file; follow-up commit |
| Product type validation | `lit(True)` column chaining | Invalid PySpark Column logic | Changed to `condition = None` iterative pattern |
| Customer transform docstring | Said "products" instead of "customers" | Copy-paste error | Corrected before commit |
| Design vs implementation naming | Mixed `_ingested_at` / `_ingestion_timestamp` | Inconsistent across docs | Documented both; used prompt-specified name in Bronze |
| Git unrelated histories | Pushed without merging remote main | PR blocked on GitHub | Manual merge with `--allow-unrelated-histories` |

---

## 4. How Suggestions Were Validated

### 4.1 Code Validation

| Method | Applied To |
|--------|-----------|
| Run script locally | `generate_sample_data.py` — verified row counts and defect summary |
| Log output inspection | Bronze/Silver scripts — checked INFO logs for row counts |
| Git diff review | Every commit — ensured only intended files staged |
| Cross-reference docs | SQL schemas matched data generator column names |
| Prompt history cross-check | `ai-prompts/*.md` matched actual files produced |

### 4.2 Documentation Validation

| Method | Applied To |
|--------|-----------|
| Codebase grep | README commands match actual script paths |
| Entity schema check | Gold SQL columns match Silver output |
| Defect count arithmetic | Manual sum of injected defects = 460 |

### 4.3 Not Yet Validated on Databricks

The following require Databricks CE cluster execution:

- [ ] Full Bronze → Silver → Gold pipeline on DBFS paths
- [ ] Dashboard queries against registered Gold views
- [ ] PySpark validation in data generator with Java available
- [ ] Silver quality summary pass rates against 460 known defects

---

## 5. AI Productivity Assessment

| Metric | Estimate |
|--------|----------|
| Time saved on scaffolding | ~60% vs manual setup |
| Time saved on documentation | ~70% for first drafts |
| Time spent on review/fixes | ~25% of total project time |
| Commits with AI-generated code | 100% of pipeline code |
| Commits requiring human fix follow-up | 2 (Silver quality_checks, data gen Java handling) |

---

## 6. Prompt History Index

| File | Phase |
|------|-------|
| `ai-prompts/phase-1-planning.md` | Repository structure + planning docs |
| `ai-prompts/data-generation.md` | Synthetic data with defects |
| `ai-prompts/bronze-layer.md` | Bronze CSV → Delta ingestion |
| `ai-prompts/silver-layer.md` | Silver quality checks |
| `ai-prompts/gold-layer.md` | Gold SQL aggregations |

---

## 7. Disclosure Statement

All AI-generated code and documentation was reviewed by the candidate before commit. No credentials, API keys, or real personal data were included in prompts or committed files. Synthetic data was generated using Faker with fixed seeds for reproducibility. The candidate takes responsibility for final submission accuracy.
