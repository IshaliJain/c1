# Reflection

**Project:** Databricks Medallion E-Commerce Data Pipeline  
**Candidate:** Ishali Jain  
**Date:** 2026-08-30

---

## 1. Project Overview

This project implements a full Medallion Architecture pipeline for synthetic e-commerce data on Databricks Community Edition. The pipeline spans data generation with intentional defects, Bronze raw ingestion, Silver quality flagging, Gold business aggregations, and SQL dashboard queries.

---

## 2. Technical Decisions I'm Proud Of

### Medallion Layer Separation

Keeping Bronze append-only and defect-preserving, while pushing all cleansing logic to Silver with row-level flags, made the quality testing objective achievable. Invalid rows remain auditable rather than silently disappearing.

### Explicit Schemas Everywhere

Using `StructType` in Bronze and string preservation until Gold casting prevented subtle data corruption. This was especially important for testing NULL foreign keys and orphan references.

### Modular Common Modules

`bronze_common.py`, `silver_common.py`, and `gold_common.py` each centralized path resolution for local vs Databricks environments. This eliminated hardcoded `/Users/...` paths and made the same scripts runnable in both contexts.

### Prompt History as Documentation

Maintaining `ai-prompts/` alongside code created a clear audit trail of what AI generated vs what was human-modified. This was valuable during submission preparation.

---

## 3. Challenges Faced

### Balancing AI Speed with Code Quality

Cursor generated large files quickly, but occasionally introduced bugs during multi-edit sessions (e.g., corrupted `quality_checks.py`). I learned to commit in smaller phases and always review unstaged diffs.

### Local vs Databricks Parity

Without Java installed locally, PySpark validation could not run end-to-end on my machine. I relied on pandas for generation and deferred Spark validation to Databricks, which added deployment risk.

### Defect Count Ambiguity

The project documentation referenced 700 intentional defects, but specific generation prompts totaled 460. I chose to implement exactly what was specified and document the gap rather than invent additional defects without instruction.

### Dashboard Table Registration

Gold SQL files assume temp views exist. On Databricks CE without Unity Catalog, extra setup steps are needed before dashboards work — this could be automated in a setup notebook.

---

## 4. What I Would Do Differently

1. **Single orchestrator script** — `run_pipeline.py` to chain generate → bronze → silver → gold in one command.
2. **Defect manifest CSV** — Generate `data/manifest/defect_manifest.csv` during data generation for automated Silver validation.
3. **Unit tests** — Pytest for defect injection counts and quality check logic without a Spark cluster.
4. **Databricks notebook** — Package pipeline as a `.dbc` or multi-cell notebook for easier CE execution.
5. **Naming consistency** — Align `_ingested_at` / `_ingestion_timestamp` across all docs and code from day one.

---

## 5. Skills Developed

| Skill | How |
|-------|-----|
| Medallion Architecture | Designed and implemented Bronze → Silver → Gold with clear layer contracts |
| PySpark Data Quality | Window functions for uniqueness, left joins for referential integrity |
| Delta Lake | Append (Bronze) and overwrite (Silver/Gold) patterns |
| AI-Assisted Development | Cursor rules, phased prompts, prompt history discipline |
| Data Engineering Documentation | Requirements, design notes, quality strategy, debugging logs |

---

## 6. Conclusion

The project successfully demonstrates an end-to-end e-commerce data pipeline with intentional quality defects, row-level flagging, and Gold analytics ready for BI dashboards. AI tooling accelerated scaffolding significantly, but human review remained essential for correctness — especially around edge cases, naming consistency, and validation logic. The combination of Cursor rules, phased prompts, and structured documentation made the AI-assisted workflow reproducible and submission-ready.
