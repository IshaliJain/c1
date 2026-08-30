# Candidate Information

> Fill in or update the template parameters below before final submission.

| Parameter | Value |
|-----------|-------|
| **Candidate Name** | Ishali Jain |
| **Candidate Email** | `{{CANDIDATE_EMAIL}}` |
| **Candidate ID** | `{{CANDIDATE_ID}}` |
| **Project Title** | Databricks Medallion E-Commerce Data Pipeline |
| **Repository Name** | `databricks-medallion-pipeline` |
| **Repository URL** | https://github.com/IshaliJain/c1 |
| **Branch** | `feature/databricks-medallion-pipeline` |
| **Submission Date** | `{{SUBMISSION_DATE}}` |
| **Assessment / Course** | `{{ASSESSMENT_NAME}}` |
| **Instructor / Reviewer** | `{{REVIEWER_NAME}}` |

## Technical Environment

| Parameter | Value |
|-----------|-------|
| **Target Platform** | Databricks Community Edition |
| **Cluster Type** | Single-node |
| **Runtime** | `{{DATABRICKS_RUNTIME_VERSION}}` (e.g., 14.3 LTS) |
| **Language** | Python 3.10+, PySpark SQL |
| **Storage Format** | Delta Lake |
| **IDE** | Cursor (Composer / Agent Mode) |
| **Local Data Path** | `data/` |
| **Databricks Path** | `/dbfs/tmp/databricks-medallion-pipeline/` |

## Project Scope Summary

| Parameter | Value |
|-----------|-------|
| **Domain** | E-commerce (customers, products, orders, order items) |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Synthetic Records (target)** | `{{RECORD_COUNT_PER_ENTITY}}` per core entity |
| **Intentional Data Defects** | 700 total across 4 quality dimensions |
| **Dashboard Tool** | `{{DASHBOARD_TOOL}}` (e.g., Databricks SQL Dashboard) |
| **Orchestration** | `{{ORCHESTRATION_APPROACH}}` (e.g., Databricks Jobs / manual notebook run) |

## Deliverables Checklist

- [x] Repository structure (`data/`, `src/`, `ai-prompts/`)
- [x] Cursor rules (`.cursor/rules/databricks-medallion.mdc`)
- [x] Planning documentation (this file + requirements, design, quality strategy)
- [ ] Synthetic data generation (`src/data_generation/`)
- [ ] Bronze ingestion layer (`src/bronze/`)
- [ ] Silver cleansing & quality layer (`src/silver/`)
- [ ] Gold analytics layer (`src/gold/`)
- [ ] Dashboard (`src/dashboard/`)
- [ ] AI prompt history (`ai-prompts/`)

## AI Tooling Disclosure

| Parameter | Value |
|-----------|-------|
| **AI Tool Used** | Cursor (Agent / Composer) |
| **Primary Use** | Scaffolding, documentation, PySpark code generation, validation logic |
| **Prompt History Location** | `ai-prompts/` |
| **Human Review** | All generated code and docs reviewed before commit |

## Contact & Notes

```
{{ADDITIONAL_NOTES}}
```
