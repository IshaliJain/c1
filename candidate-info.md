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
| **Domain** | E-commerce (customers, products, orders) |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Synthetic Records** | 10,000 customers / 100,000 orders / 500 products |
| **Intentional Data Defects** | 700 total across 4 quality dimensions |
| **Dashboard Tool** | Databricks SQL Dashboard |
| **Orchestration** | Manual script execution / Databricks notebook |

## Deliverables Checklist

- [x] Repository structure (`data/`, `src/`, `ai-prompts/`)
- [x] Cursor rules (`.cursor/rules/databricks-medallion.mdc`)
- [x] Planning documentation (requirements, design, quality strategy)
- [x] Synthetic data generation with 700 defects (`src/data_generation/`)
- [x] Defect manifest (`data/manifest/defect_manifest.csv`)
- [x] Bronze ingestion layer (`src/bronze/`)
- [x] Silver cleansing & quality layer (`src/silver/`)
- [x] Quality report with pass percentages (`quality-report.md`)
- [x] Gold analytics layer (`src/gold/`)
- [x] Dashboard queries (`src/dashboard/`)
- [x] AI prompt history (`ai-prompts/`)
- [x] Submission docs (`tool-workflow.md`, `reflection.md`, `debugging-notes.md`, `final-ai-usage-summary.md`)
- [x] Databricks CE verification (`databricks-ce-verification.md`)
- [x] Submission checklist (`SUBMISSION_CHECKLIST.md`)
- [x] End-to-end README (`README.md`)

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
