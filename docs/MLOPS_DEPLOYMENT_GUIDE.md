# SIMON Health Habits — MLOps Deployment Guide
### Databricks Job Run (Batch Mode)

---

## Overview

This guide covers deploying the SIMON Health Habits pipeline as a scheduled **Databricks Job** with two notebooks running in series each day. The job writes one personalized health insight per patient into a Delta table — no API endpoint needed.

---

## What Gets Deployed

| # | Notebook | What It Does | Est. Runtime |
|---|----------|--------------|-------------|
| 1 | `Feature_store_Creation/notebook.py` | Reads 16+ raw health tables, computes all per-patient daily features, writes to Gold table | ~30 min *(TBC after prod test)* |
| 2 | `insight_generation_job.py` | Reads Gold table, runs logic engine + LLM for every patient, writes insights to output table | ~1–2 hrs *(TBC after prod test)* |

> **Note:** Runtimes above are estimates. Update this table after the first production run.

---

## Infrastructure Requirements

| Requirement | Value |
|-------------|-------|
| Platform | Databricks (Azure) — production workspace |
| Cluster type | Multi-node, autoscale recommended |
| Runtime | Databricks Runtime 14+ (PySpark included) |
| Python packages | `pyyaml`, `databricks-sdk` (install via `%pip` at notebook top) |
| LLM endpoint | `databricks-meta-llama-3-3-70b-instruct` (Databricks Foundation Models — must be enabled in prod workspace) |
| Unity Catalog | Write access to `bronz_als_azuat2.llm` schema |

---

## Catalog & Table Map

| Role | Full Table Name |
|------|----------------|
| **Input — feature store (Gold)** | `bronz_als_azuat2.llm.user_daily_health_habits` |
| **Input — message history** | `bronz_als_azuat2.llm.metabolic_readiness_message_history` |
| **Output — daily insights** | `bronz_als_azuat2.llm.daily_patient_insights` |

The output table is created automatically by Notebook 2 on first run. No manual DDL needed.

---

## Databricks Job Setup

### Step 1 — Upload Code to Workspace

Sync the following files to the prod workspace path:

```
/Workspace/Users/<your-email>/Welldoc_4_0/Metabolic_Readiness/
    ├── Feature_store_Creation/notebook.py
    ├── insight_generation_job.py        ← new, Job Notebook 2
    ├── logic_engine.py
    ├── insight_generator.py
    ├── main_pipeline.py
    └── prompts.yml
```

Update `code_path` and `prompts_config_path` in the `JOB_CONFIG` block at the top of `insight_generation_job.py` if the workspace path differs.

### Step 2 — Create a Multi-Task Job

Go to **Workflows → Jobs → Create Job** in the prod Databricks UI.

| Field | Value |
|-------|-------|
| Job name | `SIMON Health Habits — Daily Batch` |
| Schedule | Daily (e.g., `0 2 * * *` — 2 AM UTC, adjust to patient timezone) |
| Cluster | New job cluster — Databricks Runtime 14+, autoscale 2–8 workers |

### Step 3 — Add Task 1: Feature Store

| Field | Value |
|-------|-------|
| Task name | `feature_store_creation` |
| Type | Notebook |
| Source | Workspace |
| Path | `/Workspace/.../Feature_store_Creation/notebook.py` |
| Depends on | *(none — this is the first task)* |

### Step 4 — Add Task 2: Insight Generation

| Field | Value |
|-------|-------|
| Task name | `insight_generation` |
| Type | Notebook |
| Source | Workspace |
| Path | `/Workspace/.../insight_generation_job.py` |
| Depends on | `feature_store_creation` |

Task 2 will only start after Task 1 completes successfully. If Task 1 fails, Task 2 is skipped automatically.

### Step 5 — Set Job Parameters (optional)

No required job parameters. The notebooks use `datetime.now()` to determine today's report date automatically.

---

## Output Table Schema

The table written by Notebook 2:

```
bronz_als_azuat2.llm.daily_patient_insights
```

| Column | Type | Description |
|--------|------|-------------|
| `patient_id` | STRING | Patient identifier (primary key component) |
| `insight_date` | DATE | Date the message was generated for |
| `insight` | STRING | Full personalised health message (~250 words) |
| `score_name` | STRING | Daily rating: `Committed` / `Strong` / `Consistent` / `Building` / `Ready` |
| `generated_at` | TIMESTAMP | When this row was written |

**Primary key:** (`patient_id`, `insight_date`) — each run does a MERGE, so re-running is safe and idempotent.

---

## Monitoring & Alerting

- Set up a **Job failure alert** in Databricks (Workflows → Job → Edit → Notifications) to email the team on any task failure.
- Notebook 2 prints a **failure report** at the end listing any patients that could not be processed — check job run output regularly.
- After each run, a summary cell shows insight counts broken down by `score_name` for that day — useful for a quick sanity check.

---

## Failure Recovery

| Scenario | Action |
|----------|--------|
| Task 1 fails | Fix the issue, re-run the job. Task 2 will not have run. |
| Task 2 fails partway | Safe to re-run. MERGE logic is idempotent — already-written rows are updated, not duplicated. |
| LLM endpoint unavailable | Patients that fail are logged in the failure report. Reprocess them by re-running the job after the endpoint recovers. |
| Gold table not updated | Check Task 1 logs. Task 2 will find no feature rows and write nothing. |

---

## Checklist Before First Prod Run

- [ ] Code synced to prod workspace path
- [ ] `JOB_CONFIG` paths updated in `insight_generation_job.py`
- [ ] LLM endpoint `databricks-meta-llama-3-3-70b-instruct` enabled in prod workspace
- [ ] Service principal / user has write access to `bronz_als_azuat2.llm`
- [ ] Job cluster sized appropriately (suggest testing with 4 workers first)
- [ ] Job failure alert configured
- [ ] Ran one manual trigger to verify timing and row counts before enabling schedule
