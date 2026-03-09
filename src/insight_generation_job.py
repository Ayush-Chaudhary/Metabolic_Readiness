# Databricks notebook source
# MAGIC %md
# MAGIC # SIMON Health Habits — Batch Insight Generation Job
# MAGIC
# MAGIC This notebook is **Job Notebook 2 of 2**.
# MAGIC
# MAGIC It reads from the Gold feature store created by Notebook 1, runs the logic engine
# MAGIC and LLM for every active patient, and writes one row per patient per day to the
# MAGIC output insights table.
# MAGIC
# MAGIC **Depends on:** `Feature_store_Creation/notebook.py` having completed successfully for today's date.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# %pip install -U --quiet pyyaml databricks-sdk
# dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
import os
import sys

# ===== JOB CONFIGURATION =====
JOB_CONFIG: Dict[str, Any] = {
    # Source: Gold feature store (written by Notebook 1)
    "gold_table": {
        "catalog": "bronz_als_azuat2",
        "schema": "llm",
        "table_name": "user_daily_health_habits",
    },

    # Source: Message history (for frequency capping)
    "history_table": {
        "catalog": "bronz_als_azuat2",
        "schema": "llm",
        "table_name": "metabolic_readiness_message_history",
    },

    # Destination: Daily insights output table
    "output_table": {
        "catalog": "bronz_als_azuat2",
        "schema": "llm",
        "table_name": "daily_patient_insights",
    },

    # Prompts / config file (Databricks Workspace path)
    "prompts_config_path": "/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/files/prompts.yml",

    # Code directory (where logic_engine.py and insight_generator.py live)
    "code_path": "/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/src",
}

def full_table(cfg: dict) -> str:
    return f"{cfg['catalog']}.{cfg['schema']}.{cfg['table_name']}"

GOLD_TABLE    = full_table(JOB_CONFIG["gold_table"])
HISTORY_TABLE = full_table(JOB_CONFIG["history_table"])
OUTPUT_TABLE  = full_table(JOB_CONFIG["output_table"])

print(f"Gold table   : {GOLD_TABLE}")
print(f"History table: {HISTORY_TABLE}")
print(f"Output table : {OUTPUT_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Import Pipeline Modules

# COMMAND ----------

# Make sure the project code is on the Python path
code_path = JOB_CONFIG["code_path"]
if code_path not in sys.path:
    sys.path.insert(0, code_path)

from logic_engine import LogicEngine, UserContext, MessageHistory, A1CTargetGroup, SelectedContent
from insight_generator import InsightGenerator

# Re-import helper functions from main_pipeline (they are standalone functions there)
# We reproduce the minimal set needed here to keep this notebook self-contained.
import importlib.util, types

# Load main_pipeline as a module without executing Spark cells
_spec = importlib.util.spec_from_file_location("main_pipeline", os.path.join(code_path, "main_pipeline.py"))
_mod  = importlib.util.module_from_spec(_spec)
# Expose spark so the module-level code that references it does not fail
_mod.spark = spark  # noqa: F821  (spark is injected by Databricks)
_spec.loader.exec_module(_mod)

build_user_context   = _mod.build_user_context
get_user_profile     = _mod.get_user_profile
get_message_history  = _mod.get_message_history
upsert_message_history = _mod.upsert_message_history
PIPELINE_CONFIG      = _mod.PIPELINE_CONFIG

print("✓ Modules imported")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Initialise the Logic Engine and Insight Generator

# COMMAND ----------

config_path = JOB_CONFIG["prompts_config_path"]
if not os.path.exists(config_path):
    # Fall back to local copy when running outside Databricks
    config_path = os.path.join(code_path, "prompts.yml")

logic_engine      = LogicEngine(config_path)
insight_generator = InsightGenerator(config_path)

print(f"✓ Logic engine and insight generator initialised  [config: {config_path}]")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Create Output Table (if it does not exist)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
    patient_id   STRING    NOT NULL  COMMENT 'Unique patient identifier',
    insight_date DATE      NOT NULL  COMMENT 'Date the insight was generated for (yesterday data)',
    insight      STRING              COMMENT 'Full personalised health message (~250 words)',
    score_name   STRING              COMMENT 'Daily health rating: Committed | Strong | Consistent | Building | Ready',
    generated_at TIMESTAMP           COMMENT 'Timestamp when this row was written',
    CONSTRAINT pk_daily_insight PRIMARY KEY (patient_id, insight_date)
)
USING DELTA
COMMENT 'One row per patient per day. Written by the SIMON Health Habits batch job (Notebook 2).'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

print(f"✓ Output table ready: {OUTPUT_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Fetch All Patients with Features for Today

# COMMAND ----------

# Report date is today (features were computed for yesterday's activity)
report_date     = datetime.now()
report_date_str = report_date.strftime("%Y-%m-%d")

patients_df = spark.sql(f"""
    SELECT DISTINCT patientid
    FROM   {GOLD_TABLE}
    WHERE  report_date = '{report_date_str}'
""")

patient_ids: List[str] = [row["patientid"] for row in patients_df.collect()]
total_patients = len(patient_ids)
print(f"✓ Found {total_patients} patients with feature data for {report_date_str}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Batch Generate Insights

# COMMAND ----------

results = []
failed  = []

for i, patient_id in enumerate(patient_ids, start=1):
    try:
        # --- Load data ---
        row = spark.sql(f"""
            SELECT * FROM {GOLD_TABLE}
            WHERE  patientid = '{patient_id}'
            AND    report_date = '{report_date_str}'
            LIMIT 1
        """).collect()

        if not row:
            failed.append({"patient_id": patient_id, "error": "No feature row found"})
            continue

        features = row[0].asDict()
        profile  = get_user_profile(spark, patient_id)
        context  = build_user_context(features, profile)
        history  = get_message_history(spark, patient_id)

        # --- Logic engine: select content ---
        selected: SelectedContent = logic_engine.select_content(context, history)

        # --- LLM: generate natural language ---
        gen = insight_generator.generate_insight(
            daily_rating       = selected.daily_rating,
            rating_description = selected.rating_description,
            positive_actions   = selected.positive_actions,
            opportunity        = selected.opportunity,
            greeting           = selected.greeting,
        )

        if not gen["success"]:
            failed.append({"patient_id": patient_id, "error": gen.get("error", "LLM failure")})
            continue

        result_row = {
            "patient_id"  : patient_id,
            "insight_date": report_date_str,
            "insight"     : gen["message"],
            "score_name"  : selected.daily_rating,
            "generated_at": datetime.utcnow().isoformat(),
            # Keep extra fields for history upsert
            "rating_description"   : selected.rating_description,
            "character_count"      : gen.get("character_count", len(gen["message"])),
            "word_count"           : len(gen["message"].split()),
            "positive_actions_used": gen.get("positive_actions_used", []),
            "opportunity_used"     : gen.get("opportunity_used", ""),
        }
        results.append(result_row)

        # --- Update message history table (for frequency capping) ---
        upsert_message_history(spark, patient_id, report_date_str, result_row)

        if i % 50 == 0 or i == total_patients:
            pct = round(i / total_patients * 100)
            print(f"  Progress: {i}/{total_patients} ({pct}%)  |  failures so far: {len(failed)}")

    except Exception as exc:
        import traceback
        failed.append({"patient_id": patient_id, "error": str(exc), "trace": traceback.format_exc()})

print(f"\n✓ Generation complete — {len(results)} succeeded, {len(failed)} failed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Write Results to Output Table (MERGE / Upsert)

# COMMAND ----------

if results:
    from pyspark.sql import Row
    from pyspark.sql.functions import lit, current_timestamp

    output_rows = [
        Row(
            patient_id   = r["patient_id"],
            insight_date = r["insight_date"],
            insight      = r["insight"],
            score_name   = r["score_name"],
            generated_at = r["generated_at"],
        )
        for r in results
    ]

    result_df = spark.createDataFrame(output_rows)

    # Write to a temporary view so we can MERGE from it
    temp_view = "tmp_insights_batch"
    result_df.createOrReplaceTempView(temp_view)

    spark.sql(f"""
        MERGE INTO {OUTPUT_TABLE} AS target
        USING {temp_view} AS source
            ON  target.patient_id   = source.patient_id
            AND target.insight_date = source.insight_date
        WHEN MATCHED THEN
            UPDATE SET
                target.insight      = source.insight,
                target.score_name   = source.score_name,
                target.generated_at = source.generated_at
        WHEN NOT MATCHED THEN
            INSERT (patient_id, insight_date, insight, score_name, generated_at)
            VALUES (source.patient_id, source.insight_date, source.insight, source.score_name, source.generated_at)
    """)

    print(f"✓ Upserted {len(results)} rows into {OUTPUT_TABLE}")
else:
    print("⚠ No results to write — check failed list below")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Failure Report

# COMMAND ----------

if failed:
    print(f"\n{'='*60}")
    print(f"FAILED PATIENTS ({len(failed)} total)")
    print(f"{'='*60}")
    for f in failed:
        print(f"  patient_id: {f['patient_id']}  |  error: {f['error']}")
else:
    print("✓ No failures — all patients processed successfully")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Quick Validation

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*)            AS total_rows,
        COUNT(DISTINCT patient_id)  AS unique_patients,
        insight_date,
        COUNT(CASE WHEN score_name = 'Committed'  THEN 1 END) AS committed,
        COUNT(CASE WHEN score_name = 'Strong'     THEN 1 END) AS strong,
        COUNT(CASE WHEN score_name = 'Consistent' THEN 1 END) AS consistent,
        COUNT(CASE WHEN score_name = 'Building'   THEN 1 END) AS building,
        COUNT(CASE WHEN score_name = 'Ready'      THEN 1 END) AS ready
    FROM {OUTPUT_TABLE}
    WHERE insight_date = '{report_date_str}'
    GROUP BY insight_date
""").display()
