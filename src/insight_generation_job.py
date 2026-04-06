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

%pip install -U --quiet pyyaml databricks-sdk mlflow
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# ===== WIDGETS =====
dbutils.widgets.text("gold_catalog",         "bronz_als_azuat2",                                              "Gold Catalog")
dbutils.widgets.text("gold_schema",          "llm",                                                           "Gold Schema")
dbutils.widgets.text("gold_table_name",      "user_daily_health_habits",                                      "Gold Table Name")
dbutils.widgets.text("output_catalog",       "bronz_als_azuat2",                                              "Output Catalog")
dbutils.widgets.text("output_schema",        "llm",                                                           "Output Schema")
dbutils.widgets.text("output_table_name",    "simon_healthy_habits_insights",                                 "Output Table Name")
dbutils.widgets.text("src_path",             "/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/files/src", "Source Path")
dbutils.widgets.text("llm_endpoint",         "databricks-meta-llama-3-3-70b-instruct",                        "LLM Endpoint Name")
dbutils.widgets.text("insight_date",         "",                                                               "Insight Date (YYYY-MM-DD, blank=today)")

_gold_catalog       = dbutils.widgets.get("gold_catalog")
_gold_schema        = dbutils.widgets.get("gold_schema")
_gold_table_name    = dbutils.widgets.get("gold_table_name")
_output_catalog     = dbutils.widgets.get("output_catalog")
_output_schema      = dbutils.widgets.get("output_schema")
_output_table_name  = dbutils.widgets.get("output_table_name")
_src_path           = dbutils.widgets.get("src_path")
_llm_endpoint       = dbutils.widgets.get("llm_endpoint")
_insight_date_raw   = dbutils.widgets.get("insight_date").strip()

# COMMAND ----------

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
import logging
import os
import sys

# Suppress noisy py4j gateway callback logs
logging.getLogger("py4j").setLevel(logging.WARNING)

_src_dir = _src_path or os.path.dirname(os.path.abspath(__file__))

# ===== RESOLVE INSIGHT DATE =====
# Defaults to today when the widget is left blank; otherwise parse the user-supplied date.
if _insight_date_raw:
    _insight_date = datetime.strptime(_insight_date_raw, "%Y-%m-%d")
else:
    _insight_date = datetime.now()

insight_date_str = _insight_date.strftime("%Y-%m-%d")
feature_date     = _insight_date - timedelta(days=1)
feature_date_str = feature_date.strftime("%Y-%m-%d")

print(f"Insight date : {insight_date_str}  (feature date / yesterday: {feature_date_str})")

# ===== JOB CONFIGURATION =====
JOB_CONFIG: Dict[str, Any] = {
    # Source: Gold feature store (written by Notebook 1)
    "gold_table": {
        "catalog": _gold_catalog,
        "schema": _gold_schema,
        "table_name": _gold_table_name,
    },

    # Source: Message history (for frequency capping)
    "history_table": {
        "catalog": _gold_catalog,
        "schema": _gold_schema,
        "table_name": "metabolic_readiness_message_history",
    },

    # Destination: Daily insights output table
    "output_table": {
        "catalog": _output_catalog,
        "schema": _output_schema,
        "table_name": _output_table_name,
    },

    # Prompts / config file — resolved relative to this notebook's directory
    "prompts_config_path": os.path.join(_src_dir, "prompts.yml"),

    # Code directory (where logic_engine.py and insight_generator.py live)
    "code_path": _src_dir,
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

from pipeline_utils import (
    build_user_context,
    create_history_table,
    get_user_profile,
    get_message_history,
    _extract_categories_from_actions,
)

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

# Override endpoint from widget (takes precedence over prompts.yml)
if _llm_endpoint:
    insight_generator.model_config['endpoint_name'] = _llm_endpoint

print(f"✓ Logic engine and insight generator initialised  [config: {config_path}]")
print(f"✓ LLM endpoint: {insight_generator.model_config['endpoint_name']}")

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
    generated_at TIMESTAMP           COMMENT 'Timestamp when this row was written'
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
# MAGIC ## 5b. Create History Table (if it does not exist)

# COMMAND ----------

_history_exists = spark.catalog.tableExists(HISTORY_TABLE)

if not _history_exists:
    create_history_table(spark, HISTORY_TABLE)
else:
    print(f"✓ History table already exists: {HISTORY_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Fetch All Patients with Features for Today

# COMMAND ----------

patients_df = spark.sql(f"""
    SELECT DISTINCT patientid
    FROM   {GOLD_TABLE}
    WHERE  report_date = '{feature_date_str}'
""")

patient_ids: List[str] = [row["patientid"] for row in patients_df.collect()]
total_patients = len(patient_ids)
print(f"✓ Found {total_patients} patients with feature data for {feature_date_str} (generating insights for {insight_date_str})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Batch Generate Insights

# COMMAND ----------

import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

MAX_WORKERS = 10   # concurrent LLM threads; tune based on endpoint rate limits

results      = []
failed       = []
history_batch = []  # accumulate for a single batch MERGE at the end

# ----------------------------------------------------------
# 7a. Pre-fetch ALL feature rows in one query (eliminates N individual SELECTs)
# ----------------------------------------------------------
print("Pre-fetching all feature rows…")
features_map: Dict[str, Dict] = {}
for row in spark.sql(f"""
    SELECT * FROM {GOLD_TABLE}
    WHERE  report_date = '{feature_date_str}'
""").collect():
    pid = row["patientid"]
    if pid not in features_map:
        features_map[pid] = row.asDict()
print(f"  ✓ Loaded features for {len(features_map)} patients")

# ----------------------------------------------------------
# 7b. Pre-fetch ALL message history in one query (JOIN avoids large IN literals)
# ----------------------------------------------------------
print("Pre-fetching message history…")
lookback_date_7d = (_insight_date - timedelta(days=7)).strftime('%Y-%m-%d')
lookback_date_6d = (_insight_date - timedelta(days=6)).strftime('%Y-%m-%d')
yesterday_str = (_insight_date - timedelta(days=1)).strftime('%Y-%m-%d')

spark.createDataFrame(
    [(pid,) for pid in patient_ids],
    schema="patientid STRING"
).createOrReplaceTempView("tmp_active_patient_ids")

history_rows_by_patient: Dict[str, list] = defaultdict(list)
try:
    for row in spark.sql(f"""
        SELECT h.patientid, h.category, h.message_date, h.opportunity_used
        FROM   {HISTORY_TABLE} h
        JOIN   tmp_active_patient_ids p ON h.patientid = p.patientid
        WHERE  h.message_date >= '{lookback_date_7d}'
    """).collect():
        history_rows_by_patient[row["patientid"]].append(row)
except Exception as e:
    print(f"  Warning: could not pre-fetch history ({e}) — proceeding with empty history")

print(f"  ✓ History pre-fetched")

# ----------------------------------------------------------
# Helper: build MessageHistory from pre-fetched rows (no Spark call)
# ----------------------------------------------------------
def _build_history_from_cache(patient_id: str) -> MessageHistory:
    rows = history_rows_by_patient.get(patient_id, [])
    history = MessageHistory(patient_id=patient_id)

    # categories_shown_last_6d — only within the 6-day window
    history.categories_shown_last_6d = list(set(
        r["category"] for r in rows
        if str(r["message_date"]) >= lookback_date_6d
    ))

    # keys_shown_last_6d — specific opportunity keys within 6-day window
    keys = set()
    for r in rows:
        if str(r["message_date"]) >= lookback_date_6d:
            opp = r.get("opportunity_used")
            if opp:
                keys.add(opp)
    history.keys_shown_last_6d = list(keys)

    # weight_messages_this_week — full 7-day window (requirement: max 2x/week)
    history.weight_messages_this_week = sum(1 for r in rows if r["category"] == "weight")

    # weight_shown_yesterday
    history.weight_shown_yesterday = any(
        r["category"] == "weight" and str(r["message_date"]) == yesterday_str
        for r in rows
    )

    # category_streaks — consecutive days ending at yesterday
    category_dates = defaultdict(set)
    for r in rows:
        category_dates[r["category"]].add(str(r["message_date"]))

    yesterday_dt = _insight_date - timedelta(days=1)
    for cat, dates in category_dates.items():
        streak = 0
        for i in range(7):
            check_date = (yesterday_dt - timedelta(days=i)).strftime('%Y-%m-%d')
            if check_date in dates:
                streak += 1
            else:
                break
        if streak > 0:
            history.category_streaks[cat] = streak

    return history

# ----------------------------------------------------------
# 7c. Per-patient worker: pure Python + LLM only (no Spark)
# ----------------------------------------------------------
def process_patient(patient_id: str):
    features = features_map.get(patient_id)
    if not features:
        return None, {"patient_id": patient_id, "error": "No feature row found"}

    profile  = get_user_profile(patient_id)
    context  = build_user_context(features, profile)
    history  = _build_history_from_cache(patient_id)
    selected: SelectedContent = logic_engine.select_content(context, history)

    gen = insight_generator.generate_insight(
        daily_rating       = selected.daily_rating,
        rating_description = selected.rating_description,
        positive_actions   = selected.positive_actions,
        opportunity        = selected.opportunity,
        greeting           = selected.greeting,
    )

    if not gen["success"]:
        return None, {"patient_id": patient_id, "error": gen.get("error", "LLM failure")}

    result_row = {
        "patient_id"           : patient_id,
        "insight_date"         : insight_date_str,
        "insight"              : gen["message"],
        "score_name"           : selected.daily_rating,
        "generated_at"         : datetime.utcnow().isoformat(),
        "rating_description"   : selected.rating_description,
        "character_count"      : gen.get("character_count", len(gen["message"])),
        "word_count"           : len(gen["message"].split()),
        "positive_actions_used": gen.get("positive_actions_used", []),
        "opportunity_used"     : gen.get("opportunity_used", ""),
    }
    return result_row, None

# ----------------------------------------------------------
# 7d. Run LLM calls concurrently
# ----------------------------------------------------------
completed = 0
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_pid = {executor.submit(process_patient, pid): pid for pid in patient_ids}

    for future in as_completed(future_to_pid):
        pid = future_to_pid[future]
        completed += 1
        try:
            result_row, error = future.result()
        except Exception as exc:
            failed.append({"patient_id": pid, "error": str(exc), "trace": traceback.format_exc()})
            result_row, error = None, None

        if error:
            failed.append(error)
        elif result_row:
            results.append(result_row)
            history_batch.append(result_row)

        if completed % 50 == 0 or completed == total_patients:
            pct = round(completed / total_patients * 100)
            print(f"  Progress: {completed}/{total_patients} ({pct}%)  |  failures so far: {len(failed)}")

print(f"\n✓ Generation complete — {len(results)} succeeded, {len(failed)} failed")

# ----------------------------------------------------------
# 7e. Batch-write all history rows in a single MERGE (replaces N×M individual MERGEs)
# ----------------------------------------------------------
print("Writing message history (batch upsert)…")
if history_batch:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

    history_schema = StructType([
        StructField("patientid",             StringType(),              True),
        StructField("category",              StringType(),              True),
        StructField("message_date",          StringType(),              True),
        StructField("message_text",          StringType(),              True),
        StructField("rating",                StringType(),              True),
        StructField("rating_description",    StringType(),              True),
        StructField("positive_actions_used", ArrayType(StringType()),   True),
        StructField("opportunity_used",      StringType(),              True),
        StructField("character_count",       IntegerType(),             True),
        StructField("word_count",            IntegerType(),             True),
    ])

    history_rows = []
    for result in history_batch:
        action_keys = result.get("positive_actions_used", [])
        opp_key     = result.get("opportunity_used", "")
        categories  = _extract_categories_from_actions(action_keys + ([opp_key] if opp_key else []))
        for category in categories:
            history_rows.append({
                "patientid"            : result["patient_id"],
                "category"             : category,
                "message_date"         : result["insight_date"],
                "message_text"         : result["insight"],
                "rating"               : result.get("score_name", ""),
                "rating_description"   : result.get("rating_description", ""),
                "positive_actions_used": action_keys,
                "opportunity_used"     : opp_key,
                "character_count"      : result.get("character_count", 0),
                "word_count"           : result.get("word_count", 0),
            })

    spark.createDataFrame(history_rows, schema=history_schema).createOrReplaceTempView("tmp_history_batch")

    spark.sql(f"""
        MERGE INTO {HISTORY_TABLE} AS target
        USING tmp_history_batch AS source
            ON  target.patientid    = source.patientid
            AND target.category     = source.category
            AND target.message_date = source.message_date
        WHEN MATCHED THEN
            UPDATE SET
                target.message_text          = source.message_text,
                target.rating                = source.rating,
                target.rating_description    = source.rating_description,
                target.positive_actions_used = source.positive_actions_used,
                target.opportunity_used      = source.opportunity_used,
                target.character_count       = source.character_count,
                target.word_count            = source.word_count,
                target.created_at            = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (patientid, category, message_date, message_text, rating,
                    rating_description, positive_actions_used, opportunity_used,
                    character_count, word_count, created_at)
            VALUES (source.patientid, source.category, source.message_date,
                    source.message_text, source.rating, source.rating_description,
                    source.positive_actions_used, source.opportunity_used,
                    source.character_count, source.word_count, current_timestamp())
    """)
    print(f"  ✓ Upserted {len(history_rows)} history rows in one batch MERGE")
else:
    print("  No history rows to write")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Write Results to Output Table (MERGE / Upsert)

# COMMAND ----------

if results:
    from pyspark.sql.types import StructType, StructField, StringType

    output_schema = StructType([
        StructField("patient_id",   StringType(), True),
        StructField("insight_date", StringType(), True),
        StructField("insight",      StringType(), True),
        StructField("score_name",   StringType(), True),
        StructField("generated_at", StringType(), True),
    ])

    output_rows = [
        {
            "patient_id"  : r["patient_id"],
            "insight_date": r["insight_date"],
            "insight"     : r["insight"],
            "score_name"  : r["score_name"],
            "generated_at": r["generated_at"],
        }
        for r in results
    ]

    result_df = spark.createDataFrame(output_rows, schema=output_schema)

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
    WHERE insight_date = '{insight_date_str}'
    GROUP BY insight_date
""").display()
