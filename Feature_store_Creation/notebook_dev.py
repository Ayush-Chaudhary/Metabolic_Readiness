# Databricks notebook source
# MAGIC %md
# MAGIC # Health Habits Feature Store - Gold Layer Creation
# MAGIC 
# MAGIC This notebook creates a daily feature store for personalized health messaging.
# MAGIC 
# MAGIC **Purpose**: Transform Bronze/Silver health data into a Gold layer with:
# MAGIC - Daily aggregated metrics per patient
# MAGIC - Timezone-corrected observations
# MAGIC - Historical context (trends, streaks, lookbacks)
# MAGIC - Eligibility flags for positive actions and opportunities
# MAGIC 
# MAGIC **Dependencies**: PySpark, Delta Lake, Databricks Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Section
# MAGIC All configurable parameters are centralized here

# COMMAND ----------

# ===== WIDGETS =====
# Configure source and target catalogs/tables via Databricks widgets.
# Change these values to point the notebook at a different environment.
dbutils.widgets.text("source_catalog",   "bronz_als_prod",          "Source Catalog")
dbutils.widgets.text("gold_catalog",     "bronz_als_azuat2",           "Gold Catalog")
dbutils.widgets.text("gold_schema",      "llm",                        "Gold Schema")
dbutils.widgets.text("gold_table_name",  "user_daily_health_habits",   "Gold Table Name")
dbutils.widgets.text("insight_date",     "",                            "Insight Date (YYYY-MM-DD, blank=today)")

_source_catalog  = dbutils.widgets.get("source_catalog")
_gold_catalog    = dbutils.widgets.get("gold_catalog")
_gold_schema     = dbutils.widgets.get("gold_schema")
_gold_table_name = dbutils.widgets.get("gold_table_name")
_insight_date_raw = dbutils.widgets.get("insight_date").strip()

# Resolve insight_date: defaults to today when blank; the feature store
# will use (insight_date - 1 day) as the reference "yesterday" for features.
from datetime import datetime as _dt, timedelta as _td
if _insight_date_raw:
    _insight_date = _dt.strptime(_insight_date_raw, "%Y-%m-%d").date()
else:
    _insight_date = _dt.now().date()
_feature_date = _insight_date - _td(days=1)

print(f"source_catalog  : {_source_catalog}")
print(f"gold_catalog    : {_gold_catalog}")
print(f"gold_schema     : {_gold_schema}")
print(f"gold_table_name : {_gold_table_name}")
print(f"insight_date    : {_insight_date}  (feature / yesterday: {_feature_date})")

# COMMAND ----------

from typing import Dict, Any
from datetime import datetime

# ===== TABLE CONFIGURATION =====
CONFIG: Dict[str, Any] = {
    
    # Source Tables (Bronze/Silver)
    "source_tables": {
        "glucose": f"{_source_catalog}.trxdb_dsmbasedb_observation.elogbgentry",
        "activity": f"{_source_catalog}.trxdb_dsmbasedb_observation.elogexerciseentry",
        "steps": f"{_source_catalog}.trxdb_dsmbasedb_observation.stepentry",
        "weight": f"{_source_catalog}.trxdb_dsmbasedb_observation.elogweightentry",
        "weight_goals": f"{_source_catalog}.trxdb_dsmbasedb_observation.weightgoal",
        "food": f"{_source_catalog}.trxdb_dsmbasedb_observation.foodmoduleitem",
        "sleep": f"{_source_catalog}.trxdb_dsmbasedb_observation.sleepentry",
        "med_administration": f"{_source_catalog}.trxdb_dsmbasedb_observation.medadministration",
        "med_prescription": f"{_source_catalog}.trxdb_dsmbasedb_observation.medprescription",
        "med_schedule": f"{_source_catalog}.trxdb_dsmbasedb_observation.medprescriptiondayschedule",
        "patient_nutrition_goals": f"{_source_catalog}.trxdb_dsmbasedb_user.patientgoaldetails",
        "a1c_target": f"{_source_catalog}.trxdb_dsmbasedb_observation.patienttargetsegment",
        "journal": f"{_source_catalog}.trxdb_dsmbasedb_userengagement.userjournal",
        "grocery": f"{_source_catalog}.trxdb_dsmbasedb_user.grocerydetails",
        "action_plan": f"{_source_catalog}.trxdb_dsmbasedb_user.actionplanprogress",
        # Note: meditation is derived from the activity table (exercisetype=30045)
        # Journey and exercise tracking tables
        "journey": f"{_source_catalog}.trxdb_dsmbasedb_user.GuidedJourneyWeeksAndTasksDetail",
        "exercise_video": f"{_source_catalog}.trxdb_dsmbasedb_user.curatedvideositemdetail",
        "exercise_program": f"{_source_catalog}.trxdb_dsmbasedb_user.curatedvideosprogramdetail",
        "user_focus": f"{_source_catalog}.trxdb_dsmbasedb_user.customizemyappdetails",
    },
    
    # Target Gold Layer
    "gold_table": {
        "catalog": _gold_catalog,
        "schema": _gold_schema,
        "table_name": _gold_table_name,
    },
    
    # Status/Enum Mappings
    "status_values": {
        "active_status": [1, 2],  # Valid numeric status codes: 1=Active, 2=Completed
        "cgm_source_id": 18,  # External source ID for CGM data
        "meditation_exercise_type": 30045,  # exercisetype enum for meditation
        "a1c_target_mapping": {1: "dm_target_7", 2: "dm_target_8", 3: "dm_target_6"},
        "action_plan_status": {1: "final", 2: "completed", 3: "deleted"},
        # Journey status: 1=active, 2=completed, 3=incomplete
        "journey_status": {"active": 1, "completed": 2, "incomplete": 3},
        # Exercise video status: 1=active, 2=completed
        "exercise_video_status": {"active": 1, "completed": 2},
        # Exercise program status: 1=active, 2=completed, 3=stopped
        "exercise_program_status": {"active": 1, "completed": 2, "stopped": 3},
    },
    
    # Business Logic Thresholds
    "thresholds": {
        # Glucose (Time in Range)
        "glucose_target_low": 70,   # mg/dL
        "glucose_target_high": 180,  # mg/dL
        "glucose_low_threshold": 70,
        "glucose_very_low_threshold": 54,
        
        # A1C-Based TIR Targets (configurable per patient later)
        "a1c_targets": {
            "dm_target_7": {"tir_min": 70, "high_max": 25, "low_max": 4},
            "dm_target_8": {"tir_min": 50, "high_max": 50, "low_max": 1},
            "dip": {"tir_min": 70, "high_max": 25, "low_max": 5},
            "non_dm": {"tir_min": 90, "high_max": 5, "low_max": 1},
            "default": {"tir_min": 70, "high_max": 25, "low_max": 4},  # Default to DM <7
        },
        
        # Activity & Steps
        "steps_daily_target": 10000,
        "steps_weekly_avg_min": 6000,
        "activity_minutes_weekly_target": 150,
        "activity_tolerance_minutes": 3,  # For "same as yesterday" logic
        
        # Weight
        "weight_maintenance_tolerance_pct": 3,  # +/- 3%
        "weight_log_frequency_days": 6,
        
        # Sleep
        "sleep_hours_target": 7,
        "sleep_rating_target": 7,
        
        # Medication
        "med_adherence_opportunity_threshold": 50,  # < 50% triggers opportunity
        
        # Food
        "nutrient_target_range": (90, 110),  # 90-110% of goal
        "exercise_video_completion_pct": 90,
        
        # Historical Context
        "lookback_days": 6,  # For "not shown in last 6 days" logic
        "streak_days": 3,    # For identifying 3-day streaks
        "monthly_lookback": 30,
    },
    
    # Journey Configuration (now enabled with actual table)
    "journey_config": {
        "enabled": True,
        "journey_table": f"{_source_catalog}.trxdb_dsmbasedb_user.GuidedJourneyWeeksAndTasksDetail",
    },
    
    # Processing Configuration
    "processing": {
        "incremental": True,  # Set to False for full refresh
        "lookback_window_days": 35,  # 30-day max rolling window + 5-day buffer
        "partition_by": "report_date",
    }
}

# Helper function to get full table path
def get_table_path(table_key: str) -> str:
    """Returns full table path from CONFIG"""
    return CONFIG["source_tables"][table_key]

def get_gold_table_path() -> str:
    """Returns full Gold table path"""
    g = CONFIG["gold_table"]
    return f"{g['catalog']}.{g['schema']}.{g['table_name']}"

print("✓ Configuration loaded")
print(f"Gold table target: {get_gold_table_path()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Helper Functions - Timezone & Date Utilities

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def add_local_date(df: DataFrame, 
                   timestamp_col: str, 
                   offset_col: str = "timezoneoffset",
                   alias: str = "local_date") -> DataFrame:
    """
    Converts UTC timestamp to local date using timezone offset.
    
    Args:
        df: Input DataFrame
        timestamp_col: Name of timestamp column
        offset_col: Name of timezone offset column (in minutes)
        alias: Name for output date column
    
    Returns:
        DataFrame with local_date column added
    """
    return df.withColumn(
        alias,
        F.to_date(
            F.from_unixtime(
                F.unix_timestamp(F.col(timestamp_col)) + (F.col(offset_col) * 60)
            )
        )
    )

def filter_active_records(df: DataFrame, status_col: str = "observationstatus") -> DataFrame:
    """
    Filters to only active/completed records based on CONFIG.
    Note: observationstatus is a numeric column (BIGINT), not string.
    """
    valid_statuses = CONFIG["status_values"]["active_status"]
    
    # Filter to numeric status codes
    condition = (F.col(status_col).isin(valid_statuses))
    
    return df.filter(condition)

def add_day_over_day_delta(df: DataFrame, 
                           value_col: str,
                           partition_col: str = "patientid",
                           order_col: str = "local_date",
                           alias_suffix: str = "_delta_1d") -> DataFrame:
    """
    Adds a column with day-over-day change.
    """
    window = Window.partitionBy(partition_col).orderBy(order_col)
    
    return df.withColumn(
        f"{value_col}{alias_suffix}",
        F.col(value_col) - F.lag(value_col, 1).over(window)
    )

def add_rolling_avg(df: DataFrame,
                   value_col: str,
                   window_days: int,
                   partition_col: str = "patientid",
                   order_col: str = "local_date",
                   alias_suffix: str = None) -> DataFrame:
    """
    Adds rolling average over specified window.
    """
    if alias_suffix is None:
        alias_suffix = f"_avg_{window_days}d"
    
    days = lambda i: i * 86400  # Convert days to seconds
    window = (Window
              .partitionBy(partition_col)
              .orderBy(F.col(order_col).cast("timestamp").cast("long"))
              .rangeBetween(-days(window_days-1), 0))
    
    return df.withColumn(
        f"{value_col}{alias_suffix}",
        F.avg(value_col).over(window)
    )

print("✓ Helper functions defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2b. Feature Engineering - A1C Target Segment

# COMMAND ----------

def create_a1c_target_features() -> DataFrame:
    """
    Reads the patient A1C target segment table and maps each patient to their
    A1C target group (dm_target_7, dm_target_8, dm_target_6).
    
    Uses the most recent record per patient based on lastmodifieddatetime.
    Enum: 1 = less than 7, 2 = less than 8, 3 = less than 6
    """
    
    a1c_df = (spark.read.table(get_table_path("a1c_target"))
              .select("patientid", "lastmodifieddatetime", "a1ctarget"))
    
    window_latest = (Window.partitionBy("patientid")
                     .orderBy(F.col("lastmodifieddatetime").desc()))
    
    a1c_latest = (a1c_df
                  .withColumn("rn", F.row_number().over(window_latest))
                  .filter(F.col("rn") == 1)
                  .drop("rn"))
    
    a1c_mapping = CONFIG["status_values"]["a1c_target_mapping"]
    mapping_expr = F.create_map([F.lit(x) for kv in a1c_mapping.items() for x in kv])
    
    a1c_latest = a1c_latest.withColumn(
        "a1c_target_group",
        mapping_expr[F.col("a1ctarget")]
    # ).withColumn(
    #     "a1c_target_group",
    #     F.coalesce(F.col("a1c_target_group"), F.lit("dm_target_7"))
    )
    
    return a1c_latest.select("patientid", "a1c_target_group")

print("✓ A1C target feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Feature Engineering - Glucose & CGM

# COMMAND ----------

def create_glucose_features() -> DataFrame:
    """
    Calculates daily glucose metrics including TIR, high/low percentages, and CGM detection.
    """
    
    # Read and prepare glucose data
    glucose_df = (spark.read.table(get_table_path("glucose"))
                  .select(
                      "patientid",
                      "bgvalue",
                      "observationstatus",
                      "observationdatetime",
                      "timezoneoffset",
                      "externalsourceid"
                  ))
    
    # Apply timezone and filter
    glucose_df = add_local_date(glucose_df, "observationdatetime")
    glucose_df = filter_active_records(glucose_df)
    
    # Define thresholds
    target_low = CONFIG["thresholds"]["glucose_target_low"]
    target_high = CONFIG["thresholds"]["glucose_target_high"]
    low_threshold = CONFIG["thresholds"]["glucose_low_threshold"]
    very_low = CONFIG["thresholds"]["glucose_very_low_threshold"]
    cgm_id = CONFIG["status_values"]["cgm_source_id"]
    
    # Calculate daily aggregations
    glucose_daily = glucose_df.groupBy("patientid", "local_date").agg(
        # Total readings
        F.count("*").alias("glucose_reading_count"),
        
        # Time in Range (TIR) - safe division
        (F.sum(F.when(F.col("bgvalue").between(target_low, target_high), 1).otherwise(0)) 
         / F.greatest(F.count("*"), F.lit(1)) * 100).alias("tir_pct"),
        
        # Time in High - safe division
        (F.sum(F.when(F.col("bgvalue") > target_high, 1).otherwise(0)) 
         / F.greatest(F.count("*"), F.lit(1)) * 100).alias("glucose_high_pct"),
        
        # Time in Low - safe division
        (F.sum(F.when(F.col("bgvalue") < low_threshold, 1).otherwise(0)) 
         / F.greatest(F.count("*"), F.lit(1)) * 100).alias("glucose_low_pct"),
        
        # Time in Very Low - safe division
        (F.sum(F.when(F.col("bgvalue") < very_low, 1).otherwise(0)) 
         / F.greatest(F.count("*"), F.lit(1)) * 100).alias("glucose_very_low_pct"),
        
        # CGM Detection
        F.max(F.when(F.col("externalsourceid") == cgm_id, 1).otherwise(0)).alias("has_cgm_connected"),
        
        # Average glucose
        F.avg("bgvalue").alias("avg_glucose")
    )
    
    # Add day-over-day delta for TIR
    glucose_daily = add_day_over_day_delta(glucose_daily, "tir_pct")
    
    return glucose_daily

print("✓ Glucose feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Feature Engineering - Activity & Exercise

# COMMAND ----------

def create_activity_features() -> DataFrame:
    """
    Calculates daily activity metrics including exercise minutes and rolling averages.
    Excludes meditation entries (exercisetype=30045) from activity totals.
    """
    
    meditation_type = CONFIG["status_values"]["meditation_exercise_type"]
    
    # Read activity data
    activity_df = (spark.read.table(get_table_path("activity"))
                   .select(
                       "patientid",
                       "exerciseduration",
                       "exercisetype",
                       "observationdatetime",
                       "timezoneoffset"
                   ))
    
    # Apply timezone
    activity_df = add_local_date(activity_df, "observationdatetime")
    
    # Exclude meditation from activity metrics
    physical_activity_df = activity_df.filter(F.col("exercisetype") != meditation_type)
    
    # Calculate daily aggregations
    activity_daily = physical_activity_df.groupBy("patientid", "local_date").agg(
        F.sum("exerciseduration").alias("active_minutes"),
        F.count("*").alias("exercise_session_count"),
        F.countDistinct("exercisetype").alias("exercise_variety_count")
    )
    
    # Add day-over-day delta
    activity_daily = add_day_over_day_delta(activity_daily, "active_minutes")
    
    # Add 7-day rolling sum for weekly target comparison
    window = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    activity_daily = activity_daily.withColumn(
        "active_minutes_7d_sum",
        F.sum("active_minutes").over(window)
    )
    
    # Calculate if within tolerance of previous day (for "same as yesterday" logic)
    tolerance = CONFIG["thresholds"]["activity_tolerance_minutes"]
    activity_daily = activity_daily.withColumn(
        "active_min_same_or_more",
        F.when(
            (F.col("active_minutes_delta_1d") >= -tolerance),
            1
        ).otherwise(0)
    )
    
    return activity_daily

print("✓ Activity feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4b. Feature Engineering - Meditation (from Activity table)

# COMMAND ----------

def create_meditation_features() -> DataFrame:
    """
    Extracts meditation entries from the activity table (exercisetype=30045).
    Computes daily, weekly, and monthly flags for mental wellbeing scoring.
    """
    
    meditation_type = CONFIG["status_values"]["meditation_exercise_type"]
    
    meditation_df = (spark.read.table(get_table_path("activity"))
                     .select("patientid", "exercisetype", "observationdatetime", "timezoneoffset")
                     .filter(F.col("exercisetype") == meditation_type))
    
    meditation_df = add_local_date(meditation_df, "observationdatetime")
    
    meditation_daily = meditation_df.groupBy("patientid", "local_date").agg(
        F.count("*").alias("meditation_count")
    ).withColumn("meditation_opened_today", F.lit(1))
    
    # Rolling 7-day and 30-day flags
    window_7d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-29, 0)
    
    meditation_daily = meditation_daily.withColumn(
        "meditation_opened_7d",
        F.when(F.sum("meditation_count").over(window_7d) > 0, 1).otherwise(0)
    ).withColumn(
        "meditation_opened_30d",
        F.when(F.sum("meditation_count").over(window_30d) > 0, 1).otherwise(0)
    )
    
    return meditation_daily

print("✓ Meditation feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Feature Engineering - Steps

# COMMAND ----------

def create_steps_features() -> DataFrame:
    """
    Calculates daily step counts, comparisons, and rolling averages.
    """
    
    # Read steps data
    steps_df = (spark.read.table(get_table_path("steps"))
                .select(
                    "patientid",
                    "numberofsteps",
                    "startdatetime",
                    "enddatetime",
                    "timezoneoffset",
                    "observationstatus"
                ))
    
    # Apply timezone (use startdatetime for date bucketing)
    steps_df = add_local_date(steps_df, "startdatetime")
    
    # Filter to active/completed records and non-null step counts
    steps_df = filter_active_records(steps_df, "observationstatus")
    steps_df = steps_df.filter(F.col("numberofsteps").isNotNull())
    
    # Calculate daily totals
    steps_daily = steps_df.groupBy("patientid", "local_date").agg(
        F.sum("numberofsteps").alias("daily_step_count")
    )
    
    # Add day-over-day comparison
    steps_daily = add_day_over_day_delta(steps_daily, "daily_step_count")
    
    # Add 7-day rolling average
    steps_daily = add_rolling_avg(steps_daily, "daily_step_count", 7)
    
    # Flag if more steps than previous day
    steps_daily = steps_daily.withColumn(
        "steps_more_than_prev",
        F.when(F.col("daily_step_count_delta_1d") > 0, 1).otherwise(0)
    )
    
    # Flag if step tracker is connected (if any steps logged)
    steps_daily = steps_daily.withColumn(
        "has_step_tracker",
        F.when(F.col("daily_step_count").isNotNull(), 1).otherwise(0)
    )
    
    # Check if daily target met
    target = CONFIG["thresholds"]["steps_daily_target"]
    steps_daily = steps_daily.withColumn(
        "steps_target_met",
        F.when(F.col("daily_step_count") >= target, 1).otherwise(0)
    )
    
    return steps_daily

print("✓ Steps feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Feature Engineering - Weight & Goals

# COMMAND ----------

def create_weight_features() -> DataFrame:
    """
    Calculates weight tracking metrics including logging frequency and goal progress.
    Joins with the weightgoal table to add:
    - has_weight_goal: bool
    - weight_goal_type: "lose" | "gain" | "maintain"
    - is_within_maintenance_range: bool (within +/- 3% of target weight)
    - distance_from_goal: current weight minus target weight (lbs)
    """
    
    # Read weight entries
    weight_df = (spark.read.table(get_table_path("weight"))
                 .select(
                     "patientid",
                     "weight",
                     "weightuomid",
                     "observationdatetime",
                     "timezoneoffset"
                 ))
    
    # Apply timezone
    weight_df = add_local_date(weight_df, "observationdatetime")
    
    # Normalize to pounds (assuming weightuomid defaults to lbs, add conversion if needed)
    weight_df = weight_df.withColumn(
        "weight_lbs",
        F.col("weight")  # Add conversion logic here if needed
    )
    
    # Get latest weight per day (in case multiple entries)
    window_latest = (Window.partitionBy("patientid", "local_date")
                     .orderBy(F.col("observationdatetime").desc()))
    
    weight_daily = (weight_df
                    .withColumn("rn", F.row_number().over(window_latest))
                    .filter(F.col("rn") == 1)
                    .drop("rn"))
    
    # Calculate days since last weight entry
    window_all = Window.partitionBy("patientid").orderBy(F.col("local_date").desc())
    weight_daily = weight_daily.withColumn(
        "days_since_last_weight",
        F.datediff(F.current_date(), F.col("local_date"))
    )
    
    # Add weight change from previous entry
    weight_daily = add_day_over_day_delta(weight_daily, "weight_lbs", alias_suffix="_delta")
    
    # Calculate weight change percentage - safe division (avoid divide by zero)
    prev_weight_window = Window.partitionBy("patientid").orderBy("local_date")
    weight_daily = weight_daily.withColumn(
        "weight_change_pct",
        F.when(
            (F.lag("weight_lbs", 1).over(prev_weight_window).isNotNull()) & 
            (F.lag("weight_lbs", 1).over(prev_weight_window) > 0),
            (F.col("weight_lbs_delta") / F.lag("weight_lbs", 1).over(prev_weight_window)) * 100
        ).otherwise(None)
    )
    
    # Flag: weight logged today
    weight_daily = weight_daily.withColumn(
        "weight_logged_today",
        F.lit(1)
    )
    
    # ──────────────────────────────────────────────
    # Join with weight goals table
    # ──────────────────────────────────────────────
    try:
        goals_df = (spark.read.table(get_table_path("weight_goals"))
                    .select(
                        "patientid",
                        "startdate",
                        "targetweight",
                        F.col("type").cast("string").alias("type"),
                        F.col("status").cast("string").alias("status"),
                        "targetreacheddate"
                    ))

        # Filter to active goals — compare as strings to avoid CAST errors when the
        # source table stores status as BIGINT and active_status list contains integers.
        valid_statuses = [str(s) for s in CONFIG["status_values"]["active_status"]]
        active_goals = goals_df.filter(F.col("status").isin(valid_statuses))
        
        # Get the most recent active goal per patient
        window_latest_goal = Window.partitionBy("patientid").orderBy(F.col("startdate").desc())
        active_goals = (active_goals
                       .withColumn("rn", F.row_number().over(window_latest_goal))
                       .filter(F.col("rn") == 1)
                       .drop("rn")
                       .withColumnRenamed("type", "weight_goal_type"))
        
        # Join weight entries with the patient's current goal
        weight_daily = weight_daily.join(
            active_goals.select("patientid", "weight_goal_type", "targetweight"),
            "patientid",
            "left"
        )
        
        # has_weight_goal: True if the patient has an active goal
        weight_daily = weight_daily.withColumn(
            "has_weight_goal",
            F.when(F.col("weight_goal_type").isNotNull(), 1).otherwise(0)
        )
        
        # is_within_maintenance_range: weight within +/- 3% of target (maintenance goal only)
        tol = CONFIG["thresholds"]["weight_maintenance_tolerance_pct"] / 100.0  # e.g. 0.03
        weight_daily = weight_daily.withColumn(
            "is_within_maintenance_range",
            F.when(
                (F.col("weight_goal_type") == "maintain") &
                F.col("targetweight").isNotNull() &
                (F.col("weight_lbs") >= F.col("targetweight") * (1 - tol)) &
                (F.col("weight_lbs") <= F.col("targetweight") * (1 + tol)),
                1
            ).otherwise(0)
        )
        
        # distance_from_goal: positive means above target, negative means below
        weight_daily = weight_daily.withColumn(
            "distance_from_goal",
            F.when(
                F.col("targetweight").isNotNull(),
                F.col("weight_lbs") - F.col("targetweight")
            ).otherwise(None)
        )
        
    except Exception as e:
        print(f"⚠ Warning: Could not join weight goals: {e}")
        weight_daily = weight_daily.withColumn("has_weight_goal", F.lit(0))
        weight_daily = weight_daily.withColumn("weight_goal_type", F.lit(None).cast("string"))
        weight_daily = weight_daily.withColumn("is_within_maintenance_range", F.lit(0))
        weight_daily = weight_daily.withColumn("distance_from_goal", F.lit(None).cast("double"))
    
    final_weight = weight_daily.select(
        "patientid",
        "local_date",
        "weight_lbs",
        "weight_lbs_delta",
        "weight_change_pct",
        "days_since_last_weight",
        "weight_logged_today",
        "has_weight_goal",
        "weight_goal_type",
        "is_within_maintenance_range",
        "distance_from_goal"
    )
    
    return final_weight

print("✓ Weight feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Feature Engineering - Food & Nutrition

# COMMAND ----------

def create_food_features() -> DataFrame:
    """
    Calculates daily food logging metrics and nutrient target achievement.
    """
    
    # Read food logs
    food_df = (spark.read.table(get_table_path("food"))
               .select(
                   "patientid",
                   "observationdatetime",
                   "timezoneoffset",
                   "carbs",
                   "fiber",
                   "fat",
                   "calories",
                   "protein",
                   "sugar",
                   "addedsugars",
                   "activitytypeid",  # breakfast, lunch, dinner, snack
                   "observationstatus"
               ))
    
    # Apply timezone and filter
    food_df = add_local_date(food_df, "observationdatetime")
    food_df = filter_active_records(food_df, "observationstatus")
    
    # Calculate daily aggregations
    food_daily = food_df.groupBy("patientid", "local_date").agg(
        # Count of meals logged
        F.count("*").alias("total_food_entries"),
        F.countDistinct("activitytypeid").alias("unique_meals_logged"),
        
        # Nutrient totals
        F.sum("calories").alias("total_calories"),
        F.sum("protein").alias("total_protein"),
        F.sum("carbs").alias("total_carbs"),
        F.sum("fat").alias("total_fat"),
        F.sum("fiber").alias("total_fiber"),
        F.sum("sugar").alias("total_sugar")
    )
    
    # Flag: logged at least one meal
    food_daily = food_daily.withColumn(
        "meal_logged_today",
        F.when(F.col("unique_meals_logged") >= 1, 1).otherwise(0)
    )
    
    # Calculate 7-day rolling count of days with >1 meal
    window = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    food_daily = food_daily.withColumn(
        "days_with_meals_7d",
        F.sum(F.when(F.col("unique_meals_logged") > 1, 1).otherwise(0)).over(window)
    )
    
    # Note: Nutrient target comparison is done in create_food_with_goals_features()
    
    return food_daily

print("✓ Food feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Feature Engineering - Food with Nutrition Goals

# COMMAND ----------

def create_food_with_goals_features() -> DataFrame:
    """
    Joins food features with nutrition goals to calculate target achievement.
    """
    
    # Get base food features
    food_daily = create_food_features()
    
    # Read nutrition goals
    try:
        goals_df = (spark.read.table(get_table_path("patient_nutrition_goals"))
                    .select(
                        "patientid",
                        "protein",
                        "fat",
                        "calories",
                        "carb"
                    )
                    .withColumnRenamed("protein", "goal_protein")
                    .withColumnRenamed("fat", "goal_fat")
                    .withColumnRenamed("calories", "goal_calories")
                    .withColumnRenamed("carb", "goal_carbs"))
        
        # Join with food data
        food_with_goals = food_daily.join(goals_df, "patientid", "left")
        
        # Calculate percentage of goal achieved
        target_min, target_max = CONFIG["thresholds"]["nutrient_target_range"]
        
        for nutrient in ["protein", "carbs", "fat", "calories"]:
            total_col = f"total_{nutrient}"
            goal_col = f"goal_{nutrient}"
            pct_col = f"{nutrient}_target_pct"
            met_col = f"{nutrient}_target_met"
            
            # Calculate percentage
            food_with_goals = food_with_goals.withColumn(
                pct_col,
                F.when(F.col(goal_col).isNotNull() & (F.col(goal_col) > 0),
                       (F.col(total_col) / F.col(goal_col)) * 100)
                .otherwise(None)
            )
            
            # Flag if within target range (90-110%)
            food_with_goals = food_with_goals.withColumn(
                met_col,
                F.when(F.col(pct_col).between(target_min, target_max), 1).otherwise(0)
            )
        
        # Flag: ANY nutrient target met
        food_with_goals = food_with_goals.withColumn(
            "any_nutrient_target_met",
            F.when(
                (F.col("protein_target_met") == 1) |
                (F.col("carbs_target_met") == 1) |
                (F.col("fat_target_met") == 1) |
                (F.col("calories_target_met") == 1),
                1
            ).otherwise(0)
        )
        
        return food_with_goals
        
    except Exception as e:
        print(f"⚠ Warning: Could not load nutrition goals table: {e}")
        print("  Returning food features without goal calculations")
        return food_daily

print("✓ Food with goals feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Feature Engineering - Sleep

# COMMAND ----------

def _merge_sleep_intervals(pdf):
    """
    Merge overlapping or contained sleep intervals to prevent double-counting duration.
    Called per (patientid, local_date) group via applyInPandas.

    Algorithm (O(n log n)):
      1. Sort by StartDateTime.
      2. Walk the sorted list once. If the next session starts before the current
         one ends, extend the current interval; otherwise close it and open a new one.

    Returns a DataFrame with columns:
      patientid, local_date, sleep_duration_hours, sleep_rating, sleep_entry_count
    """
    import pandas as pd

    patientid  = pdf['patientid'].iloc[0]
    local_date = pdf['local_date'].iloc[0]

    # Ratings are per-session and not affected by overlap — average across original rows
    avg_rating = pdf['sleeprating'].mean()
    entry_count = len(pdf)

    if pdf.empty:
        return pd.DataFrame([{
            'patientid': patientid,
            'local_date': local_date,
            'sleep_duration_hours': 0.0,
            'sleep_rating': avg_rating,
            'sleep_entry_count': 0,
        }])

    sorted_df = pdf.sort_values('startdatetime').reset_index(drop=True)
    current_start = sorted_df.loc[0, 'startdatetime']
    current_end   = sorted_df.loc[0, 'enddatetime']
    total_seconds = 0.0

    for i in range(1, len(sorted_df)):
        row_start = sorted_df.loc[i, 'startdatetime']
        row_end   = sorted_df.loc[i, 'enddatetime']
        if row_start <= current_end:
            # Overlapping or contained — extend current window
            current_end = max(current_end, row_end)
        else:
            # Non-overlapping gap — commit current window
            total_seconds += (current_end - current_start).total_seconds()
            current_start = row_start
            current_end   = row_end

    # Commit final window
    total_seconds += (current_end - current_start).total_seconds()

    return pd.DataFrame([{
        'patientid'           : str(patientid),
        'local_date'          : local_date,
        'sleep_duration_hours': total_seconds / 3600.0,
        'sleep_rating'        : avg_rating,
        'sleep_entry_count'   : int(entry_count),
    }])


def create_sleep_features() -> DataFrame:
    """
    Calculates sleep duration, ratings, and rolling averages.
    Overlapping sleep intervals are merged before summing so duration is not
    double-counted when the source table contains multiple overlapping sessions.
    """
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

    # Read sleep data
    sleep_df = (spark.read.table(get_table_path("sleep"))
                .select(
                    "patientid",
                    "startdatetime",
                    "enddatetime",
                    "timezoneoffset",
                    "observationstatus",
                    "sleeprating"
                ))

    # Apply timezone offset to get local timestamps, then derive local_date from enddatetime
    # (sleep counts for the day it ended — when the patient woke up)
    sleep_df = (
        sleep_df
        .withColumn(
            "startdatetime",
            F.to_timestamp(F.from_unixtime(
                F.unix_timestamp(F.col("startdatetime")) + (F.col("timezoneoffset") * 60)
            ))
        )
        .withColumn(
            "enddatetime",
            F.to_timestamp(F.from_unixtime(
                F.unix_timestamp(F.col("enddatetime")) + (F.col("timezoneoffset") * 60)
            ))
        )
        .withColumn("local_date", F.to_date(F.col("enddatetime")))
    )

    sleep_df = filter_active_records(sleep_df, "observationstatus")

    # Schema returned by _merge_sleep_intervals
    merge_schema = StructType([
        StructField("patientid",            StringType(), True),
        StructField("local_date",           DateType(),   True),
        StructField("sleep_duration_hours", DoubleType(), True),
        StructField("sleep_rating",         DoubleType(), True),
        StructField("sleep_entry_count",    LongType(),   True),
    ])

    # Merge overlapping intervals per (patientid, local_date) group
    sleep_daily = sleep_df.groupBy("patientid", "local_date").applyInPandas(
        _merge_sleep_intervals, schema=merge_schema
    )
    
    # Add day-over-day deltas
    sleep_daily = add_day_over_day_delta(sleep_daily, "sleep_duration_hours")
    sleep_daily = add_day_over_day_delta(sleep_daily, "sleep_rating")
    
    # Add 7-day rolling averages
    sleep_daily = add_rolling_avg(sleep_daily, "sleep_duration_hours", 7)
    sleep_daily = add_rolling_avg(sleep_daily, "sleep_rating", 7)
    
    # Flags for targets
    sleep_target_hrs = CONFIG["thresholds"]["sleep_hours_target"]
    sleep_target_rating = CONFIG["thresholds"]["sleep_rating_target"]
    
    sleep_daily = sleep_daily.withColumn(
        "sleep_hours_target_met",
        F.when(F.col("sleep_duration_hours") >= sleep_target_hrs, 1).otherwise(0)
    )
    
    sleep_daily = sleep_daily.withColumn(
        "sleep_rating_target_met",
        F.when(F.col("sleep_rating") >= sleep_target_rating, 1).otherwise(0)
    )
    
    # Flag: slept more than previous day
    sleep_daily = sleep_daily.withColumn(
        "slept_more_than_prev",
        F.when(F.col("sleep_duration_hours_delta_1d") > 0, 1).otherwise(0)
    )
    
    # Flag: rating better than previous day
    sleep_daily = sleep_daily.withColumn(
        "rating_better_than_prev",
        F.when(F.col("sleep_rating_delta_1d") > 0, 1).otherwise(0)
    )
    
    return sleep_daily

print("✓ Sleep feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Feature Engineering - Medications

# COMMAND ----------

def create_medication_features() -> DataFrame:
    """
    Calculates medication adherence metrics by joining administration with prescriptions.
    
    Uses per-medication expected dose calculation based on frequencytype/frequencyvalue
    and caps adherence per-medication to prevent over-taking one med from masking
    missed doses of another.
    
    Output columns:
    - meds_taken_count: Total doses taken that day
    - active_prescription_count: Number of active prescriptions for the patient
    - expected_daily_doses: Total expected doses across all active prescriptions
    - med_adherence_pct_1d: Adherence % (capped at 100 per medication)
    - took_all_meds: Boolean flag if adherence == 100%
    - med_adherence_7d_avg: Rolling 7-day average adherence
    - med_reminders_enabled: 1 if patient has any reminder set in medprescriptiondayschedule
    """
    
    # ──────────────────────────────────────────────
    # 1. Read & prepare administration records
    # ──────────────────────────────────────────────
    admin_df = (spark.read.table(get_table_path("med_administration"))
                .select(
                    "patientid",
                    "medicationid",
                    "medprescriptionid",
                    "statusid",
                    "dose",
                    "administrationdate",
                    "administrationtimezoneoffset"
                )
                .withColumnRenamed("administrationtimezoneoffset", "timezoneoffset"))
    
    # Apply timezone and filter to taken medications (statusid=1 means taken)
    admin_df = add_local_date(admin_df, "administrationdate")
    admin_df = admin_df.filter(F.col("statusid") == 1)
    
    # ──────────────────────────────────────────────
    # 2. Read & prepare prescription records
    # ──────────────────────────────────────────────
    try:
        prescription_df = (spark.read.table(get_table_path("med_prescription"))
                          .select(
                              "patientid",
                              "medprescriptionid",
                              "medicationid",
                              "frequencytype",
                              "frequencyvalue",
                              "startdate",
                              "statusid",
                              "discontinueddate"
                          ))
        
        # Filter to active prescriptions (statusid=1)
        active_rx = prescription_df.filter(F.col("statusid") == 1)
        
        # ──────────────────────────────────────────────
        # 3. Calculate expected daily doses per prescription
        # ──────────────────────────────────────────────
        # Determine doses per day from frequencyvalue (default to 1 if unknown)
        active_rx = active_rx.withColumn(
            "doses_per_day",
            F.coalesce(F.expr("try_cast(frequencyvalue as int)"), F.lit(1))
        )
        
        # Get the date each prescription became active (for date-range filtering)
        active_rx = active_rx.withColumn(
            "rx_start_date",
            F.to_date("startdate")
        ).withColumn(
            "rx_end_date",
            F.coalesce(F.to_date("discontinueddate"), F.current_date())
        )
        
        # ──────────────────────────────────────────────
        # 4. Count active prescriptions per patient (patient-level)
        # ──────────────────────────────────────────────
        prescription_count = active_rx.groupBy("patientid").agg(
            F.count("*").alias("active_prescription_count"),
            F.sum("doses_per_day").alias("expected_daily_doses")
        )
        
        # ──────────────────────────────────────────────
        # 5. Per-medication daily adherence (capped)
        # ──────────────────────────────────────────────
        # Count doses taken per patient, per medication, per day
        admin_per_med_day = admin_df.groupBy("patientid", "local_date", "medicationid").agg(
            F.count("*").alias("doses_taken")
        )
        
        # Join with prescriptions to get expected doses per med
        # A medication may have multiple prescriptions; sum their doses_per_day
        rx_per_med = active_rx.groupBy("patientid", "medicationid").agg(
            F.sum("doses_per_day").alias("expected_doses_per_med"),
            F.min("rx_start_date").alias("rx_start_date"),
            F.max("rx_end_date").alias("rx_end_date")
        )
        
        admin_with_expected = admin_per_med_day.join(
            rx_per_med,
            ["patientid", "medicationid"],
            "left"
        )
        
        # Filter: only count days where the prescription was active
        admin_with_expected = admin_with_expected.filter(
            (F.col("rx_start_date").isNull()) |  # no prescription record - still count
            (
                (F.col("local_date") >= F.col("rx_start_date")) &
                (F.col("local_date") <= F.col("rx_end_date"))
            )
        )
        
        # Cap doses taken at expected per medication (prevents over-taking from masking misses)
        admin_with_expected = admin_with_expected.withColumn(
            "expected_doses_per_med",
            F.coalesce(F.col("expected_doses_per_med"), F.lit(1))
        ).withColumn(
            "capped_doses_taken",
            F.least(F.col("doses_taken"), F.col("expected_doses_per_med"))
        )
        
        # ──────────────────────────────────────────────
        # 6. Aggregate to patient-day level
        # ──────────────────────────────────────────────
        med_daily = admin_with_expected.groupBy("patientid", "local_date").agg(
            F.sum("capped_doses_taken").alias("capped_taken_total"),
            F.sum("expected_doses_per_med").alias("expected_total"),
            F.sum("doses_taken").alias("meds_taken_count")
        )
        
        # Join with prescription counts
        med_daily = med_daily.join(prescription_count, "patientid", "left")
        
        # Calculate adherence: capped_taken / expected (per-med capping already applied)
        med_daily = med_daily.withColumn(
            "med_adherence_pct_1d",
            F.when(
                F.col("expected_total").isNotNull() & (F.col("expected_total") > 0),
                F.least(
                    (F.col("capped_taken_total") / F.col("expected_total")) * 100,
                    F.lit(100.0)
                )
            ).otherwise(None)
        )
        
        # Flag: took all meds (100% adherence)
        med_daily = med_daily.withColumn(
            "took_all_meds",
            F.when(F.col("med_adherence_pct_1d") >= 100, 1).otherwise(0)
        )
        
        # ──────────────────────────────────────────────
        # 7. Rolling 7-day adherence average
        # ──────────────────────────────────────────────
        window_7d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
        med_daily = med_daily.withColumn(
            "med_adherence_7d_avg",
            F.avg("med_adherence_pct_1d").over(window_7d)
        )
        
        # ──────────────────────────────────────────────
        # 8. Medication reminders from medprescriptiondayschedule
        # ──────────────────────────────────────────────
        try:
            schedule_df = (spark.read.table(get_table_path("med_schedule"))
                          .select("medprescriptionid", "medicationreminder"))
            
            # Join schedule records with active prescriptions to get patientid
            rx_with_reminders = (active_rx
                                 .select("patientid", "medprescriptionid")
                                 .join(schedule_df, "medprescriptionid", "inner"))
            
            # Patient has reminders enabled if ANY prescription schedule has a non-null,
            # non-zero medicationreminder value
            reminder_per_patient = rx_with_reminders.groupBy("patientid").agg(
                F.max(
                    F.when(
                        F.col("medicationreminder").isNotNull() &
                        (F.expr("try_cast(medicationreminder as int)") != 0),
                        1
                    ).otherwise(0)
                ).alias("med_reminders_enabled")
            )
            
            med_daily = med_daily.join(reminder_per_patient, "patientid", "left")
            med_daily = med_daily.withColumn(
                "med_reminders_enabled",
                F.coalesce(F.col("med_reminders_enabled"), F.lit(0))
            )
        except Exception as e:
            print(f"⚠ Warning: Could not fetch med reminders from schedule table: {e}")
            med_daily = med_daily.withColumn("med_reminders_enabled", F.lit(0))
        
        # Select final columns
        med_daily = med_daily.select(
            "patientid",
            "local_date",
            "meds_taken_count",
            "active_prescription_count",
            "expected_daily_doses",
            "med_adherence_pct_1d",
            "took_all_meds",
            "med_adherence_7d_avg",
            "med_reminders_enabled"
        )
        
    except Exception as e:
        print(f"⚠ Warning: Could not calculate adherence with prescriptions: {e}")
        # Fallback: simple count-based approach
        med_daily = admin_df.groupBy("patientid", "local_date").agg(
            F.count("*").alias("meds_taken_count")
        )
        med_daily = med_daily.withColumn("active_prescription_count", F.lit(None).cast("int"))
        med_daily = med_daily.withColumn("expected_daily_doses", F.lit(None).cast("int"))
        med_daily = med_daily.withColumn("med_adherence_pct_1d", F.lit(None).cast("double"))
        med_daily = med_daily.withColumn("took_all_meds", F.lit(0))
        med_daily = med_daily.withColumn("med_adherence_7d_avg", F.lit(None).cast("double"))
        med_daily = med_daily.withColumn("med_reminders_enabled", F.lit(0))
    
    return med_daily

print("✓ Medication feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10b. Feature Engineering - Journal Entries

# COMMAND ----------

def create_journal_features() -> DataFrame:
    """
    Computes journal entry features from the userjournal table.
    Produces daily, 7-day, and 30-day flags for mental wellbeing scoring.
    """
    
    journal_df = (spark.read.table(get_table_path("journal"))
                  .select(F.col("userid").alias("patientid"), "createddatetime"))
    
    journal_df = journal_df.withColumn(
        "local_date", F.to_date("createddatetime")
    )
    
    journal_daily = journal_df.groupBy("patientid", "local_date").agg(
        F.count("*").alias("journal_entry_count")
    ).withColumn("journal_entry_today", F.lit(1))
    
    window_7d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-29, 0)
    
    journal_daily = journal_daily.withColumn(
        "journal_entry_7d",
        F.when(F.sum("journal_entry_count").over(window_7d) > 0, 1).otherwise(0)
    ).withColumn(
        "journal_entry_30d",
        F.when(F.sum("journal_entry_count").over(window_30d) > 0, 1).otherwise(0)
    )
    
    return journal_daily

print("✓ Journal feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10c. Feature Engineering - Grocery

# COMMAND ----------

def create_grocery_features() -> DataFrame:
    """
    Computes daily grocery activity flag from the grocerydetails table.
    entrydatetimeinmillis is epoch milliseconds.
    """
    
    grocery_df = (spark.read.table(get_table_path("grocery"))
                  .select("patientid", "entrydatetimeinmillis"))
    
    grocery_df = grocery_df.withColumn(
        "local_date",
        F.to_date(F.from_unixtime(F.col("entrydatetimeinmillis") / 1000))
    )
    
    grocery_daily = grocery_df.groupBy("patientid", "local_date").agg(
        F.count("*").alias("grocery_entry_count")
    ).withColumn("grocery_shopped_today", F.lit(1))
    
    return grocery_daily

print("✓ Grocery feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10d. Feature Engineering - Action Plan Progress

# COMMAND ----------

def create_action_plan_features() -> DataFrame:
    """
    Computes action plan features from actionplanprogress table.
    
    actionplanstatus enum: 1=final (active), 2=completed, 3=deleted
    Uses createddate + timezoneoffset for local date.
    """
    
    action_plan_df = (spark.read.table(get_table_path("action_plan"))
                      .select("patientid", "statusid", "createddate", "timezoneoffset"))
    
    action_plan_df = add_local_date(action_plan_df, "createddate")
    
    # Filter out deleted plans
    action_plan_df = action_plan_df.filter(F.col("statusid") != 3)
    
    action_plan_daily = action_plan_df.groupBy("patientid", "local_date").agg(
        F.count("*").alias("action_plan_entries"),
        F.max(F.when(F.col("statusid") == 1, 1).otherwise(0)).alias("action_plan_active"),
        F.max(F.when(F.col("statusid") == 2, 1).otherwise(0)).alias("action_plan_completed_today"),
    )
    
    # Rolling 7-day and 30-day progress flags
    window_7d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    window_30d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-29, 0)
    
    action_plan_daily = action_plan_daily.withColumn(
        "action_plan_progress_7d",
        F.when(F.sum("action_plan_entries").over(window_7d) > 0, 1).otherwise(0)
    ).withColumn(
        "action_plan_progress_30d",
        F.when(F.sum("action_plan_entries").over(window_30d) > 0, 1).otherwise(0)
    )
    
    return action_plan_daily

print("✓ Action plan feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10e. Feature Engineering - Journey

# COMMAND ----------

def create_journey_features() -> DataFrame:
    """
    Computes journey tracking features from GuidedJourneyWeeksAndTasksDetail table.
    
    isjourneycompleted enum: 1=active, 2=completed, 3=incomplete
    
    Features:
    - has_active_journey: Boolean if patient has an active (status=1) journey
    - journey_completed_today: Boolean if a journey was completed (status=2) today
    - journey_task_completed: Derived based on progression (active to completed)
    """
    
    journey_status = CONFIG["status_values"]["journey_status"]
    
    journey_df = (spark.read.table(get_table_path("journey"))
                  .select("patientid", "isjourneycompleted"))
    
    # Since the table doesn't have a date column, we get the latest status per patient
    # and create a current snapshot of journey status
    journey_latest = journey_df.groupBy("patientid").agg(
        # Check if patient has any active journey
        F.max(F.when(F.col("isjourneycompleted") == journey_status["active"], 1)
              .otherwise(0)).alias("has_active_journey"),
        # Check if patient has any completed journey
        F.max(F.when(F.col("isjourneycompleted") == journey_status["completed"], 1)
              .otherwise(0)).alias("has_completed_journey"),
        # Count of active journeys
        F.sum(F.when(F.col("isjourneycompleted") == journey_status["active"], 1)
              .otherwise(0)).alias("active_journey_count"),
        # Count of completed journeys
        F.sum(F.when(F.col("isjourneycompleted") == journey_status["completed"], 1)
              .otherwise(0)).alias("completed_journey_count"),
    )
    
    # For now, journey_task_completed is approximated - a user is considered
    # to have completed a task if they have both active and completed journey items
    # (in a proper setup, this would need a date-based check)
    journey_latest = journey_latest.withColumn(
        "journey_task_completed",
        F.when(
            (F.col("active_journey_count") > 0) & (F.col("completed_journey_count") > 0), 1
        ).otherwise(0)
    )
    
    return journey_latest

print("✓ Journey feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10f. Feature Engineering - Exercise Video

# COMMAND ----------

def create_exercise_video_features() -> DataFrame:
    """
    Computes exercise video completion features from curatedvideositemdetail table.
    
    statusid enum: 1=active, 2=completed
    
    Features:
    - exercise_video_completed_today: Boolean if user completed a video today
    - exercise_video_completed_7d: Boolean if user completed a video in last 7 days
    - has_exercise_video_activity: Boolean if user has any video activity
    """
    
    video_status = CONFIG["status_values"]["exercise_video_status"]
    
    video_df = (spark.read.table(get_table_path("exercise_video"))
                .select("patientid", "createddate", "modifieddate", "statusid"))
    
    # Use modifieddate as the indicator of when the video was completed
    # (status changes happen on modification)
    video_df = video_df.withColumn(
        "local_date",
        F.to_date("modifieddate")
    )
    
    # Filter to completed videos (statusid=2)
    completed_videos = video_df.filter(F.col("statusid") == video_status["completed"])
    
    # Daily aggregation of completed videos
    video_daily = completed_videos.groupBy("patientid", "local_date").agg(
        F.count("*").alias("videos_completed_count")
    ).withColumn("exercise_video_completed_today", F.lit(1))
    
    # Add 7-day rolling flag
    window_7d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    
    video_daily = video_daily.withColumn(
        "exercise_video_completed_7d",
        F.when(F.sum("videos_completed_count").over(window_7d) > 0, 1).otherwise(0)
    )
    
    return video_daily


def create_exercise_video_patient_summary() -> DataFrame:
    """
    Creates a patient-level summary of exercise video activity.
    Useful for checking if a patient has ever completed a video.
    """
    
    video_status = CONFIG["status_values"]["exercise_video_status"]
    
    video_df = (spark.read.table(get_table_path("exercise_video"))
                .select("patientid", "createddate", "modifieddate", "statusid"))
    
    # Patient-level aggregation
    video_summary = video_df.groupBy("patientid").agg(
        # Has completed at least one video ever
        F.max(F.when(F.col("statusid") == video_status["completed"], 1)
              .otherwise(0)).alias("has_completed_video_ever"),
        # Count of completed videos
        F.sum(F.when(F.col("statusid") == video_status["completed"], 1)
              .otherwise(0)).alias("total_videos_completed"),
        # Has any video activity (started or completed)
        F.lit(1).alias("has_exercise_video_activity"),
    )
    
    return video_summary

print("✓ Exercise video feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10g. Feature Engineering - Exercise Program

# COMMAND ----------

def create_exercise_program_features() -> DataFrame:
    """
    Computes exercise program features from curatedvideosprogramdetail table.
    
    statusid enum: 1=active, 2=completed, 3=stopped
    
    Features:
    - has_active_exercise_program: Boolean if user has an active program
    - exercise_program_completed_today: Boolean if user completed a program today
    - exercise_program_started_today: Boolean if user started a new program today
    - exercise_program_progress: Boolean if there's been any activity (new or completed)
    """
    
    program_status = CONFIG["status_values"]["exercise_program_status"]
    
    program_df = (spark.read.table(get_table_path("exercise_program"))
                  .select("patientid", "statusid", "activateddatetime", 
                          "createddate", "modifieddate"))
    
    # Create local dates for different events
    program_df = program_df.withColumn(
        "activation_date",
        F.to_date("activateddatetime")
    ).withColumn(
        "created_date",
        F.to_date("createddate")
    ).withColumn(
        "modified_date",
        F.to_date("modifieddate")
    )
    
    # Patient-level summary (current status)
    program_summary = program_df.groupBy("patientid").agg(
        # Has at least one active program
        F.max(F.when(F.col("statusid") == program_status["active"], 1)
              .otherwise(0)).alias("has_active_exercise_program"),
        # Has completed at least one program
        F.max(F.when(F.col("statusid") == program_status["completed"], 1)
              .otherwise(0)).alias("has_completed_exercise_program"),
        # Count of active programs
        F.sum(F.when(F.col("statusid") == program_status["active"], 1)
              .otherwise(0)).alias("active_program_count"),
        # Total programs ever
        F.count("*").alias("total_programs"),
    )
    
    # For program progress (daily), we track modifications where status changed
    # Use modified_date as the key date

    # Programs completed - filter by status and use modified date
    completed_programs = program_df.filter(F.col("statusid") == program_status["completed"])
    completed_daily = completed_programs.groupBy("patientid", "modified_date").agg(
        F.count("*").alias("programs_completed_count")
    ).withColumnRenamed("modified_date", "local_date")
    completed_daily = completed_daily.withColumn("exercise_program_completed_today", F.lit(1))
    
    # Programs started - filter by status=active and use activation date
    started_programs = program_df.filter(F.col("statusid") == program_status["active"])
    started_daily = started_programs.groupBy("patientid", "activation_date").agg(
        F.count("*").alias("programs_started_count")
    ).withColumnRenamed("activation_date", "local_date")
    started_daily = started_daily.withColumn("exercise_program_started_today", F.lit(1))
    
    # Merge started and completed into a single daily view
    program_daily = (started_daily
                     .join(completed_daily, ["patientid", "local_date"], "full_outer")
                     .na.fill(0, ["programs_started_count", "programs_completed_count"]))
    
    # Fill nulls for the flag columns
    program_daily = program_daily.withColumn(
        "exercise_program_started_today",
        F.coalesce(F.col("exercise_program_started_today"), F.lit(0))
    ).withColumn(
        "exercise_program_completed_today",
        F.coalesce(F.col("exercise_program_completed_today"), F.lit(0))
    )
    
    # Flag for any program progress (started or completed)
    program_daily = program_daily.withColumn(
        "exercise_program_progress_today",
        F.when(
            (F.col("exercise_program_started_today") == 1) | 
            (F.col("exercise_program_completed_today") == 1), 1
        ).otherwise(0)
    )
    
    # Add 7-day rolling flag for program activity
    window_7d = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
    
    program_daily = program_daily.withColumn(
        "exercise_program_progress_7d",
        F.when(
            F.sum(
                F.col("programs_started_count") + F.col("programs_completed_count")
            ).over(window_7d) > 0, 1
        ).otherwise(0)
    )
    
    return program_daily, program_summary

print("✓ Exercise program feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10b. Feature Engineering - User Focus Areas

# COMMAND ----------

def create_focus_features() -> DataFrame:
    """
    Extracts active user focus areas from the customizemyappdetails table.
    
    The myfocusdata column contains a JSON array of structs with:
      - MyFocusID: int  (maps to focus name)
      - IsMyFocus: int  (0/1 whether it's active)
      - FocusOptionID: int (selection detail)
    
    Focus ID → Name mapping:
      1  → Medications
      2  → Glucose
      3  → Eating Habits
      5  → Blood Pressure    (not used in scoring)
      6  → Activity
      7  → Sleep
      8  → Weight
      11 → Thoughts          (not used in scoring)
      12 → Mood              (not used in scoring)
      13 → Anxiety
      19 → PAP Device        (not used in scoring)
    
    Returns:
        DataFrame with columns:
          - patientid (string)
          - user_focus (string, comma-separated list of active focus names,
                        e.g. "Weight,Glucose,Activity" or None if none active)
    """
    
    customize_df = spark.read.table(get_table_path("user_focus"))
    
    # Focus ID → category name mapping (only scoring-relevant focuses)
    focus_id_mapping = {
        1: "Medications",
        2: "Glucose",
        3: "Eating Habits",
        6: "Activity",
        7: "Sleep",
        8: "Weight",
        13: "Anxiety",
    }
    
    # Parse the myfocusdata JSON array
    focus_parsed = customize_df.select(
        F.col("patientid"),
        F.from_json(F.col("myfocusdata"), "array<struct<MyFocusID:int,IsMyFocus:int,FocusOptionID:int>>").alias("focus_array")
    ).select(
        F.col("patientid"),
        F.explode(F.col("focus_array")).alias("focus_item")
    ).select(
        F.col("patientid"),
        F.col("focus_item.MyFocusID").alias("focus_id"),
        F.col("focus_item.IsMyFocus").alias("is_active"),
    )
    
    # Filter to active focuses only (IsMyFocus = 1)
    active_focuses = focus_parsed.filter(F.col("is_active") == 1)
    
    # Map focus IDs to names using a CASE expression
    focus_case = F.lit(None).cast("string")
    for fid, fname in focus_id_mapping.items():
        focus_case = F.when(F.col("focus_id") == fid, F.lit(fname)).otherwise(focus_case)
    
    active_focuses = active_focuses.withColumn("focus_name", focus_case)
    
    # Drop unmapped focus IDs (Blood Pressure, Mood, Thoughts, PAP Device)
    active_focuses = active_focuses.filter(F.col("focus_name").isNotNull())
    
    # Aggregate to one row per patient: comma-separated list of active focus names
    focus_agg = (active_focuses
                 .groupBy("patientid")
                 .agg(
                     F.concat_ws(",", F.collect_set("focus_name")).alias("user_focus")
                 ))
    
    # Replace empty strings with None
    focus_agg = focus_agg.withColumn(
        "user_focus",
        F.when(F.col("user_focus") == "", None).otherwise(F.col("user_focus"))
    )
    
    return focus_agg.select("patientid", "user_focus")

print("✓ Focus feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10i. Feature Engineering - Glycemic-Lowering Medications

# COMMAND ----------

def create_glycemic_med_features():
    """
    Identifies patients on glycemic-lowering medications (diabetesclass = TRUE)
    and tracks whether they took the medication on each day.

    Covers drug classes such as metformin, GLP-1 agonists, insulin,
    SGLT2 inhibitors, and sulfonylureas.

    Returns a tuple of two DataFrames:

    glycemic_patient_df  (patient-level, join on patientid):
      - takes_glycemic_lowering_med (bool): True if patient has any active
        glycemic-lowering prescription.

    glycemic_daily_df  (daily, join on [patientid, local_date]):
      - glycemic_med_adherent (bool): True if the patient took at least one
        glycemic-lowering medication on that day (statusid = 1).
    """

    source_catalog = _source_catalog

    # 1. Diabetes-class medication IDs
    medclass_df = (
        spark.read.table(f"{source_catalog}.trxdb_dsmbasedb_observation.medclass")
        .filter(F.col("_fivetran_deleted") == False)
        .filter(F.col("diabetesclass") == True)
        .select("medclassid")
    )

    glycemic_med_ids = (
        spark.read.table(f"{source_catalog}.trxdb_dsmbasedb_observation.medication")
        .filter(F.col("_fivetran_deleted") == False)
        .select("medicationid", "medclassid")
        .join(medclass_df, "medclassid", "inner")
        .select("medicationid")
        .distinct()
    )

    # 2. Active prescriptions for glycemic-lowering medications
    glycemic_rx = (
        spark.read.table(f"{source_catalog}.trxdb_dsmbasedb_observation.medprescription")
        .filter(F.col("_fivetran_deleted") == False)
        .filter(F.col("statusid") == 1)   # Active only
        .select("patientid", "prescriptionguid", "medicationid")
        .join(glycemic_med_ids, "medicationid", "inner")
    )

    # 3. Patient-level flag: has any active glycemic-lowering prescription
    glycemic_patient_df = (
        glycemic_rx
        .select("patientid")
        .distinct()
        .withColumn("takes_glycemic_lowering_med", F.lit(True))
    )

    # 4. Daily flag: took a glycemic-lowering medication on this day
    admin_df = (
        spark.read.table(f"{source_catalog}.trxdb_dsmbasedb_observation.medadministration")
        .filter(F.col("_fivetran_deleted") == False)
        .filter(F.col("statusid") == 1)   # Taken
        .select("patientid", "prescriptionguid", "administrationdate",
                "administrationtimezoneoffset")
        .withColumnRenamed("administrationtimezoneoffset", "timezoneoffset")
    )
    admin_df = add_local_date(admin_df, "administrationdate")

    glycemic_daily_df = (
        admin_df
        .join(glycemic_rx.select("prescriptionguid").distinct(),
              "prescriptionguid", "inner")
        .groupBy("patientid", "local_date")
        .agg(F.lit(True).alias("glycemic_med_adherent"))
    )

    return glycemic_patient_df, glycemic_daily_df

print("✓ Glycemic medication feature function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10h. Column-Level Metadata Comments

# COMMAND ----------

def add_column_comments(table_path: str) -> None:
    """
    Applies column-level COMMENT metadata to every column in the Gold feature table.

    Each comment follows the pattern:
        [schema.table] Human-readable description of the column.

    The schema abbreviation is derived by splitting the fully-qualified source table
    path on '_' and taking the **last** segment (e.g.
    ``trxdb_dsmbasedb_observation`` → ``observation``).

    The comments are stored as Delta table metadata and are visible in:
      - ``DESCRIBE TABLE EXTENDED``
      - Databricks catalog / data explorer UI
      - Databricks Feature Engineering UI
    """

    # (column_name, comment_string)
    # comment format: "[schema_last.table] description"
    COLUMN_COMMENTS = [
        # ── Identity ──────────────────────────────────────────────────────────
        ("patientid",              "[user.patient] Unique patient identifier (primary key component)."),
        ("report_date",            "[pipeline] Calendar date this feature row represents (partition key)."),

        # ── Glucose (elogbgentry) ──────────────────────────────────────────────
        ("glucose_reading_count",  "[observation.elogbgentry] Number of blood-glucose readings logged on this day."),
        ("tir_pct",                "[observation.elogbgentry] Percentage of readings within target range (70–180 mg/dL)."),
        ("glucose_high_pct",       "[observation.elogbgentry] Percentage of readings above 180 mg/dL (hyperglycaemia)."),
        ("glucose_low_pct",        "[observation.elogbgentry] Percentage of readings below 70 mg/dL (hypoglycaemia)."),
        ("glucose_very_low_pct",   "[observation.elogbgentry] Percentage of readings below 54 mg/dL (severe hypoglycaemia)."),
        ("has_cgm_connected",      "[observation.elogbgentry] 1 if any reading came from a CGM device (externalsourceid=18)."),
        ("avg_glucose",            "[observation.elogbgentry] Mean blood-glucose value (mg/dL) across all readings that day."),
        ("tir_pct_delta_1d",       "[observation.elogbgentry] Day-over-day change in time-in-range percentage."),

        # ── Activity (elogexerciseentry) ───────────────────────────────────────
        ("active_minutes",         "[observation.elogexerciseentry] Total exercise duration logged (minutes)."),
        ("exercise_session_count", "[observation.elogexerciseentry] Number of distinct exercise sessions logged."),
        ("exercise_variety_count", "[observation.elogexerciseentry] Number of distinct exercise types logged."),
        ("active_minutes_delta_1d","[observation.elogexerciseentry] Day-over-day change in active minutes."),
        ("active_minutes_7d_sum",  "[observation.elogexerciseentry] Rolling 7-day sum of active minutes (vs 150-min weekly target)."),
        ("active_min_same_or_more","[observation.elogexerciseentry] 1 if active minutes ≥ previous day (within 3-min tolerance)."),

        # ── Steps (stepentry) ─────────────────────────────────────────────────
        ("daily_step_count",       "[observation.stepentry] Total steps recorded on this day."),
        ("daily_step_count_delta_1d","[observation.stepentry] Day-over-day change in step count."),
        ("daily_step_count_avg_7d","[observation.stepentry] Rolling 7-day average daily step count."),
        ("steps_more_than_prev",   "[observation.stepentry] 1 if today's steps exceed the previous day."),
        ("has_step_tracker",       "[observation.stepentry] 1 if patient has any step data (tracker connected)."),
        ("steps_target_met",       "[observation.stepentry] 1 if daily_step_count ≥ 10 000 (daily target)."),

        # ── Weight (elogweightentry + weightgoal) ─────────────────────────────
        ("weight_lbs",             "[observation.elogweightentry] Body weight recorded on this day (lbs)."),
        ("weight_lbs_delta",       "[observation.elogweightentry] Change in weight vs. the previous recorded entry (lbs)."),
        ("weight_change_pct",      "[observation.elogweightentry] Percentage change in weight vs. previous entry."),
        ("days_since_last_weight", "[observation.elogweightentry] Calendar days elapsed since the most recent weight entry."),
        ("weight_logged_today",    "[observation.elogweightentry] 1 if at least one weight reading was logged on this day."),
        ("has_weight_goal",        "[observation.weightgoal] 1 if patient has an active weight goal."),
        ("weight_goal_type",       "[observation.weightgoal] Goal type: 'lose', 'gain', or 'maintain' (from weightgoal.type)."),
        ("is_within_maintenance_range","[observation.weightgoal] 1 if weight is within ±3% of maintenance target weight."),
        ("distance_from_goal",     "[observation.weightgoal] Current weight minus target weight (lbs); positive = above target."),

        # ── Food (foodmoduleitem + patientgoaldetails) ─────────────────────────
        ("total_food_entries",     "[observation.foodmoduleitem] Total individual food items logged on this day."),
        ("unique_meals_logged",    "[observation.foodmoduleitem] Number of distinct meal slots logged (activitytypeid)."),
        ("total_calories",         "[observation.foodmoduleitem] Total calories consumed (kcal)."),
        ("total_protein",          "[observation.foodmoduleitem] Total protein consumed (g)."),
        ("total_carbs",            "[observation.foodmoduleitem] Total carbohydrates consumed (g)."),
        ("total_fat",              "[observation.foodmoduleitem] Total fat consumed (g)."),
        ("total_fiber",            "[observation.foodmoduleitem] Total dietary fibre consumed (g)."),
        ("total_sugar",            "[observation.foodmoduleitem] Total sugar consumed (g)."),
        ("meal_logged_today",      "[observation.foodmoduleitem] 1 if at least one meal was logged on this day."),
        ("days_with_meals_7d",     "[observation.foodmoduleitem] Number of days in the past 7 with at least one meal logged."),
        ("any_nutrient_target_met","[user.patientgoaldetails] 1 if any tracked nutrient was within 90–110% of the daily goal."),
        ("calories_pct_of_goal",   "[user.patientgoaldetails] Today's calorie intake as a percentage of the patient's goal."),
        ("protein_pct_of_goal",    "[user.patientgoaldetails] Today's protein intake as a percentage of the patient's goal."),
        ("carbs_pct_of_goal",      "[user.patientgoaldetails] Today's carbohydrate intake as a percentage of the patient's goal."),
        ("fat_pct_of_goal",        "[user.patientgoaldetails] Today's fat intake as a percentage of the patient's goal."),

        # ── Sleep (sleepentry) ────────────────────────────────────────────────
        ("sleep_duration_hours",   "[observation.sleepentry] Total sleep duration on this day (hours); bucketed by wake-up time (enddatetime)."),
        ("sleep_rating",           "[observation.sleepentry] Average patient-reported sleep quality rating."),
        ("sleep_entry_count",      "[observation.sleepentry] Number of sleep sessions recorded on this day."),
        ("sleep_duration_hours_delta_1d","[observation.sleepentry] Day-over-day change in sleep duration (hours)."),
        ("sleep_rating_delta_1d",  "[observation.sleepentry] Day-over-day change in sleep quality rating."),
        ("sleep_duration_hours_avg_7d","[observation.sleepentry] Rolling 7-day average sleep duration (hours)."),
        ("sleep_rating_avg_7d",    "[observation.sleepentry] Rolling 7-day average sleep quality rating."),
        ("sleep_hours_target_met", "[observation.sleepentry] 1 if sleep_duration_hours ≥ 7 (target)."),
        ("sleep_rating_target_met","[observation.sleepentry] 1 if sleep_rating ≥ 7 (target)."),
        ("slept_more_than_prev",   "[observation.sleepentry] 1 if today's sleep duration exceeds the previous day."),
        ("rating_better_than_prev","[observation.sleepentry] 1 if today's sleep rating exceeds the previous day."),

        # ── Medications (medadministration + medprescription + medprescriptiondayschedule) ─
        ("meds_taken_count",       "[observation.medadministration] Total medication doses taken (statusid=1) on this day."),
        ("active_prescription_count","[observation.medprescription] Number of active prescriptions for the patient."),
        ("expected_daily_doses",   "[observation.medprescription] Total expected doses per day across all active prescriptions."),
        ("med_adherence_pct_1d",   "[observation.medadministration] Per-medication capped adherence % for today (100% = all doses taken)."),
        ("took_all_meds",          "[observation.medadministration] 1 if med_adherence_pct_1d = 100 (all doses taken)."),
        ("med_adherence_7d_avg",   "[observation.medadministration] Rolling 7-day average medication adherence percentage."),
        ("med_reminders_enabled",  "[observation.medprescriptiondayschedule] 1 if the patient has at least one reminder configured."),

        # ── Meditation (elogexerciseentry, exercisetype=30045) ─────────────────
        ("meditation_count",       "[observation.elogexerciseentry] Number of meditation sessions logged today (exercisetype=30045)."),
        ("meditation_opened_today","[observation.elogexerciseentry] 1 if patient opened a meditation session today."),
        ("meditation_opened_7d",   "[observation.elogexerciseentry] 1 if patient opened any meditation session in the past 7 days."),
        ("meditation_opened_30d",  "[observation.elogexerciseentry] 1 if patient opened any meditation session in the past 30 days."),

        # ── Journal (userjournal) ──────────────────────────────────────────────
        ("journal_entry_count",    "[userengagement.userjournal] Number of journal entries written on this day."),
        ("journal_entry_today",    "[userengagement.userjournal] 1 if at least one journal entry was written today."),
        ("journal_entry_7d",       "[userengagement.userjournal] 1 if at least one journal entry was written in the past 7 days."),
        ("journal_entry_30d",      "[userengagement.userjournal] 1 if at least one journal entry was written in the past 30 days."),

        # ── Grocery (grocerydetails) ───────────────────────────────────────────
        ("grocery_entry_count",    "[user.grocerydetails] Number of grocery items added on this day."),
        ("grocery_shopped_today",  "[user.grocerydetails] 1 if any grocery activity was recorded today."),

        # ── Action Plan (actionplanprogress) ──────────────────────────────────
        ("action_plan_entries",    "[user.actionplanprogress] Total action plan records created on this day."),
        ("action_plan_active",     "[user.actionplanprogress] 1 if patient has an active (statusid=1) action plan today."),
        ("action_plan_completed_today","[user.actionplanprogress] 1 if an action plan was completed (statusid=2) today."),
        ("action_plan_progress_7d","[user.actionplanprogress] 1 if any action plan activity occurred in the past 7 days."),
        ("action_plan_progress_30d","[user.actionplanprogress] 1 if any action plan activity occurred in the past 30 days."),

        # ── A1C Target (patienttargetsegment) ─────────────────────────────────
        ("a1c_target_group",       "[observation.patienttargetsegment] Patient's A1C target group: 'dm_target_7' (<7%), 'dm_target_8' (<8%), or 'dm_target_6' (<6%). NULL = not set."),

        # ── Journey (GuidedJourneyWeeksAndTasksDetail) ─────────────────────────
        ("has_active_journey",     "[user.GuidedJourneyWeeksAndTasksDetail] 1 if patient currently has at least one active guided journey (isjourneycompleted=1)."),
        ("has_completed_journey",  "[user.GuidedJourneyWeeksAndTasksDetail] 1 if patient has ever completed a guided journey."),
        ("active_journey_count",   "[user.GuidedJourneyWeeksAndTasksDetail] Number of journeys currently in active status."),
        ("completed_journey_count","[user.GuidedJourneyWeeksAndTasksDetail] Number of journeys the patient has completed."),
        ("journey_task_completed", "[user.GuidedJourneyWeeksAndTasksDetail] 1 if patient has both active and completed items (proxy for task completion)."),

        # ── Exercise Video (curatedvideositemdetail) ───────────────────────────
        ("videos_completed_count", "[user.curatedvideositemdetail] Number of exercise videos marked completed on this day."),
        ("exercise_video_completed_today","[user.curatedvideositemdetail] 1 if at least one exercise video was completed today."),
        ("exercise_video_completed_7d",   "[user.curatedvideositemdetail] 1 if at least one exercise video was completed in the past 7 days."),
        ("has_completed_video_ever",      "[user.curatedvideositemdetail] 1 if patient has ever completed an exercise video."),
        ("total_videos_completed",        "[user.curatedvideositemdetail] Lifetime count of exercise videos completed by the patient."),
        ("has_exercise_video_activity",   "[user.curatedvideositemdetail] 1 if patient has any exercise video records at all."),

        # ── Exercise Program (curatedvideosprogramdetail) ─────────────────────
        ("programs_started_count",        "[user.curatedvideosprogramdetail] Number of exercise programs activated on this day."),
        ("programs_completed_count",      "[user.curatedvideosprogramdetail] Number of exercise programs completed on this day."),
        ("exercise_program_started_today","[user.curatedvideosprogramdetail] 1 if any exercise program was started/activated today."),
        ("exercise_program_completed_today","[user.curatedvideosprogramdetail] 1 if any exercise program was completed today."),
        ("exercise_program_progress_today","[user.curatedvideosprogramdetail] 1 if any program was started or completed today."),
        ("exercise_program_progress_7d",  "[user.curatedvideosprogramdetail] 1 if any program activity occurred in the past 7 days."),
        ("has_active_exercise_program",   "[user.curatedvideosprogramdetail] 1 if patient currently has an active exercise program (statusid=1)."),
        ("has_completed_exercise_program","[user.curatedvideosprogramdetail] 1 if patient has ever completed an exercise program."),
        ("active_program_count",          "[user.curatedvideosprogramdetail] Number of programs currently in active status."),
        ("total_programs",                "[user.curatedvideosprogramdetail] Lifetime count of exercise programs for the patient."),

        # ── Glycemic-Lowering Medications (medclass diabetesclass=TRUE) ───────
        ("takes_glycemic_lowering_med", "[observation.medprescription+medclass] True if the patient has at least one active prescription for a glycemic-lowering medication (diabetesclass = TRUE). Covers metformin, GLP-1 agonists, insulin, SGLT2 inhibitors, sulfonylureas. False if no active glycemic prescription found."),
        ("glycemic_med_adherent",       "[observation.medadministration+medclass] True if the patient took at least one glycemic-lowering medication on this day (statusid = 1). False if prescribed but not taken, or not prescribed."),

        # ── User Focus (customizemyappdetails) ────────────────────────────────
        ("user_focus",             "[user.customizemyappdetails] Comma-separated list of active focus areas chosen by the patient "
                                   "(e.g. 'Weight,Glucose,Activity'). NULL if none selected. "
                                   "Values: Medications | Glucose | Eating Habits | Activity | Sleep | Weight | Anxiety."),

        # ── Eligibility Arrays ─────────────────────────────────────────────────
        ("eligible_positive_actions","[pipeline] Array of positive-action codes this patient earned today "
                                     "(e.g. ['GLUCOSE_TIR_MET','STEPS_TARGET_MET']). Computed from feature flags."),
        ("eligible_opportunities",   "[pipeline] Array of opportunity codes signalling areas for improvement "
                                     "(e.g. ['FOOD_LOG_MEAL','SLEEP_INCREASE_DURATION']). Computed from feature flags."),

        # ── Pipeline Metadata ─────────────────────────────────────────────────
        ("created_at",      "[pipeline] Timestamp when this feature row was written to the Gold table."),
        ("feature_version", "[pipeline] Version identifier for the feature engineering logic applied to this row."),
    ]

    print("\n📝 Applying column-level comments...")
    failed_cols = []
    for col_name, comment in COLUMN_COMMENTS:
        # Escape single quotes inside comments
        safe_comment = comment.replace("'", "\\'")
        try:
            spark.sql(
                f"ALTER TABLE {table_path} ALTER COLUMN `{col_name}` COMMENT '{safe_comment}'"
            )
        except Exception:
            # Column may not exist if a feature table was empty (e.g. no weight goals)
            failed_cols.append(col_name)

    ok_count = len(COLUMN_COMMENTS) - len(failed_cols)
    print(f"  ✓ {ok_count}/{len(COLUMN_COMMENTS)} column comments applied")
    if failed_cols:
        print(f"  ⚠ Skipped (column absent): {failed_cols}")

print("✓ Column-comment function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. Master Feature Assembly - Create Gold Table

# COMMAND ----------

def create_gold_feature_table() -> DataFrame:
    """
    Assembles all feature tables into a single Gold layer table.
    Each row represents one patient on one day with all calculated features.
    """
    
    print("🔧 Generating feature tables...")
    
    # Generate all feature tables
    glucose_features = create_glucose_features()
    print("  ✓ Glucose features")
    
    activity_features = create_activity_features()
    print("  ✓ Activity features")
    
    steps_features = create_steps_features()
    print("  ✓ Steps features")
    
    weight_features = create_weight_features()
    print("  ✓ Weight features")
    
    food_features = create_food_with_goals_features()
    print("  ✓ Food features")
    
    sleep_features = create_sleep_features()
    print("  ✓ Sleep features")
    
    med_features = create_medication_features()
    print("  ✓ Medication features")
    
    meditation_features = create_meditation_features()
    print("  ✓ Meditation features")
    
    journal_features = create_journal_features()
    print("  ✓ Journal features")
    
    grocery_features = create_grocery_features()
    print("  ✓ Grocery features")
    
    action_plan_features = create_action_plan_features()
    print("  ✓ Action plan features")
    
    a1c_features = create_a1c_target_features()
    print("  ✓ A1C target features")
    
    # Journey features (patient-level)
    journey_features = create_journey_features()
    print("  ✓ Journey features")
    
    # Exercise video features (daily)
    exercise_video_features = create_exercise_video_features()
    exercise_video_summary = create_exercise_video_patient_summary()
    print("  ✓ Exercise video features")
    
    # Exercise program features (daily + patient summary)
    exercise_program_daily, exercise_program_summary = create_exercise_program_features()
    print("  ✓ Exercise program features")
    
    # User focus features (patient-level)
    focus_features = create_focus_features()
    print("  ✓ Focus features")

    # Glycemic-lowering medication features (patient-level + daily)
    glycemic_patient_features, glycemic_daily_features = create_glycemic_med_features()
    print("  ✓ Glycemic medication features")

    # Create a base spine of all patient-date combinations
    # This ensures we don't lose days where a patient had no activity
    print("\n🔧 Joining all features...")
    
    # Start with glucose as base (most frequent data)
    gold_df = glucose_features
    
    # Perform full outer joins to preserve all dates
    gold_df = gold_df.join(activity_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(steps_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(weight_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(food_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(sleep_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(med_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(meditation_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(journal_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(grocery_features, ["patientid", "local_date"], "full_outer")
    gold_df = gold_df.join(action_plan_features, ["patientid", "local_date"], "full_outer")
    
    # Exercise video features (daily)
    gold_df = gold_df.join(exercise_video_features, ["patientid", "local_date"], "full_outer")
    
    # Exercise program daily features
    gold_df = gold_df.join(exercise_program_daily, ["patientid", "local_date"], "full_outer")
    
    # Patient-level features (not daily) - left join on patientid only
    # A1C target is patient-level (not daily), so left join on patientid only
    gold_df = gold_df.join(a1c_features, "patientid", "left")
    
    # Journey features (patient-level)
    gold_df = gold_df.join(journey_features, "patientid", "left")
    
    # Exercise video patient summary (patient-level)
    gold_df = gold_df.join(exercise_video_summary, "patientid", "left")
    
    # Exercise program summary (patient-level)
    gold_df = gold_df.join(exercise_program_summary, "patientid", "left")
    
    # User focus areas (patient-level)
    gold_df = gold_df.join(focus_features, "patientid", "left")

    # Glycemic med daily flag (full outer — preserves patient-days with no CGM data)
    gold_df = gold_df.join(glycemic_daily_features, ["patientid", "local_date"], "left")

    # Glycemic med patient-level flag
    gold_df = gold_df.join(glycemic_patient_features, "patientid", "left")

    # Coerce nulls to False for both glycemic flags so downstream logic is clean
    gold_df = gold_df.withColumn(
        "takes_glycemic_lowering_med",
        F.coalesce(F.col("takes_glycemic_lowering_med"), F.lit(False))
    )
    gold_df = gold_df.withColumn(
        "glycemic_med_adherent",
        F.coalesce(F.col("glycemic_med_adherent"), F.lit(False))
    )

    # Rename local_date to report_date for clarity
    gold_df = gold_df.withColumnRenamed("local_date", "report_date")
    
    # Add metadata columns
    gold_df = gold_df.withColumn("created_at", F.current_timestamp())
    gold_df = gold_df.withColumn("feature_version", F.lit("3.0"))  # Updated with Journey, Exercise Video, Exercise Program
    
    print("  ✓ All features joined")
    
    return gold_df

print("✓ Master assembly function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 12. Eligibility Bitmask Creation

# COMMAND ----------

def add_eligibility_flags(df: DataFrame) -> DataFrame:
    """
    Adds eligibility bitmask arrays for positive actions and opportunities.
    These arrays make it easy for the LLM logic engine to select messages.
    """
    
    print("🔧 Creating eligibility flags...")
    
    # Get thresholds from config
    tir_target = CONFIG["thresholds"]["a1c_targets"]["default"]["tir_min"]
    steps_target = CONFIG["thresholds"]["steps_daily_target"]
    sleep_hrs_target = CONFIG["thresholds"]["sleep_hours_target"]
    sleep_rating_target = CONFIG["thresholds"]["sleep_rating_target"]
    
    # ===== POSITIVE ACTIONS =====
    positive_conditions = []
    
    # Glucose positive actions
    positive_conditions.append(
        F.when(F.col("tir_pct") >= tir_target, "GLUCOSE_TIR_MET")
    )
    positive_conditions.append(
        F.when(F.col("tir_pct_delta_1d") > 0, "GLUCOSE_TIR_IMPROVED")
    )
    
    # Activity positive actions
    positive_conditions.append(
        F.when(F.col("active_minutes").isNotNull() & (F.col("active_minutes") > 0), "ACTIVITY_LOGGED")
    )
    positive_conditions.append(
        F.when(F.col("active_min_same_or_more") == 1, "ACTIVITY_MAINTAINED_OR_IMPROVED")
    )
    
    # Steps positive actions
    positive_conditions.append(
        F.when(F.col("steps_target_met") == 1, "STEPS_TARGET_MET")
    )
    positive_conditions.append(
        F.when(F.col("steps_more_than_prev") == 1, "STEPS_IMPROVED")
    )
    
    # Food positive actions
    positive_conditions.append(
        F.when(F.col("meal_logged_today") == 1, "MEAL_LOGGED")
    )
    positive_conditions.append(
        F.when(F.col("any_nutrient_target_met") == 1, "NUTRIENT_TARGET_MET")
    )
    
    # Sleep positive actions
    positive_conditions.append(
        F.when(F.col("sleep_hours_target_met") == 1, "SLEEP_HOURS_MET")
    )
    positive_conditions.append(
        F.when(F.col("sleep_rating_target_met") == 1, "SLEEP_RATING_MET")
    )
    positive_conditions.append(
        F.when(F.col("slept_more_than_prev") == 1, "SLEEP_IMPROVED")
    )
    
    # Medication positive actions
    positive_conditions.append(
        F.when(F.col("took_all_meds") == 1, "MEDS_ALL_TAKEN")
    )
    
    # Weight positive actions
    positive_conditions.append(
        F.when(F.col("weight_logged_today") == 1, "WEIGHT_LOGGED")
    )
    positive_conditions.append(
        F.when(F.col("weight_change_pct") < 0, "WEIGHT_DECREASED")
    )
    
    # Mental wellbeing positive actions
    positive_conditions.append(
        F.when(F.col("meditation_opened_today") == 1, "MEDITATION_OPENED")
    )
    positive_conditions.append(
        F.when(F.col("journal_entry_today") == 1, "JOURNAL_ENTRY")
    )
    positive_conditions.append(
        F.when(F.col("action_plan_active") == 1, "ACTION_PLAN_ACTIVE")
    )
    positive_conditions.append(
        F.when(F.col("action_plan_completed_today") == 1, "ACTION_PLAN_COMPLETED")
    )
    
    # Grocery bonus
    positive_conditions.append(
        F.when(F.col("grocery_shopped_today") == 1, "GROCERY_SHOPPED")
    )
    
    # Journey positive actions
    positive_conditions.append(
        F.when(F.col("has_active_journey") == 1, "JOURNEY_ACTIVE")
    )
    positive_conditions.append(
        F.when(F.col("journey_task_completed") == 1, "JOURNEY_TASK_COMPLETED")
    )
    
    # Exercise video positive actions
    positive_conditions.append(
        F.when(F.col("exercise_video_completed_today") == 1, "EXERCISE_VIDEO_COMPLETED")
    )
    
    # Exercise program positive actions
    positive_conditions.append(
        F.when(F.col("exercise_program_started_today") == 1, "EXERCISE_PROGRAM_STARTED")
    )
    positive_conditions.append(
        F.when(F.col("exercise_program_completed_today") == 1, "EXERCISE_PROGRAM_COMPLETED")
    )
    positive_conditions.append(
        F.when(F.col("exercise_program_progress_today") == 1, "EXERCISE_PROGRAM_PROGRESS")
    )
    
    # Create positive actions array (filter out nulls)
    df = df.withColumn(
        "eligible_positive_actions",
        F.array_remove(F.array(*positive_conditions), None)
    )
    
    # ===== OPPORTUNITIES =====
    opportunity_conditions = []
    
    # Glucose opportunities
    opportunity_conditions.append(
        F.when(F.col("tir_pct") < tir_target, "GLUCOSE_IMPROVE_TIR")
    )
    opportunity_conditions.append(
        F.when(F.col("glucose_high_pct") > CONFIG["thresholds"]["a1c_targets"]["default"]["high_max"], 
               "GLUCOSE_REDUCE_HIGH")
    )
    opportunity_conditions.append(
        F.when(F.col("glucose_low_pct") > CONFIG["thresholds"]["a1c_targets"]["default"]["low_max"],
               "GLUCOSE_PREVENT_LOW")
    )
    
    # Activity opportunities
    opportunity_conditions.append(
        F.when(F.col("active_minutes_7d_sum") < CONFIG["thresholds"]["activity_minutes_weekly_target"],
               "ACTIVITY_INCREASE_WEEKLY")
    )
    
    # Steps opportunities
    opportunity_conditions.append(
        F.when(F.col("daily_step_count_avg_7d") < CONFIG["thresholds"]["steps_weekly_avg_min"],
               "STEPS_INCREASE_DAILY")
    )
    
    # Food opportunities
    opportunity_conditions.append(
        F.when((F.col("meal_logged_today") == 0) | F.col("meal_logged_today").isNull(),
               "FOOD_LOG_MEAL")
    )
    opportunity_conditions.append(
        F.when(F.col("days_with_meals_7d") < 1, "FOOD_LOG_MORE_FREQUENTLY")
    )
    
    # Sleep opportunities
    opportunity_conditions.append(
        F.when(F.col("sleep_duration_hours_avg_7d") < sleep_hrs_target,
               "SLEEP_INCREASE_DURATION")
    )
    opportunity_conditions.append(
        F.when((F.col("sleep_duration_hours_avg_7d") >= sleep_hrs_target) & 
               (F.col("sleep_rating_avg_7d") < sleep_rating_target),
               "SLEEP_IMPROVE_QUALITY")
    )
    
    # Medication opportunities
    opportunity_conditions.append(
        F.when(F.col("med_adherence_7d_avg") < CONFIG["thresholds"]["med_adherence_opportunity_threshold"],
               "MEDS_IMPROVE_ADHERENCE")
    )
    
    # Weight opportunities
    opportunity_conditions.append(
        F.when(F.col("days_since_last_weight") > CONFIG["thresholds"]["weight_log_frequency_days"],
               "WEIGHT_LOG_ENTRY")
    )
    
    # Mental wellbeing opportunities
    opportunity_conditions.append(
        F.when(
            F.col("meditation_opened_30d").isNull() | (F.col("meditation_opened_30d") == 0),
            "MENTAL_TRY_MEDITATION"
        )
    )
    opportunity_conditions.append(
        F.when(
            F.col("journal_entry_30d").isNull() | (F.col("journal_entry_30d") == 0),
            "MENTAL_TRY_JOURNALING"
        )
    )
    opportunity_conditions.append(
        F.when(
            F.col("action_plan_progress_30d").isNull() | (F.col("action_plan_progress_30d") == 0),
            "MENTAL_WORK_ON_ACTION_PLAN"
        )
    )
    
    # Exercise program opportunities
    opportunity_conditions.append(
        F.when(
            (F.col("has_active_exercise_program") == 1) & 
            (F.col("exercise_program_progress_7d").isNull() | (F.col("exercise_program_progress_7d") == 0)),
            "EXERCISE_CONTINUE_PROGRAM"
        )
    )
    opportunity_conditions.append(
        F.when(
            (F.col("has_active_exercise_program").isNull() | (F.col("has_active_exercise_program") == 0)) &
            (F.col("has_exercise_video_activity").isNull() | (F.col("has_exercise_video_activity") == 0)),
            "EXERCISE_START_PROGRAM"
        )
    )
    
    # Exercise video opportunities (if user has video activity but hasn't completed recently)
    opportunity_conditions.append(
        F.when(
            (F.col("has_exercise_video_activity") == 1) & 
            (F.col("exercise_video_completed_7d").isNull() | (F.col("exercise_video_completed_7d") == 0)),
            "EXERCISE_COMPLETE_VIDEO"
        )
    )
    
    # Create opportunities array
    df = df.withColumn(
        "eligible_opportunities",
        F.array_remove(F.array(*opportunity_conditions), None)
    )
    
    print("  ✓ Eligibility flags created")
    
    return df

print("✓ Eligibility flag function created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 13. Execute Feature Store Creation

# COMMAND ----------

def execute_feature_store_creation():
    """
    Main execution function to create and register the feature store.
    """
    
    print("="*80)
    print("🚀 STARTING FEATURE STORE CREATION")
    print("="*80)
    
    # Step 1: Create base gold table
    gold_df = create_gold_feature_table()
    
    # Step 2: Add eligibility flags
    gold_df = add_eligibility_flags(gold_df)
    
    # Step 3: Filter to recent data if incremental
    if CONFIG["processing"]["incremental"]:
        lookback = CONFIG["processing"]["lookback_window_days"]
        cutoff_date = F.date_sub(F.current_date(), lookback)
        gold_df = gold_df.filter(F.col("report_date") >= cutoff_date)
        print(f"\n📅 Filtered to last {lookback} days")
    
    # Step 4: Get row count
    row_count = gold_df.count()
    print(f"\n📊 Total rows in feature table: {row_count:,}")
    
    # Step 5: Write to Delta table
    gold_table_path = get_gold_table_path()
    print(f"\n💾 Writing to: {gold_table_path}")
    
    # Ensure schema exists
    catalog = CONFIG["gold_table"]["catalog"]
    schema = CONFIG["gold_table"]["schema"]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Write with merge logic for idempotency
    (gold_df.write
     .format("delta")
     .mode("overwrite")  # Change to "append" or add merge logic for production
     .option("overwriteSchema", "true")
     .partitionBy(CONFIG["processing"]["partition_by"])
     .saveAsTable(gold_table_path))
    
    print("  ✓ Feature table written successfully")
    
    # Step 6: Optimize table
    print("\n⚡ Optimizing table for serving performance...")
    spark.sql(f"OPTIMIZE {gold_table_path} ZORDER BY (patientid)")
    print("  ✓ Table optimized")
    
    # Step 7: Add column-level comments (lineage + description)
    add_column_comments(gold_table_path)

    # Step 8: Register with Feature Store (if desired)
    try:
        from databricks.feature_engineering import FeatureEngineeringClient
        fe = FeatureEngineeringClient()
        
        # Check if table already exists as feature table
        # If not, you can create it
        print("\n📝 Feature table ready for Feature Engineering registration")
        print("   Use FeatureEngineeringClient.create_table() if needed")
        
    except ImportError:
        print("\n⚠ Databricks Feature Engineering client not available")
        print("   Table created as standard Delta table")
    
    print("\n" + "="*80)
    print("✅ FEATURE STORE CREATION COMPLETE")
    print("="*80)
    print(f"\nTable: {gold_table_path}")
    print(f"Rows: {row_count:,}")
    print("\nNext steps:")
    print("1. Review feature table schema and sample data")
    print("2. Create feature lookup for model serving")
    print("3. Build LLM selection logic using eligibility flags")
    print("4. Deploy as Databricks Model Serving endpoint")
    
    return gold_df

# COMMAND ----------
# MAGIC %md
# MAGIC ## 14. Run the Pipeline

# COMMAND ----------

# Execute the feature store creation
gold_feature_table = execute_feature_store_creation()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 15. Data Quality Checks

# COMMAND ----------

def run_data_quality_checks(df: DataFrame):
    """
    Performs data quality checks on the generated feature table.
    """
    
    print("="*80)
    print("🔍 RUNNING DATA QUALITY CHECKS")
    print("="*80)
    
    # Check 1: No duplicate patient-date combinations
    duplicate_count = (df.groupBy("patientid", "report_date")
                       .count()
                       .filter(F.col("count") > 1)
                       .count())
    
    if duplicate_count == 0:
        print("✓ No duplicate (patientid, report_date) keys found")
    else:
        print(f"⚠ WARNING: Found {duplicate_count} duplicate keys!")
    
    # Check 2: TIR values within valid range
    invalid_tir = df.filter(
        F.col("tir_pct").isNotNull() & 
        ((F.col("tir_pct") < 0) | (F.col("tir_pct") > 100))
    ).count()
    
    if invalid_tir == 0:
        print("✓ All TIR values within valid range (0-100%)")
    else:
        print(f"⚠ WARNING: Found {invalid_tir} rows with invalid TIR values!")
    
    # Check 3: Date range coverage
    date_stats = df.select(
        F.min("report_date").alias("earliest_date"),
        F.max("report_date").alias("latest_date"),
        F.countDistinct("report_date").alias("unique_dates"),
        F.countDistinct("patientid").alias("unique_patients")
    ).collect()[0]
    
    print(f"\n📅 Date Coverage:")
    print(f"   Earliest: {date_stats['earliest_date']}")
    print(f"   Latest: {date_stats['latest_date']}")
    print(f"   Unique dates: {date_stats['unique_dates']}")
    print(f"   Unique patients: {date_stats['unique_patients']}")
    
    # Check 4: Null rates for key features
    print(f"\n📊 Feature Completeness:")
    key_features = [
        "tir_pct", "daily_step_count", "active_minutes",
        "sleep_duration_hours", "meal_logged_today", "med_adherence_pct_1d"
    ]
    
    for feature in key_features:
        if feature in df.columns:
            null_pct = (df.filter(F.col(feature).isNull()).count() / df.count()) * 100
            print(f"   {feature}: {100-null_pct:.1f}% complete")
    
    # Check 5: Sample eligibility flags
    print(f"\n🏷️ Eligibility Flags Sample:")
    sample_flags = (df.filter(
        (F.size("eligible_positive_actions") > 0) | 
        (F.size("eligible_opportunities") > 0)
    ).select("patientid", "report_date", "eligible_positive_actions", "eligible_opportunities")
    .limit(3))
    
    sample_flags.show(truncate=False)
    
    print("\n" + "="*80)
    print("✅ DATA QUALITY CHECKS COMPLETE")
    print("="*80)

# Run quality checks
run_data_quality_checks(gold_feature_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 16. Sample Queries for Validation

# COMMAND ----------

# Query 1: Show sample of features for one patient
sample_patient = gold_feature_table.select("patientid").first()[0]

print(f"Sample features for patient: {sample_patient}")
(gold_feature_table
 .filter(F.col("patientid") == sample_patient)
 .orderBy(F.col("report_date").desc())
 .limit(7)
 .select(
     "report_date",
     "tir_pct",
     "daily_step_count",
     "active_minutes",
     "sleep_duration_hours",
     "eligible_positive_actions",
     "eligible_opportunities"
 )
 .show(truncate=False))

# COMMAND ----------

# Query 2: Count of positive actions by type across all patients
from pyspark.sql.functions import explode

print("Distribution of positive actions:")
(gold_feature_table
 .select(explode("eligible_positive_actions").alias("action"))
 .groupBy("action")
 .count()
 .orderBy(F.col("count").desc())
 .show(truncate=False))

# COMMAND ----------

# Query 3: Patients with high engagement (multiple positive actions)
print("Patients with 3+ positive actions yesterday:")
yesterday = F.lit(_feature_date)

(gold_feature_table
 .filter(F.col("report_date") == yesterday)
 .filter(F.size("eligible_positive_actions") >= 3)
 .select(
     "patientid",
     "eligible_positive_actions",
     "tir_pct",
     "daily_step_count",
     "sleep_rating"
 )
 .limit(10)
 .show(truncate=False))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration Summary
# MAGIC 
# MAGIC **Configurable Parameters** (update in Section 1):
# MAGIC - All source table names in `CONFIG["source_tables"]`
# MAGIC - Gold table location: `CONFIG["gold_table"]`
# MAGIC - Status ID values: `CONFIG["status_values"]`
# MAGIC - All thresholds (TIR targets, step goals, etc.): `CONFIG["thresholds"]`
# MAGIC - Processing mode (incremental/full refresh): `CONFIG["processing"]`
# MAGIC - Journey config (when available): `CONFIG["journey_config"]`
# MAGIC 
# MAGIC **Next Steps**:
# MAGIC 1. Validate A1C patient profile table and confirm enum values
# MAGIC 2. Validate A1C patient profile table and add to config
# MAGIC 3. Run this notebook to create feature store
# MAGIC 4. Build LLM selection logic using the eligibility flags
# MAGIC 5. Create MLflow model wrapper for serving
# MAGIC 6. Deploy as Databricks Model Serving endpoint