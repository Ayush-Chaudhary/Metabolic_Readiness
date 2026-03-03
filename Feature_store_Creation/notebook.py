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

from typing import Dict, Any
from datetime import datetime

# ===== TABLE CONFIGURATION =====
CONFIG: Dict[str, Any] = {
    
    # Source Tables (Bronze/Silver)
    "source_tables": {
        "glucose": "bronz_als_azdev24.trxdb_dsmbasedb_observation.elogbgentry",
        "activity": "bronz_als_azdev24.trxdb_dsmbasedb_observation.elogexerciseentry",
        "steps": "bronz_als_azdev24.trxdb_dsmbasedb_observation.stepentry",
        "weight": "bronz_als_azdev24.trxdb_dsmbasedb_observation.elogweightentry",
        "weight_goals": "bronz_als_azuat2.trxdb_dsmbasedb_observation.weightgoal",
        "food": "bronz_als_azdev24.trxdb_dsmbasedb_observation.foodmoduleitem",
        "sleep": "bronz_als_azdev24.trxdb_dsmbasedb_observation.sleepentry",
        "med_administration": "bronz_als_azdev24.trxdb_dsmbasedb_observation.medadministration",
        "med_prescription": "bronz_als_azdev24.trxdb_dsmbasedb_observation.medprescription",
        "patient_nutrition_goals": "bronz_als_azuat2.trxdb_dsmbasedb_user.patientgoaldetails",
        "a1c_target": "bronz_als_azdev24.trxdb_dsmbasedb_observation.patienttargetsegment",
        "journal": "bronz_als_azdev24.trxdb_dsmbasedb_userengagement.userjournal",
        "grocery": "bronz_als_azdev24.trxdb_dsmbasedb_user.grocerydetails",
        "action_plan": "bronz_als_azdev24.trxdb_dsmbasedb_user.actionplanprogress",
        # Note: meditation is derived from the activity table (exercisetype=30045)
        # Journey and exercise tracking tables
        "journey": "bronz_als_azdev24.trxdb_dsmbasedb_user.GuidedJourneyWeeksAndTasksDetail",
        "exercise_video": "bronz_als_azdev24.trxdb_dsmbasedb_user.curatedvideositemdetail",
        "exercise_program": "bronz_als_azdev24.trxdb_dsmbasedb_user.curatedvideosprogramdetail",
    },
    
    # Target Gold Layer
    "gold_table": {
        "catalog": "bronz_als_azuat2",  # UPDATE as needed
        "schema": "llm",
        "table_name": "user_daily_health_habits",
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
        "journey_table": "bronz_als_azdev24.trxdb_dsmbasedb_user.GuidedJourneyWeeksAndTasksDetail",
    },
    
    # Processing Configuration
    "processing": {
        "incremental": True,  # Set to False for full refresh
        "lookback_window_days": 90,  # How far back to process
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
    ).withColumn(
        "a1c_target_group",
        F.coalesce(F.col("a1c_target_group"), F.lit("dm_target_7"))
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
                    "timezoneoffset"
                ))
    
    # Apply timezone (use startdatetime for date bucketing)
    steps_df = add_local_date(steps_df, "startdatetime")
    
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
    
    # STILL NEEDED: Join with weight_goals table to calculate goal-specific metrics
    # (is_within_maintenance_range, distance_from_goal, weight_goal_type, etc.)
    
    final_weight = weight_daily.select(
        "patientid",
        "local_date",
        "weight_lbs",
        "weight_lbs_delta",
        "weight_change_pct",
        "days_since_last_weight",
        "weight_logged_today"
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

def create_sleep_features() -> DataFrame:
    """
    Calculates sleep duration, ratings, and rolling averages.
    """
    
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
    
    # Apply timezone and filter
    sleep_df = add_local_date(sleep_df, "startdatetime")
    sleep_df = filter_active_records(sleep_df, "observationstatus")
    
    # Calculate sleep duration in hours
    sleep_df = sleep_df.withColumn(
        "sleep_duration_hours",
        (F.unix_timestamp("enddatetime") - F.unix_timestamp("startdatetime")) / 3600
    )
    
    # Group by patient and date (in case multiple sleep sessions per day)
    sleep_daily = sleep_df.groupBy("patientid", "local_date").agg(
        F.sum("sleep_duration_hours").alias("sleep_duration_hours"),
        F.avg("sleeprating").alias("sleep_rating"),
        F.count("*").alias("sleep_entry_count")
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
    """
    
    # Read medication administration
    admin_df = (spark.read.table(get_table_path("med_administration"))
                .select(
                    "patientid",
                    "statusid",
                    "administrationdate",
                    "dose",
                    "administrationtimezoneoffset"
                )
                .withColumnRenamed("administrationtimezoneoffset", "timezoneoffset"))
    
    # Apply timezone and filter to taken medications
    admin_df = add_local_date(admin_df, "administrationdate")
    admin_df = admin_df.filter(F.col("statusid").isin(CONFIG["status_values"]["active_status"]))
    
    # Count medications taken per day
    admin_daily = admin_df.groupBy("patientid", "local_date").agg(
        F.count("*").alias("meds_taken_count")
    )
    
    # Read prescriptions to get scheduled count
    # Note: This is simplified - in reality, you'd need to calculate
    # expected doses based on frequencytype and frequencyvalue
    try:
        prescription_df = (spark.read.table(get_table_path("med_prescription"))
                          .select(
                              "patientid",
                              "frequencytype",
                              "frequencyvalue",
                              "startdate",
                              "statusid"
                          ))
        
        # Filter to active prescriptions
        prescription_df = prescription_df.filter(
            F.col("statusid").isin(CONFIG["status_values"]["active_status"])
        )
        
        # Count active prescriptions per patient
        # STILL NEEDED: More sophisticated logic to calculate daily expected doses
        prescription_count = prescription_df.groupBy("patientid").agg(
            F.count("*").alias("active_prescription_count")
        )
        
        # Join with administration data
        med_daily = admin_daily.join(prescription_count, "patientid", "left")
        
        # Calculate adherence percentage
        med_daily = med_daily.withColumn(
            "med_adherence_pct_1d",
            F.when(
                F.col("active_prescription_count").isNotNull() & (F.col("active_prescription_count") > 0),
                (F.col("meds_taken_count") / F.col("active_prescription_count")) * 100
            ).otherwise(None)
        )
        
        # Cap at 100% (in case they log extra doses)
        med_daily = med_daily.withColumn(
            "med_adherence_pct_1d",
            F.when(F.col("med_adherence_pct_1d") > 100, 100).otherwise(F.col("med_adherence_pct_1d"))
        )
        
        # Flag: took all meds (100% adherence)
        med_daily = med_daily.withColumn(
            "took_all_meds",
            F.when(F.col("med_adherence_pct_1d") == 100, 1).otherwise(0)
        )
        
        # Calculate 7-day rolling adherence
        window = Window.partitionBy("patientid").orderBy("local_date").rowsBetween(-6, 0)
        med_daily = med_daily.withColumn(
            "med_adherence_7d_avg",
            F.avg("med_adherence_pct_1d").over(window)
        )
        
    except Exception as e:
        print(f"⚠ Warning: Could not calculate adherence with prescriptions: {e}")
        med_daily = admin_daily.withColumn("med_adherence_pct_1d", F.lit(None))
        med_daily = med_daily.withColumn("took_all_meds", F.lit(0))
    
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
                  .select("patientid", "createddatetime"))
    
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
    entrydatetimeinmills is epoch milliseconds.
    """
    
    grocery_df = (spark.read.table(get_table_path("grocery"))
                  .select("patientid", "entrydatetimeinmills"))
    
    grocery_df = grocery_df.withColumn(
        "local_date",
        F.to_date(F.from_unixtime(F.col("entrydatetimeinmills") / 1000))
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
                      .select("patientid", "actionplanstatus", "createddate", "timezoneoffset"))
    
    action_plan_df = add_local_date(action_plan_df, "createddate")
    
    # Filter out deleted plans
    action_plan_df = action_plan_df.filter(F.col("actionplanstatus") != 3)
    
    action_plan_daily = action_plan_df.groupBy("patientid", "local_date").agg(
        F.count("*").alias("action_plan_entries"),
        F.max(F.when(F.col("actionplanstatus") == 1, 1).otherwise(0)).alias("action_plan_active"),
        F.max(F.when(F.col("actionplanstatus") == 2, 1).otherwise(0)).alias("action_plan_completed_today"),
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
                          "createddatetime", "modifieddatetime"))
    
    # Create local dates for different events
    program_df = program_df.withColumn(
        "activation_date",
        F.to_date("activateddatetime")
    ).withColumn(
        "created_date",
        F.to_date("createddatetime")
    ).withColumn(
        "modified_date",
        F.to_date("modifieddatetime")
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
    
    # Step 7: Register with Feature Store (if desired)
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
yesterday = F.date_sub(F.current_date(), 1)

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
# MAGIC 1. Update table names in CONFIG section (especially weight_goals table)
# MAGIC 2. Validate A1C patient profile table and add to config
# MAGIC 3. Run this notebook to create feature store
# MAGIC 4. Build LLM selection logic using the eligibility flags
# MAGIC 5. Create MLflow model wrapper for serving
# MAGIC 6. Deploy as Databricks Model Serving endpoint