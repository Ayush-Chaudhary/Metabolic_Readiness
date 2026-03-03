# Databricks notebook source
# MAGIC %md
# MAGIC # SIMON Health Habits - Message Generation Pipeline
# MAGIC 
# MAGIC This notebook orchestrates the end-to-end message generation process:
# MAGIC 1. Reads user features from Gold table
# MAGIC 2. Applies business logic to select content
# MAGIC 3. Calls LLM to generate personalized messages
# MAGIC 4. Logs messages for history tracking
# MAGIC 
# MAGIC **Usage:**
# MAGIC - Batch mode: Generate messages for all active users
# MAGIC - Single user mode: Generate message for specific patient_id
# MAGIC - API mode: Deploy as Model Serving endpoint

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

%load_ext autoreload
%autoreload 2

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json

# ===== PIPELINE CONFIGURATION =====
PIPELINE_CONFIG: Dict[str, Any] = {
    
    # Gold Table (Feature Store)
    "gold_table": {
        "catalog": "bronz_als_azuat2",
        "schema": "llm",
        "table_name": "user_daily_health_habits",
    },
    
    # Message History Table (for frequency capping)
    "history_table": {
        "catalog": "bronz_als_azuat2",
        "schema": "llm",
        "table_name": "metabolic_readiness_message_history",
    },
    
    # User Profile Table (for names and preferences)
    "user_profile_table": {
        "catalog": "bronz_als_azdev24",
        "schema": "trxdb_dsmbasedb_user",
        "table_name": "patient",  # UPDATE with actual table
    },
    
    # LLM Configuration
    "llm": {
        "endpoint_name": "llama-3-3-70b",
        "fallback_endpoint": "databricks-meta-llama-3-1-70b-instruct",
    },
    
    # Processing Configuration
    "processing": {
        "batch_size": 100,  # Users per batch
        "max_retries": 3,
        "log_messages": True,  # Whether to log generated messages
    },
    
    # Config file path (for prompts)
    "prompts_config_path": "/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/files/prompts.yml",
}

def get_full_table_name(table_config: dict) -> str:
    """Build full table name from config."""
    return f"{table_config['catalog']}.{table_config['schema']}.{table_config['table_name']}"

print("✓ Configuration loaded")
print(f"Gold table: {get_full_table_name(PIPELINE_CONFIG['gold_table'])}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Import Dependencies

# COMMAND ----------

# Standard imports
import sys
import os
from pathlib import Path

# Add the main directory to path for imports
# This allows importing logic_engine and insight_generator
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import custom modules
from logic_engine import (
    LogicEngine, 
    UserContext, 
    MessageHistory, 
    SelectedContent,
    load_user_context_from_gold,
    load_message_history,
    A1CTargetGroup
)
from insight_generator import InsightGenerator

print("✓ Custom modules imported")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Initialize Components

# COMMAND ----------

# Initialize the logic engine and insight generator
# Use local path if running locally, or workspace path if on Databricks

config_path = PIPELINE_CONFIG.get('prompts_config_path', 'prompts.yml')

# Try local path first, then workspace path
if not os.path.exists(config_path):
    local_config = os.path.join(current_dir, 'prompts.yml')
    if os.path.exists(local_config):
        config_path = local_config

try:
    logic_engine = LogicEngine(config_path)
    insight_generator = InsightGenerator(config_path)
    print(f"✓ Logic engine and insight generator initialized with config: {config_path}")
except FileNotFoundError as e:
    print(f"⚠ Config file not found: {config_path}")
    print("  Using default configuration")
    logic_engine = None
    insight_generator = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Helper Functions

# COMMAND ----------

def get_user_features(spark, patient_id: str, report_date: datetime = None) -> Optional[Dict[str, Any]]:
    """
    Fetch user features from Gold table.
    
    Args:
        spark: SparkSession
        patient_id: The patient ID to lookup
        report_date: Date to get features for (default: yesterday)
        
    Returns:
        Dictionary of user features or None if not found
    """
    if report_date is None:
        report_date = datetime.now() - timedelta(days=1)
    
    gold_table = get_full_table_name(PIPELINE_CONFIG['gold_table'])
    report_date_str = report_date.strftime('%Y-%m-%d')
    
    query = f"""
    SELECT * FROM {gold_table}
    WHERE patientid = '{patient_id}'
    AND report_date = '{report_date_str}'
    LIMIT 1
    """
    
    try:
        result = spark.sql(query).collect()
        if result:
            return result[0].asDict()
        return None
    except Exception as e:
        print(f"Error fetching features for {patient_id}: {e}")
        return None


def get_user_profile(spark, patient_id: str) -> Dict[str, Any]:
    """
    Fetch user profile information (name, preferences).
    
    Args:
        spark: SparkSession
        patient_id: The patient ID
        
    Returns:
        Dictionary with user profile data
    """
    # Default profile
    profile = {
        'patient_id': patient_id,
        'user_focus': None,
        'a1c_target_group': 'dm_target_7'
    }
    
    return profile


def build_user_context(features: Dict[str, Any], profile: Dict[str, Any]) -> UserContext:
    """
    Build UserContext object from features and profile data.
    
    Args:
        features: Dictionary from Gold table
        profile: Dictionary from user profile
        
    Returns:
        UserContext object
    """
    # Map A1C target group
    a1c_mapping = {
        'dm_target_7': A1CTargetGroup.DM_TARGET_7,
        'dm_target_8': A1CTargetGroup.DM_TARGET_8,
        'dip': A1CTargetGroup.DIP,
        'non_dm': A1CTargetGroup.NON_DM,
    }
    
    a1c_group = a1c_mapping.get(
        profile.get('a1c_target_group', 'dm_target_7'),
        A1CTargetGroup.DM_TARGET_7
    )
    
    return UserContext(
        patient_id=profile['patient_id'],
        report_date=datetime.now() - timedelta(days=1),
        
        # Profile flags
        has_cgm=bool(features.get('has_cgm_connected', False)),
        has_step_tracker=bool(features.get('has_step_tracker', False)),
        has_medications=bool((features.get('active_prescription_count') or 0) > 0),
        has_weight_goal=bool(features.get('has_weight_goal', False)),
        weight_goal_type=features.get('weight_goal_type'),
        has_active_journey=bool(features.get('has_active_journey', False)),
        has_exercise_program=bool(features.get('has_exercise_program', False)),
        user_focus=profile.get('user_focus'),
        a1c_target_group=a1c_group,
        med_reminders_enabled=bool(features.get('med_reminders_enabled', False)),
        
        # Glucose features
        tir_pct=features.get('tir_pct'),
        tir_prev_day=features.get('tir_pct_delta_1d'),
        glucose_high_pct=features.get('glucose_high_pct'),
        glucose_low_pct=features.get('glucose_low_pct'),
        
        # Steps features
        daily_step_count=features.get('daily_step_count'),
        prev_day_steps=features.get('daily_step_count_delta_1d'),
        
        # Activity features
        active_minutes=features.get('active_minutes'),
        prev_day_active_minutes=features.get('active_minutes_delta_1d'),
        weekly_active_minutes=features.get('active_minutes_7d_sum'),
        exercise_video_completion_pct=features.get('exercise_video_completion_pct'),
        
        # Sleep features
        sleep_duration_hours=features.get('sleep_duration_hours'),
        prev_day_sleep_hours=features.get('sleep_duration_hours_delta_1d'),
        sleep_rating=features.get('sleep_rating'),
        prev_day_sleep_rating=features.get('sleep_rating_delta_1d'),
        avg_sleep_hours_7d=features.get('sleep_duration_hours_avg_7d'),
        avg_sleep_rating_7d=features.get('sleep_rating_avg_7d'),
        
        # Weight features
        weight_logged_yesterday=bool(features.get('weight_logged_today', False)),
        weight_change_pct=features.get('weight_change_pct'),
        days_since_last_weight=features.get('days_since_last_weight'),
        is_within_maintenance_range=bool(features.get('is_within_maintenance_range', False)),
        
        # Food features
        meals_logged_count=features.get('unique_meals_logged'),
        last_meal_type=features.get('last_meal_type'),
        any_nutrient_target_met=bool(features.get('any_nutrient_target_met', False)),
        nutrient_name_met=features.get('nutrient_name_met'),
        days_with_meals_7d=features.get('days_with_meals_7d'),
        has_nutrient_goals=bool(features.get('has_nutrient_goals', False)),
        
        # Medication features
        took_all_meds=bool(features.get('took_all_meds', False)),
        med_adherence_7d_avg=features.get('med_adherence_7d_avg'),
        
        # Mental well-being features
        meditation_opened_30d=bool(features.get('meditation_opened_30d', False)),
        journal_entry_30d=bool(features.get('journal_entry_30d', False)),
        action_plan_progress_30d=bool(features.get('action_plan_progress_30d', False)),
        
        # Journey features
        journey_task_completed=bool(features.get('journey_task_completed', False)),
        app_login_yesterday=bool(features.get('app_login_yesterday', True)),
        
        # Pre-calculated eligibility (from Gold table)
        eligible_positive_actions=features.get('eligible_positive_actions', []),
        eligible_opportunities=features.get('eligible_opportunities', [])
    )


def get_message_history(spark, patient_id: str) -> MessageHistory:
    """
    Fetch message history for frequency capping.
    
    Args:
        spark: SparkSession
        patient_id: The patient ID
        
    Returns:
        MessageHistory object
    """
    history = MessageHistory(patient_id=patient_id)
    
    try:
        history_table = get_full_table_name(PIPELINE_CONFIG['history_table'])
        lookback_date = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            category,
            message_date
        FROM {history_table}
        WHERE patientid = '{patient_id}'
        AND message_date >= '{lookback_date}'
        """
        
        rows = spark.sql(query).collect()
        
        categories = list(set(row['category'] for row in rows))
        history.categories_shown_last_6d = categories
        
        # Count weight messages this week
        week_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        weight_count = sum(1 for row in rows if row['category'] == 'weight')
        history.weight_messages_this_week = weight_count
        
        # Check yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        history.weight_shown_yesterday = any(
            row['category'] == 'weight' and str(row['message_date']) == yesterday 
            for row in rows
        )
        
    except Exception as e:
        print(f"Warning: Could not load message history: {e}")
    
    return history


def log_generated_message(
    spark, 
    patient_id: str, 
    result: Dict[str, Any]
) -> None:
    """
    Log the generated message for history tracking.
    
    Args:
        spark: SparkSession
        patient_id: The patient ID
        result: The generation result dictionary
    """
    if not PIPELINE_CONFIG['processing']['log_messages']:
        return
    
    try:
        history_table = get_full_table_name(PIPELINE_CONFIG['history_table'])
        
        # Get categories used
        categories_used = set()
        for action in result.get('positive_actions_used', []):
            # Extract category from action key
            if action.startswith('glucose_'):
                categories_used.add('glucose')
            elif action.startswith('steps_'):
                categories_used.add('steps')
            elif action.startswith('activity_'):
                categories_used.add('activity')
            elif action.startswith('sleep_'):
                categories_used.add('sleep')
            elif action.startswith('weight_'):
                categories_used.add('weight')
            elif action.startswith('meal_') or action.startswith('nutrient_'):
                categories_used.add('food')
            elif action.startswith('medication_'):
                categories_used.add('medications')
        
        # Add opportunity category
        opp_key = result.get('opportunity_used', '')
        if opp_key.startswith('glucose_'):
            categories_used.add('glucose')
        # ... add other mappings as needed
        
        # Insert records for each category
        for category in categories_used:
            # Escape single quotes in message for SQL
            escaped_message = result['message'].replace("'", "''")
            insert_sql = f"""
            INSERT INTO {history_table} (patientid, category, message_date, message_text, created_at)
            VALUES ('{patient_id}', '{category}', current_date(), '{escaped_message}', current_timestamp())
            """
            spark.sql(insert_sql)
            
    except Exception as e:
        print(f"Warning: Could not log message history: {e}")

print("✓ Helper functions defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Main Generation Function

# COMMAND ----------

def generate_message_for_user(
    spark,
    patient_id: str,
    logic_engine: LogicEngine,
    insight_generator: InsightGenerator,
    report_date: datetime = None
) -> Dict[str, Any]:
    """
    Generate a personalized health message for a single user.
    
    This is the main orchestration function that:
    1. Fetches user features from Gold table
    2. Gets user profile and message history
    3. Runs logic engine to select content
    4. Calls LLM to generate message
    5. Logs the result
    
    Args:
        spark: SparkSession
        patient_id: The patient ID
        logic_engine: Initialized LogicEngine
        insight_generator: Initialized InsightGenerator
        report_date: Date to generate message for (default: yesterday)
        
    Returns:
        Dictionary with generated message and metadata
    """
    result = {
        'patient_id': patient_id,
        'success': False,
        'message': None,
        'rating': None,
        'error': None
    }
    
    try:
        # Step 1: Fetch features from Gold table
        print("🔍 Step 1: Fetching user features...")
        features = get_user_features(spark, patient_id, report_date)
        
        if features is None:
            result['error'] = "No features found for user"
            return result
        print(f"✓ Features loaded: {len(features)} fields")
        
        # Step 2: Get user profile
        print("🔍 Step 2: Getting user profile...")
        profile = get_user_profile(spark, patient_id)
        print(f"✓ Profile loaded")
        
        # Step 3: Build user context
        print("🔍 Step 3: Building user context...")
        user_context = build_user_context(features, profile)
        print(f"✓ User context built")
        
        # Step 4: Get message history
        print("🔍 Step 4: Getting message history...")
        history = get_message_history(spark, patient_id)
        print(f"✓ Message history loaded")
        
        # Step 5: Run logic engine to select content
        print("🔍 Step 5: Running logic engine...")
        try:
            selected_content = logic_engine.select_content(user_context, history)
            print(f"✓ Content selected: {selected_content.daily_rating}")
        except Exception as e:
            import traceback
            print(f"❌ ERROR IN LOGIC ENGINE:")
            print(traceback.format_exc())
            raise
        
        # Step 6: Generate message with LLM
        print("🔍 Step 6: Generating message with LLM...")
        generation_result = insight_generator.generate_insight(
            daily_rating=selected_content.daily_rating,
            rating_description=selected_content.rating_description,
            positive_actions=selected_content.positive_actions,
            opportunity=selected_content.opportunity,
            greeting=selected_content.greeting
        )
        print(f"✓ Message generated")
        
        # Step 7: Build response
        result['success'] = generation_result['success']
        result['message'] = generation_result['message']
        result['rating'] = selected_content.daily_rating
        result['rating_description'] = selected_content.rating_description
        result['character_count'] = generation_result['character_count']
        result['positive_actions_used'] = generation_result.get('positive_actions_used', [])
        result['opportunity_used'] = generation_result.get('opportunity_used', '')
        
        # Step 8: Log the message
        if result['success']:
            log_generated_message(spark, patient_id, generation_result)
        
    except Exception as e:
        import traceback
        result['error'] = str(e)
        print(f"\n❌ Error generating message for {patient_id}: {e}")
        print(f"\n📋 Full traceback:")
        print(traceback.format_exc())
    
    return result


def generate_messages_batch(
    spark,
    patient_ids: List[str],
    logic_engine: LogicEngine,
    insight_generator: InsightGenerator
) -> List[Dict[str, Any]]:
    """
    Generate messages for a batch of users.
    
    Args:
        spark: SparkSession
        patient_ids: List of patient IDs
        logic_engine: Initialized LogicEngine
        insight_generator: Initialized InsightGenerator
        
    Returns:
        List of result dictionaries
    """
    results = []
    
    for i, patient_id in enumerate(patient_ids):
        if (i + 1) % 10 == 0:
            print(f"Processing user {i + 1}/{len(patient_ids)}...")
        
        result = generate_message_for_user(
            spark=spark,
            patient_id=patient_id,
            logic_engine=logic_engine,
            insight_generator=insight_generator
        )
        results.append(result)
    
    # Summary
    successful = sum(1 for r in results if r['success'])
    print(f"\n✓ Batch complete: {successful}/{len(patient_ids)} successful")
    
    return results

print("✓ Main generation functions defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. API Serving Functions

# COMMAND ----------

def create_mlflow_model():
    """
    Create and log an MLflow model for serving.
    
    This creates a pyfunc model that can be deployed to
    Databricks Model Serving.
    """
    import mlflow
    from mlflow.pyfunc import PythonModel
    
    class SimonHealthMessageModel(PythonModel):
        """MLflow PythonModel wrapper for message generation."""
        
        def load_context(self, context):
            """Load model artifacts."""
            import sys
            import os
            
            # Add artifacts path
            artifacts_path = context.artifacts['code_path']
            if artifacts_path not in sys.path:
                sys.path.insert(0, artifacts_path)
            
            from logic_engine import LogicEngine
            from insight_generator import InsightGenerator
            
            config_path = os.path.join(artifacts_path, 'prompts.yml')
            
            self.logic_engine = LogicEngine(config_path)
            self.insight_generator = InsightGenerator(config_path)
        
        def predict(self, context, model_input):
            """Generate messages for input users."""
            from logic_engine import UserContext, MessageHistory
            from datetime import datetime
            
            # Handle DataFrame or dict input
            if hasattr(model_input, 'to_dict'):
                inputs = model_input.to_dict(orient='records')
            elif isinstance(model_input, list):
                inputs = model_input
            else:
                inputs = [model_input]
            
            results = []
            
            for row in inputs:
                # Build context from input
                user_context = UserContext(
                    patient_id=row.get('patient_id', ''),
                    report_date=datetime.now(),
                    has_cgm=row.get('has_cgm_connected', False),
                    has_step_tracker=row.get('has_step_tracker', False),
                    has_medications=row.get('has_medications', False),
                    tir_pct=row.get('tir_pct'),
                    daily_step_count=row.get('daily_step_count'),
                    active_minutes=row.get('active_minutes'),
                    sleep_duration_hours=row.get('sleep_duration_hours'),
                    sleep_rating=row.get('sleep_rating'),
                    meals_logged_count=row.get('unique_meals_logged'),
                    took_all_meds=row.get('took_all_meds', False),
                )
                
                history = MessageHistory(patient_id=row.get('patient_id', ''))
                
                # Generate
                selected = self.logic_engine.select_content(user_context, history)
                
                result = self.insight_generator.generate_insight(
                    daily_rating=selected.daily_rating,
                    rating_description=selected.rating_description,
                    positive_actions=selected.positive_actions,
                    opportunity=selected.opportunity,
                    greeting=selected.greeting
                )
                
                results.append({
                    'patient_id': row.get('patient_id'),
                    'message': result['message'],
                    'rating': selected.daily_rating,
                    'success': result['success']
                })
            
            return results
    
    # Log the model
    with mlflow.start_run(run_name="simon_health_messages"):
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=SimonHealthMessageModel(),
            artifacts={
                'code_path': current_dir
            },
            pip_requirements=[
                'pyyaml',
                'databricks-sdk'
            ]
        )
        
        print("✓ MLflow model logged")
        return mlflow.active_run().info.run_id

print("✓ API serving functions defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Example Usage

# COMMAND ----------

# Example: Generate message for a single user
def example_single_user():
    """Example of generating a message for one user."""
    
    # For testing without Spark/actual data, use mock context
    from logic_engine import UserContext, MessageHistory
    
    # Create mock user context
    user = UserContext(
        patient_id="TEST001",
        report_date=datetime.now() - timedelta(days=1),
        has_cgm=True,
        has_step_tracker=True,
        has_medications=True,
        tir_pct=72.5,
        tir_prev_day=68.0,
        daily_step_count=7890,
        prev_day_steps=6500,
        sleep_duration_hours=7.2,
        sleep_rating=7,
        took_all_meds=True,
        meals_logged_count=2
    )
    
    history = MessageHistory(patient_id="TEST001")
    
    # Run logic engine
    selected = logic_engine.select_content(user, history)
    
    print("\n" + "="*60)
    print("LOGIC ENGINE OUTPUT")
    print("="*60)
    print(f"Daily Rating: {selected.daily_rating}")
    print(f"Greeting: {selected.greeting}")
    print(f"Positive Actions: {[a['text'] for a in selected.positive_actions]}")
    print(f"Opportunity: {selected.opportunity['text']}")
    print("="*60)
    
    # Generate message
    result = insight_generator.generate_insight(
        daily_rating=selected.daily_rating,
        rating_description=selected.rating_description,
        positive_actions=selected.positive_actions,
        opportunity=selected.opportunity,
        greeting=selected.greeting
    )
    
    print("\n" + "="*60)
    print("GENERATED MESSAGE")
    print("="*60)
    print(f"Rating: {result['rating']}")
    print(f"Message ({result['character_count']} chars):\n{result['message']}")
    print(f"Success: {result['success']}")
    print("="*60)
    
    return result

# Run example if logic_engine is available
if logic_engine is not None:
    example_result = example_single_user()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7b. Test with Real Gold Table Data

# COMMAND ----------

def test_with_gold_data(spark, patient_id: str = None, report_date: str = None):
    """
    Test the pipeline with a real user from the Gold table.
    
    Args:
        spark: SparkSession
        patient_id: Optional specific patient ID. If None, picks the first available user.
        report_date: Optional date string (YYYY-MM-DD). If None, uses yesterday.
        
    Returns:
        Generated message result dictionary
    """
    gold_table = get_full_table_name(PIPELINE_CONFIG['gold_table'])
    
    # Set default date to yesterday if not provided
    if report_date is None:
        report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # If no patient_id provided, get a sample user from the table
    if patient_id is None:
        query = f"""
        SELECT patientid
        FROM {gold_table}
        WHERE report_date = '{report_date}'
        LIMIT 1
        """
        result = spark.sql(query).collect()
        
        if len(result) == 0:
            print(f"❌ No users found in Gold table for date {report_date}")
            return None
        
        patient_id = result[0]['patientid']
        print(f"📋 Selected patient_id: {patient_id}")
    
    print(f"🔍 Testing pipeline for patient {patient_id} on {report_date}")
    print("="*70)
    
    # Generate message using the main pipeline function
    result = generate_message_for_user(
        spark=spark,
        patient_id=patient_id,
        logic_engine=logic_engine,
        insight_generator=insight_generator,
        report_date=datetime.strptime(report_date, '%Y-%m-%d')
    )
    
    # Display results
    print("\n" + "="*70)
    print("📨 PIPELINE TEST RESULTS")
    print("="*70)
    
    if result['success']:
        print(f"✅ Success: {result['success']}")
        print(f"👤 Patient ID: {result['patient_id']}")
        print(f"⭐ Rating: {result['rating']}")
        print(f"📝 Rating Description: {result['rating_description']}")
        print(f"\n💬 Generated Message ({result['character_count']} chars, {len(result['message'].split())} words):")
        print("-"*70)
        print(result['message'])
        print("-"*70)
        print(f"\n✨ Positive Actions Used: {', '.join(result.get('positive_actions_used', []))}")
        print(f"🎯 Opportunity Used: {result.get('opportunity_used', 'N/A')}")
    else:
        print(f"❌ Failed to generate message")
        print(f"Error: {result.get('error', 'Unknown error')}")
    
    print("="*70)
    
    return result

# Run test with gold data
# Uncomment one of the following to test:

# Option 1: Test with first available user from yesterday
# test_result = test_with_gold_data(spark)

# Option 2: Test with specific patient_id
# test_result = test_with_gold_data(spark, patient_id="YOUR_PATIENT_ID_HERE")

# Option 3: Test with specific patient and date
test_with_gold_data(spark, patient_id=17014, report_date="2025-12-20")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Batch Processing (Production)

# COMMAND ----------

def run_batch_generation(spark, date_str: str = None):
    """
    Run batch message generation for all active users.
    
    Args:
        spark: SparkSession
        date_str: Date string (YYYY-MM-DD) to generate for, default yesterday
    """
    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    gold_table = get_full_table_name(PIPELINE_CONFIG['gold_table'])
    
    # Get all users with features for this date
    query = f"""
    SELECT DISTINCT patientid
    FROM {gold_table}
    WHERE report_date = '{date_str}'
    """
    
    patient_ids = [row['patientid'] for row in spark.sql(query).collect()]
    
    print(f"Found {len(patient_ids)} users with features for {date_str}")
    
    if len(patient_ids) == 0:
        print("No users to process")
        return []
    
    # Process in batches
    batch_size = PIPELINE_CONFIG['processing']['batch_size']
    all_results = []
    
    for i in range(0, len(patient_ids), batch_size):
        batch = patient_ids[i:i + batch_size]
        print(f"\nProcessing batch {i // batch_size + 1} ({len(batch)} users)...")
        
        results = generate_messages_batch(
            spark=spark,
            patient_ids=batch,
            logic_engine=logic_engine,
            insight_generator=insight_generator
        )
        all_results.extend(results)
    
    # Save results to Delta table
    results_table = f"{PIPELINE_CONFIG['gold_table']['catalog']}.{PIPELINE_CONFIG['gold_table']['schema']}.generated_messages"
    
    # Define explicit schema for Spark DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType
    
    schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("message", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("rating_description", StringType(), True),
        StructField("character_count", IntegerType(), True),
        StructField("positive_actions_used", ArrayType(StringType()), True),
        StructField("opportunity_used", StringType(), True),
        StructField("error", StringType(), True),
    ])
    
    # Create DataFrame with explicit schema
    results_df = spark.createDataFrame(all_results, schema=schema)
    results_df.write.format("delta").mode("append").saveAsTable(results_table)
    
    print(f"\n✓ Results saved to {results_table}")
    
    return all_results

# Example batch run (commented out for safety)
# results = run_batch_generation(spark)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Create Message History Table

# COMMAND ----------

def create_history_table(spark):
    """Create the message history table if it doesn't exist."""
    
    history_table = get_full_table_name(PIPELINE_CONFIG['history_table'])
    catalog = PIPELINE_CONFIG['history_table']['catalog']
    schema = PIPELINE_CONFIG['history_table']['schema']
    
    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Create table
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {history_table} (
        patientid STRING,
        category STRING,
        message_date DATE,
        message_text STRING,
        rating STRING,
        positive_actions_used ARRAY<STRING>,
        opportunity_used STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (message_date)
    """
    
    spark.sql(create_sql)
    print(f"✓ Message history table created: {history_table}")

# Uncomment to create table
# create_history_table(spark)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook provides:
# MAGIC 
# MAGIC 1. **Single User Generation**: `generate_message_for_user()` - Generate message for one patient
# MAGIC 2. **Batch Generation**: `run_batch_generation()` - Generate messages for all users
# MAGIC 3. **MLflow Model**: `create_mlflow_model()` - Create deployable model for API serving
# MAGIC 4. **Testing Functions**:
# MAGIC    - `example_single_user()` - Test with mock/hardcoded data
# MAGIC    - `test_with_gold_data()` - Test with real user from Gold table
# MAGIC 
# MAGIC **Files created:**
# MAGIC - `prompts.yml` - Editable prompt templates and clinical thresholds
# MAGIC - `logic_engine.py` - Business logic for content selection
# MAGIC - `insight_generator.py` - LLM integration for message generation
# MAGIC - `main_pipeline.py` - This orchestration notebook
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Update `PIPELINE_CONFIG` with actual table names
# MAGIC 2. Run `create_history_table()` to set up logging
# MAGIC 3. Test with `test_with_gold_data(spark)` using real Gold table data
# MAGIC 4. Deploy with `create_mlflow_model()` for API serving
