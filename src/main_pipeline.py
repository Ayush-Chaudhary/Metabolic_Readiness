# Databricks notebook source
# MAGIC %md
# MAGIC # SIMON Health Habits - Message Generation Pipeline
# MAGIC 
# MAGIC This notebook provides a clean, production-ready message generation system:
# MAGIC 1. **MessageGenerationModel** class with `predict()` method
# MAGIC 2. Loads patient data from Gold table
# MAGIC 3. Generates personalized insights using LLM
# MAGIC 4. Stores results to history table with upsert logic
# MAGIC 5. Ready for MLflow deployment

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2

# %pip install -U --quiet pyyaml databricks-sql-connector databricks-sdk mlflow==3.1.4
# dbutils.library.restartPython()

# COMMAND ----------
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
import os
from mlflow.models.resources import (
  DatabricksServingEndpoint,
  DatabricksTable,
)

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
        "table_name": "patient",
    },
    
    # Config file path (for prompts)
    "prompts_config_path": "/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/files/prompts.yml",
}

def get_full_table_name(table_config: dict) -> str:
    """Build full table name from config."""
    return f"{table_config['catalog']}.{table_config['schema']}.{table_config['table_name']}"

print("✓ Configuration loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Import Dependencies

# COMMAND ----------

import sys

# Add the main directory to path
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from logic_engine import (
    LogicEngine, 
    UserContext, 
    MessageHistory, 
    SelectedContent,
    A1CTargetGroup
)
from insight_generator import InsightGenerator
from pipeline_utils import (
    get_message_history as _pu_get_message_history,
    write_patient_history as _pu_write_patient_history,
    _extract_categories_from_actions,
)

print("✓ Dependencies imported")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def get_user_features(spark, patient_id: str, report_date: datetime) -> Optional[Dict[str, Any]]:
    """Fetch user features from Gold table."""
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
        return result[0].asDict() if result else None
    except Exception as e:
        print(f"Error fetching features: {e}")
        return None


def get_user_profile(spark, patient_id: str) -> Dict[str, Any]:
    """Fetch user profile information."""
    return {'patient_id': patient_id, 'user_focus': None, 'a1c_target_group': None}


def build_user_context(features: Dict[str, Any], profile: Dict[str, Any]) -> UserContext:
    """Build UserContext object from features and profile data."""
    a1c_mapping = {
        'dm_target_7': A1CTargetGroup.DM_TARGET_7,
        'dm_target_8': A1CTargetGroup.DM_TARGET_8,
        'dip': A1CTargetGroup.DIP,
        'non_dm': A1CTargetGroup.NON_DM,
    }
    
    a1c_group = a1c_mapping.get(
        features.get('a1c_target_group'),  # Read from Gold table features
        None  # stays None — effective_a1c_group in logic_engine will fall back to DM_TARGET_7
    )

    # Derive weight logged flags from days_since_last_weight
    days_since = features.get('days_since_last_weight')

    # Derive nutrient scoring fields from individual *_target_pct columns
    nutrient_pcts = {}
    first_met_name = None
    for name in ['protein', 'carbs', 'fat', 'calories']:
        pct = features.get(f'{name}_target_pct')
        has_goal = features.get(f'goal_{name}') is not None and (features.get(f'goal_{name}') or 0) > 0
        if has_goal:
            nutrient_pcts[name] = pct
            if first_met_name is None and pct is not None and 90 <= pct <= 110:
                first_met_name = name
    total_nutrient = len(nutrient_pcts)
    
    return UserContext(
        patient_id=profile['patient_id'],
        report_date=datetime.now() - timedelta(days=1),
        
        # Device/goal flags
        has_cgm=bool(features.get('has_cgm_connected', False)),
        has_step_tracker=bool(features.get('has_step_tracker', False)),
        has_medications=bool((features.get('active_prescription_count') or 0) > 0),
        has_weight_goal=bool(features.get('has_weight_goal', False)),
        weight_goal_type=features.get('weight_goal_type'),
        has_active_journey=bool(features.get('has_active_journey', False)),
        has_exercise_program=bool(features.get('has_active_exercise_program', False)),
        user_focus=features.get('user_focus', '').split(',') if features.get('user_focus') else None,
        a1c_target_group=a1c_group,
        med_reminders_enabled=bool(features.get('med_reminders_enabled', False)),
        
        # Health metrics
        tir_pct=features.get('tir_pct'),
        tir_prev_day=features.get('tir_pct_delta_1d'),
        glucose_high_pct=features.get('glucose_high_pct'),
        glucose_low_pct=features.get('glucose_low_pct'),
        daily_step_count=features.get('daily_step_count'),
        prev_day_steps=features.get('daily_step_count_delta_1d'),
        active_minutes=features.get('active_minutes'),
        prev_day_active_minutes=features.get('active_minutes_delta_1d'),
        weekly_active_minutes=features.get('active_minutes_7d_sum'),
        exercise_video_completion_pct=features.get('exercise_video_completion_pct'),
        sleep_duration_hours=features.get('sleep_duration_hours'),
        prev_day_sleep_hours=features.get('sleep_duration_hours_delta_1d'),
        sleep_rating=features.get('sleep_rating'),
        prev_day_sleep_rating=features.get('sleep_rating_delta_1d'),
        avg_sleep_hours_7d=features.get('sleep_duration_hours_avg_7d'),
        avg_sleep_rating_7d=features.get('sleep_rating_avg_7d'),
        weight_logged_yesterday=bool(features.get('weight_logged_today', False)),
        weight_change_pct=features.get('weight_change_pct'),
        weight_change_lbs_14d=features.get('weight_change_lbs_14d'),
        weight_change_pct_14d=features.get('weight_change_pct_14d'),
        days_since_last_weight=days_since,
        weight_last_logged_7d=days_since is not None and days_since <= 7,
        weight_last_logged_14d=days_since is not None and days_since <= 14,
        weight_last_logged_30d=days_since is not None and days_since <= 30,
        is_within_maintenance_range=bool(features.get('is_within_maintenance_range', False)),
        meals_logged_count=features.get('unique_meals_logged'),
        last_meal_type=features.get('last_meal_type'),
        any_nutrient_target_met=bool(features.get('any_nutrient_target_met', False)),
        has_nutrient_goals=total_nutrient > 0,
        total_nutrient_targets=total_nutrient,
        num_nutrient_targets_90_110=sum(1 for p in nutrient_pcts.values() if p is not None and 90 <= p <= 110),
        num_nutrient_targets_60_plus=sum(1 for p in nutrient_pcts.values() if p is not None and p >= 60),
        num_nutrient_targets_30_plus=sum(1 for p in nutrient_pcts.values() if p is not None and p >= 30),
        nutrient_name_met=first_met_name,
        days_with_meals_7d=features.get('days_with_meals_7d'),
        took_all_meds=bool(features.get('took_all_meds', False)),
        med_adherence_7d_avg=features.get('med_adherence_7d_avg'),
        takes_glycemic_lowering_med=bool(features.get('takes_glycemic_lowering_med', False)),
        glycemic_med_adherent=bool(features.get('glycemic_med_adherent', False)),
        meditation_opened_30d=bool(features.get('meditation_opened_30d', False)),
        journal_entry_30d=bool(features.get('journal_entry_30d', False)),
        action_plan_progress_30d=bool(features.get('action_plan_progress_30d', False)),
        
        # Mental wellbeing 7-day fields
        action_plan_active=bool(features.get('action_plan_active', False)),
        journal_entry_7d=bool(features.get('journal_entry_7d', False)),
        meditation_opened_7d=bool(features.get('meditation_opened_7d', False)),
        
        # Journey tracking fields
        journey_task_completed=bool(features.get('journey_task_completed', False)),
        has_completed_journey=bool(features.get('has_completed_journey', False)),
        active_journey_count=features.get('active_journey_count', 0) or 0,
        completed_journey_count=features.get('completed_journey_count', 0) or 0,
        
        # Exercise Video tracking fields
        exercise_video_completed_today=bool(features.get('exercise_video_completed_today', False)),
        exercise_video_completed_7d=bool(features.get('exercise_video_completed_7d', False)),
        has_exercise_video_activity=bool(features.get('has_exercise_video_activity', False)),
        has_completed_video_ever=bool(features.get('has_completed_video_ever', False)),
        total_videos_completed=features.get('total_videos_completed', 0) or 0,
        
        # Exercise Program tracking fields
        has_active_exercise_program=bool(features.get('has_active_exercise_program', False)),
        has_completed_exercise_program=bool(features.get('has_completed_exercise_program', False)),
        exercise_program_started_today=bool(features.get('exercise_program_started_today', False)),
        exercise_program_completed_today=bool(features.get('exercise_program_completed_today', False)),
        exercise_program_progress_today=bool(features.get('exercise_program_progress_today', False)),
        exercise_program_progress_7d=bool(features.get('exercise_program_progress_7d', False)),
        active_program_count=features.get('active_program_count', 0) or 0,
        
        # Bonus activity tracking
        bonus_exercise_video_completed=bool(features.get('exercise_video_completed_today', False)),
        bonus_exercise_program_started=bool(features.get('exercise_program_started_today', False)),
        bonus_grocery_online=bool(features.get('grocery_shopped_today', False)),
        
        app_login_yesterday=bool(features.get('app_login_yesterday', True)),
        eligible_positive_actions=features.get('eligible_positive_actions', []),
        eligible_opportunities=features.get('eligible_opportunities', [])
    )


def get_message_history(spark, patient_id: str) -> MessageHistory:
    """Fetch message history for frequency capping. Delegates to pipeline_utils."""
    return _pu_get_message_history(
        spark, patient_id, get_full_table_name(PIPELINE_CONFIG['history_table'])
    )


# Re-exported from pipeline_utils so insight_generation_job can access it via _mod.
extract_categories_from_actions = _extract_categories_from_actions


def upsert_message_history(spark, patient_id: str, message_date: str, result: Dict[str, Any]) -> None:
    """Upsert message to history table. Delegates to pipeline_utils.write_patient_history."""
    _pu_write_patient_history(
        spark, patient_id, message_date, result,
        get_full_table_name(PIPELINE_CONFIG['history_table'])
    )


print("✓ Helper functions defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Message Generation Model Class

# COMMAND ----------

class MessageGenerationModel:
    """
    Production-ready model for generating personalized health messages.
    
    This class encapsulates the entire pipeline:
    - Loads patient data from Gold table
    - Applies business logic via LogicEngine
    - Generates message via LLM
    - Stores results to history table
    """
    
    def __init__(self, spark, config_path: str = None):
        """
        Initialize the model.
        
        Args:
            spark: SparkSession instance
            config_path: Path to prompts.yml config file
        """
        self.spark = spark
        
        # Use configured path or default
        if config_path is None:
            config_path = PIPELINE_CONFIG.get('prompts_config_path', 'prompts.yml')
        
        # Try local path if workspace path doesn't exist
        if not os.path.exists(config_path):
            local_config = os.path.join(current_dir, 'prompts.yml')
            if os.path.exists(local_config):
                config_path = local_config
        
        # Initialize components
        self.logic_engine = LogicEngine(config_path)
        self.insight_generator = InsightGenerator(config_path)
        self.config_path = config_path
        
        print(f"✓ MessageGenerationModel initialized with config: {config_path}")
    
    def predict(
        self, 
        patient_id: str, 
        date: Optional[str] = None,
        verbose: bool = False
    ) -> Dict[str, Any]:
        """
        Generate personalized health message for a patient.
        
        Args:
            patient_id: Patient ID to generate message for
            date: Date string (YYYY-MM-DD). Defaults to today if not provided.
            verbose: If True, prints detailed progress logs
            
        Returns:
            Dictionary containing:
                - success (bool): Whether generation succeeded
                - patient_id (str): Patient ID
                - message (str): Generated message text
                - rating (str): Daily rating (e.g., "Ready", "Committed")
                - rating_description (str): Rating description
                - character_count (int): Message length in characters
                - word_count (int): Message length in words
                - positive_actions_used (list): Action keys used
                - opportunity_used (str): Opportunity key used
                - error (str): Error message if failed
        """
        result = {
            'patient_id': patient_id,
            'success': False,
            'message': None,
            'rating': None,
            'rating_description': None,
            'error': None
        }
        
        try:
            # Parse date (default to today)
            if date is None:
                report_date = datetime.now()
            else:
                report_date = datetime.strptime(date, '%Y-%m-%d')
            
            if verbose:
                print(f"🔍 Generating message for patient {patient_id} on {report_date.strftime('%Y-%m-%d')}")
            
            # Step 1: Fetch features from Gold table
            if verbose:
                print("  → Fetching user features from Gold table...")
            features = get_user_features(self.spark, patient_id, report_date)
            
            if features is None:
                result['error'] = f"No features found for patient {patient_id} on {report_date.strftime('%Y-%m-%d')}"
                if verbose:
                    print(f"  ❌ {result['error']}")
                return result
            
            if verbose:
                print(f"  ✓ Features loaded: {len(features)} fields")
            
            # Step 2: Get user profile
            profile = get_user_profile(self.spark, patient_id)
            
            # Step 3: Build user context
            if verbose:
                print("  → Building user context...")
            user_context = build_user_context(features, profile)
            
            # Step 4: Get message history
            if verbose:
                print("  → Loading message history...")
            history = get_message_history(self.spark, patient_id)
            
            # Step 5: Run logic engine to select content
            if verbose:
                print("  → Running logic engine...")
            selected_content = self.logic_engine.select_content(user_context, history)
            
            # Step 6: Generate message with LLM
            if verbose:
                print("  → Generating message with LLM...")
            generation_result = self.insight_generator.generate_insight(
                daily_rating=selected_content.daily_rating,
                rating_description=selected_content.rating_description,
                positive_actions=selected_content.positive_actions,
                opportunity=selected_content.opportunity,
                greeting=selected_content.greeting
            )
            
            # Step 7: Build result
            result['success'] = generation_result['success']
            result['message'] = generation_result['message']
            result['rating'] = selected_content.daily_rating
            result['rating_description'] = selected_content.rating_description
            result['character_count'] = generation_result.get('character_count', len(generation_result['message']))
            result['word_count'] = len(generation_result['message'].split())
            result['positive_actions_used'] = generation_result.get('positive_actions_used', [])
            result['opportunity_used'] = generation_result.get('opportunity_used', '')
            
            # Step 8: Store to history table (upsert)
            if result['success']:
                if verbose:
                    print("  → Storing to history table...")
                upsert_message_history(
                    self.spark, 
                    patient_id, 
                    report_date.strftime('%Y-%m-%d'),
                    result
                )
            
            if verbose:
                print(f"  ✓ Message generated successfully ({result['word_count']} words)")
            
        except Exception as e:
            import traceback
            result['error'] = str(e)
            if verbose:
                print(f"  ❌ Error: {e}")
                print(f"\nFull traceback:\n{traceback.format_exc()}")
        
        return result


print("✓ MessageGenerationModel class defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Initialize Model

# COMMAND ----------

# Initialize the model
model = MessageGenerationModel(spark)

print("✓ Model ready for predictions")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Example Usage

# COMMAND ----------

# Example: Generate message for a specific patient
# result = model.predict(patient_id="17014", date="2025-12-20", verbose=True)
# print(f"\n{'='*70}")
# print("Generated Message:")
# print(f"{'='*70}")
# print(result['message'])
# print(f"{'='*70}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Log Model to MLflow

# COMMAND ----------
import mlflow
from mlflow.pyfunc import PythonModel
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksTable

class MLflowWrapper(PythonModel):
    """MLflow wrapper for MessageGenerationModel."""
    
    def load_context(self, context):
        """Load model artifacts."""
        import sys
        
        # Add code paths to sys.path
        code_path = context.artifacts.get('code_path')
        if code_path and code_path not in sys.path:
            sys.path.insert(0, code_path)
        
        from logic_engine import LogicEngine, UserContext, MessageHistory, A1CTargetGroup
        from insight_generator import InsightGenerator
        
        # Load config from artifacts
        config_path = os.path.join(code_path, 'prompts.yml')
        self.logic_engine = LogicEngine(config_path)
        self.insight_generator = InsightGenerator(config_path)
        
        # Store config for table names
        self.config = {
            'gold_table': 'bronz_als_azuat2.llm.user_daily_health_habits',
            'history_table': 'bronz_als_azuat2.llm.metabolic_readiness_message_history'
        }
    
    def _fetch_features(self, connection, patient_id: str, report_date: datetime) -> Optional[Dict[str, Any]]:
        """Fetch user features from Gold table via SQL connector."""
        query = f"""
        SELECT * FROM {self.config['gold_table']}
        WHERE patientid = '{patient_id}'
        AND report_date = '{report_date.strftime('%Y-%m-%d')}'
        LIMIT 1
        """
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
    
    def _build_user_context(self, features: Dict[str, Any], patient_id: str, report_date: datetime) -> 'UserContext':
        """Build UserContext from features (adapted from helper function)."""
        from logic_engine import UserContext, A1CTargetGroup
        
        a1c_mapping = {
            'dm_target_7': A1CTargetGroup.DM_TARGET_7,
            'dm_target_8': A1CTargetGroup.DM_TARGET_8,
            'dip': A1CTargetGroup.DIP,
            'non_dm': A1CTargetGroup.NON_DM,
        }
        
        return UserContext(
            patient_id=patient_id,
            report_date=report_date,
            has_cgm=bool(features.get('has_cgm_connected', False)),
            has_step_tracker=bool(features.get('has_step_tracker', False)),
            has_medications=bool((features.get('active_prescription_count') or 0) > 0),
            has_weight_goal=bool(features.get('has_weight_goal', False)),
            weight_goal_type=features.get('weight_goal_type'),
            user_focus=features.get('user_focus', '').split(',') if features.get('user_focus') else None,
            a1c_target_group=a1c_mapping.get(features.get('a1c_target_group'), None),
            tir_pct=features.get('tir_pct'),
            daily_step_count=features.get('daily_step_count'),
            weekly_active_minutes=features.get('active_minutes_7d_sum'),
            sleep_duration_hours=features.get('sleep_duration_hours'),
            sleep_rating=features.get('sleep_rating'),
            meals_logged_count=features.get('unique_meals_logged'),
            took_all_meds=features.get('took_all_meds', False),
            takes_glycemic_lowering_med=bool(features.get('takes_glycemic_lowering_med', False)),
            glycemic_med_adherent=bool(features.get('glycemic_med_adherent', False)),
        )
    
    def predict(self, context, model_input):
        """
        Generate messages for input patients.
        
        Expected input format:
            {"patient_id": "12345", "date": "2025-12-20"}
        or
            [{"patient_id": "12345", "date": "2025-12-20"}, ...]
        """
        from logic_engine import MessageHistory
        import traceback
        from databricks.sdk.core import Config, oauth_service_principal
        
        # Get M2M authentication credentials from environment
        server_hostname = os.environ.get('DATABRICKS_SERVER_HOSTNAME') or os.environ.get('DATABRICKS_HOST')
        http_path = os.environ.get('DATABRICKS_HTTP_PATH')
        client_id = os.environ.get('DATABRICKS_CLIENT_ID')
        client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')
        
        # Handle DataFrame or dict input
        if hasattr(model_input, 'to_dict'):
            inputs = model_input.to_dict(orient='records')
        elif isinstance(model_input, list):
            inputs = model_input
        else:
            inputs = [model_input]
        
        results = []
        
        # Setup OAuth M2M credentials provider
        def credential_provider():
            config = Config(
                host=server_hostname,
                client_id=client_id,
                client_secret=client_secret
            )
            return oauth_service_principal(config)
        
        # Connect to Databricks SQL Warehouse with M2M authentication
        # (lazy import: only available in MLflow Model Serving, not on Databricks clusters)
        from databricks import sql  # noqa: PLC0415
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            credentials_provider=credential_provider
        ) as connection:
            
            for inp in inputs:
                patient_id = inp.get('patient_id')
                date_str = inp.get('date')
                
                result = {
                    'patient_id': patient_id,
                    'success': False,
                    'message': None,
                    'rating': None,
                    'error': None
                }
                
                try:
                    # Parse date
                    report_date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.now()
                    
                    # Fetch features using helper method
                    features = self._fetch_features(connection, patient_id, report_date)
                    if not features:
                        result['error'] = f"No data found for patient {patient_id}"
                        results.append(result)
                        continue
                    
                    # Build user context using helper method
                    user_context = self._build_user_context(features, patient_id, report_date)
                    
                    # Get message history (empty for now in serving)
                    history = MessageHistory(patient_id=patient_id)
                    
                    # Generate content
                    selected_content = self.logic_engine.select_content(user_context, history)
                    
                    generation_result = self.insight_generator.generate_insight(
                        daily_rating=selected_content.daily_rating,
                        rating_description=selected_content.rating_description,
                        positive_actions=selected_content.positive_actions,
                        opportunity=selected_content.opportunity,
                        greeting=selected_content.greeting
                    )
                    
                    result['success'] = True
                    result['message'] = generation_result['message']
                    result['rating'] = selected_content.daily_rating
                    result['character_count'] = len(generation_result['message'])
                    result['word_count'] = len(generation_result['message'].split())
                    
                except Exception as e:
                    result['error'] = f"{str(e)}\n{traceback.format_exc()}"
                
                results.append(result)
        
        return results

# Uncomment to log the model to MLflow:
# with mlflow.start_run(run_name='metabolic_readiness_v1') as run:
#     mlflow.pyfunc.log_model(
#         name="metabolic_readiness_model",
#         python_model=MLflowWrapper(),
#         registered_model_name="bronz_als_azuat2.llm.metabolic_readiness_model_v1",
#         artifacts={'code_path': current_dir},
#         code_paths=[
#             os.path.join(current_dir, 'insight_generator.py'),
#             os.path.join(current_dir, 'logic_engine.py'),
#             os.path.join(current_dir, 'prompts.yml')
#         ],
#         pip_requirements=[
#             'pyyaml',
#             'databricks-sql-connector',
#             'databricks-sdk',
#             'mlflow==3.1.4',
#             'cloudpickle==3.0.0' 
#         ],
#         signature=mlflow.models.infer_signature(
#             model_input={"patient_id": "12345", "date": "2025-12-20"},
#             model_output={"success": True, "message": "Sample message", "rating": "Ready"}
#         ),
#         input_example={"patient_id": "12345", "date": "2025-12-20"},
#         resources=[
#             DatabricksServingEndpoint(endpoint_name="llama-3-3-70b"),
#             DatabricksTable(table_name=get_full_table_name(PIPELINE_CONFIG['gold_table'])),
#             DatabricksTable(table_name=get_full_table_name(PIPELINE_CONFIG['history_table']))
#         ]
#     )
#     
#     print(f"✓ Model logged to MLflow")
#     print(f"  Run ID: {run.info.run_id}")
#     print(f"  Artifact URI: {run.info.artifact_uri}")


# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Create Message History Table

# COMMAND ----------

def create_history_table(spark):
    """
    Create the message history table if it doesn't exist.
    
    Schema includes:
    - patientid: Patient identifier
    - category: Message category (glucose, sleep, weight, etc.)
    - message_date: Date the message was generated for
    - message_text: Full generated message
    - rating: Daily rating (Ready, Committed, etc.)
    - rating_description: Rating description text
    - positive_actions_used: Array of action keys used
    - opportunity_used: Opportunity key used
    - character_count: Message length in characters
    - word_count: Message length in words
    - created_at: Timestamp when record was created
    """
    history_table = get_full_table_name(PIPELINE_CONFIG['history_table'])
    catalog = PIPELINE_CONFIG['history_table']['catalog']
    schema = PIPELINE_CONFIG['history_table']['schema']
    
    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Create table with complete schema
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {history_table} (
        patientid STRING,
        category STRING,
        message_date DATE,
        message_text STRING,
        rating STRING,
        rating_description STRING,
        positive_actions_used ARRAY<STRING>,
        opportunity_used STRING,
        character_count INT,
        word_count INT,
        created_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (message_date)
    COMMENT 'Message history for Metabolic Readiness feature with frequency capping'
    """
    
    spark.sql(create_sql)
    print(f"✓ Message history table created: {history_table}")
    print(f"  Partitioned by: message_date")
    print(f"  Schema includes: patientid, category, message_date, message_text, rating, etc.")

# Uncomment to create table:
# create_history_table(spark)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook provides a clean, production-ready pipeline:
# MAGIC 
# MAGIC ### **Core Component:**
# MAGIC - `MessageGenerationModel` class with `predict(patient_id, date)` method
# MAGIC - Automatically loads patient data, generates insights, and stores to history
# MAGIC 
# MAGIC ### **Usage:**
# MAGIC ```python
# MAGIC # Initialize
# MAGIC model = MessageGenerationModel(spark)
# MAGIC 
# MAGIC # Generate message for a patient (defaults to today)
# MAGIC result = model.predict(patient_id="17014", verbose=True)
# MAGIC 
# MAGIC # Generate for specific date
# MAGIC result = model.predict(patient_id="17014", date="2025-12-20", verbose=True)
# MAGIC 
# MAGIC # Access results
# MAGIC print(result['message'])
# MAGIC print(f"Rating: {result['rating']}")
# MAGIC ```
# MAGIC 
# MAGIC ### **Files:**
# MAGIC - `logic_engine.py` - Business logic for content selection
# MAGIC - `insight_generator.py` - LLM integration  
# MAGIC - `prompts.yml` - Editable templates and thresholds
# MAGIC - `main_pipeline.py` - This orchestration notebook
# MAGIC 
# MAGIC ### **Next Steps:**
# MAGIC 1. Run `create_history_table(spark)` to set up history table
# MAGIC 2. Test with `model.predict(patient_id="YOUR_ID", verbose=True)`
# MAGIC 3. Log to MLflow with `log_model_to_mlflow(model)`
# MAGIC 4. Deploy to Model Serving endpoint
