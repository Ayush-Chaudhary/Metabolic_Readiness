"""
Metabolic Readiness Testing Tool — Backend
===========================================
Synthetic data generator + pipeline runner.
Generates realistic patient feature data based on tester-selected scenarios,
then runs the real LogicEngine and InsightGenerator to produce insights.
"""

import sys
import os
import random
from datetime import datetime, timedelta
from dataclasses import asdict
from typing import Dict, Any, Optional, List, Tuple

# Add parent directory to path so we can import logic_engine & insight_generator
PARENT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from logic_engine import (
    LogicEngine,
    UserContext,
    MessageHistory,
    SelectedContent,
    A1CTargetGroup,
    Category,
)
from insight_generator import InsightGenerator


# =============================================================================
# SCENARIO DEFINITIONS — Maps dropdown labels to realistic value ranges
# =============================================================================

GLUCOSE_SCENARIOS = {
    "Excellent (TIR 80%+)": {"tir_range": (80, 95), "high_pct_range": (2, 10), "low_pct_range": (0, 2)},
    "Good (TIR 60-80%)": {"tir_range": (60, 79), "high_pct_range": (10, 25), "low_pct_range": (1, 4)},
    "Fair (TIR 40-60%)": {"tir_range": (40, 59), "high_pct_range": (25, 40), "low_pct_range": (2, 6)},
    "Poor (TIR <40%)": {"tir_range": (15, 39), "high_pct_range": (40, 60), "low_pct_range": (4, 10)},
    "No CGM Data": {"tir_range": None, "high_pct_range": None, "low_pct_range": None},
}

STEP_SCENARIOS = {
    "Very Active (10K+)": {"range": (10000, 18000)},
    "Active (6K-10K)": {"range": (6000, 9999)},
    "Light (2K-6K)": {"range": (2000, 5999)},
    "Sedentary (<2K)": {"range": (200, 1999)},
    "No Step Data": {"range": None},
}

ACTIVITY_SCENARIOS = {
    "Very Active (150+ min/wk)": {"daily_range": (25, 60), "weekly_range": (150, 300)},
    "Active (90-150 min/wk)": {"daily_range": (15, 30), "weekly_range": (90, 149)},
    "Moderate (30-90 min/wk)": {"daily_range": (5, 15), "weekly_range": (30, 89)},
    "Sedentary (<30 min/wk)": {"daily_range": (0, 5), "weekly_range": (0, 29)},
    "No Activity Data": {"daily_range": None, "weekly_range": None},
}

SLEEP_SCENARIOS = {
    "Excellent (7h+, rating 8+)": {"hours_range": (7.0, 9.0), "rating_range": (8, 10)},
    "Good (6-7h, rating 6-7)": {"hours_range": (6.0, 6.9), "rating_range": (6, 7)},
    "Fair (5-6h, rating 4-5)": {"hours_range": (5.0, 5.9), "rating_range": (4, 5)},
    "Poor (<5h, rating <4)": {"hours_range": (3.0, 4.9), "rating_range": (1, 3)},
    "No Sleep Data": {"hours_range": None, "rating_range": None},
}

FOOD_SCENARIOS = {
    "Thorough (3+ meals)": {"meals_range": (3, 4), "nutrient_met": True, "days_with_meals_7d": 6},
    "Moderate (1-2 meals)": {"meals_range": (1, 2), "nutrient_met": False, "days_with_meals_7d": 4},
    "Minimal (logged once)": {"meals_range": (1, 1), "nutrient_met": False, "days_with_meals_7d": 1},
    "No Food Logging": {"meals_range": (0, 0), "nutrient_met": False, "days_with_meals_7d": 0},
}

MED_SCENARIOS = {
    "Perfect (100%)": {"adherence_range": (1.0, 1.0), "took_all": True},
    "Good (75-99%)": {"adherence_range": (0.75, 0.99), "took_all": False},
    "Fair (50-74%)": {"adherence_range": (0.50, 0.74), "took_all": False},
    "Poor (<50%)": {"adherence_range": (0.10, 0.49), "took_all": False},
    "No Medications": {"adherence_range": None, "took_all": False},
}

WEIGHT_SCENARIOS = {
    "Losing (on track)": {"change_lbs": (-3.0, -1.0), "change_pct": (-3.0, -1.0), "logged_recently": True},
    "Stable (within ±1 lb)": {"change_lbs": (-0.9, 0.9), "change_pct": (-1.0, 1.0), "logged_recently": True},
    "Gaining": {"change_lbs": (1.0, 4.0), "change_pct": (1.0, 4.0), "logged_recently": True},
    "Not Logging": {"change_lbs": None, "change_pct": None, "logged_recently": False},
}

MENTAL_WELLBEING_SCENARIOS = {
    "Highly Engaged": {"meditation": True, "journal": True, "action_plan": True},
    "Moderate": {"meditation": True, "journal": False, "action_plan": False},
    "Minimal": {"meditation": False, "journal": False, "action_plan": False},
}

JOURNEY_SCENARIOS = {
    "Active + Task Completed": {"active": True, "task_completed": True, "completed_journey": False},
    "Active (no recent task)": {"active": True, "task_completed": False, "completed_journey": False},
    "Completed a Journey": {"active": False, "task_completed": False, "completed_journey": True},
    "No Journey": {"active": False, "task_completed": False, "completed_journey": False},
}

EXERCISE_VIDEO_SCENARIOS = {
    "Completed Video Today": {"completed_today": True, "completed_7d": True, "has_activity": True},
    "Completed Video This Week": {"completed_today": False, "completed_7d": True, "has_activity": True},
    "Has Activity (no completion)": {"completed_today": False, "completed_7d": False, "has_activity": True},
    "No Video Activity": {"completed_today": False, "completed_7d": False, "has_activity": False},
}

EXERCISE_PROGRAM_SCENARIOS = {
    "Active + Progress Today": {"active": True, "progress_today": True, "completed_today": False},
    "Active (no progress)": {"active": True, "progress_today": False, "completed_today": False},
    "Completed Program Today": {"active": False, "progress_today": False, "completed_today": True},
    "No Exercise Program": {"active": False, "progress_today": False, "completed_today": False},
}


def _rand_in_range(r: Tuple[float, float]) -> float:
    """Return a random float within a (min, max) range."""
    return round(random.uniform(r[0], r[1]), 1)


def _rand_int_in_range(r: Tuple[int, int]) -> int:
    """Return a random int within a (min, max) range."""
    return random.randint(r[0], r[1])


# =============================================================================
# SYNTHETIC DATA GENERATOR
# =============================================================================

def generate_synthetic_context(
    # Profile settings
    a1c_target_group: str = "DM Target <7%",
    user_focus: str = "None",
    weight_goal_type: str = "None",
    has_cgm: bool = True,
    has_step_tracker: bool = True,
    has_medications: bool = True,
    # Scenario dropdowns
    glucose_scenario: str = "Good (TIR 60-80%)",
    step_scenario: str = "Active (6K-10K)",
    activity_scenario: str = "Active (90-150 min/wk)",
    sleep_scenario: str = "Good (6-7h, rating 6-7)",
    food_scenario: str = "Moderate (1-2 meals)",
    med_scenario: str = "Good (75-99%)",
    weight_scenario: str = "Stable (within ±1 lb)",
    mental_scenario: str = "Moderate",
    journey_scenario: str = "No Journey",
    exercise_video_scenario: str = "No Video Activity",
    exercise_program_scenario: str = "No Exercise Program",
) -> UserContext:
    """
    Generate a synthetic UserContext based on tester-selected scenarios.
    
    Each scenario dropdown maps to a set of realistic value ranges.  Values
    within those ranges are randomized to add variety between runs.
    """
    # Map A1C dropdown to enum
    a1c_map = {
        "DM Target <7%": A1CTargetGroup.DM_TARGET_7,
        "DM Target <8%": A1CTargetGroup.DM_TARGET_8,
        "DIP (Diabetes in Pregnancy)": A1CTargetGroup.DIP,
        "Non-DM": A1CTargetGroup.NON_DM,
    }
    a1c_group = a1c_map.get(a1c_target_group, A1CTargetGroup.DM_TARGET_7)

    # Map user focus
    focus_map = {
        "None": None,
        "Weight": "Weight",
        "Glucose": "Glucose",
        "Activity": "Activity",
        "Eating Habits": "Eating Habits",
        "Sleep": "Sleep",
        "Medications": "Medications",
        "Anxiety": "Anxiety",
    }
    focus_val = focus_map.get(user_focus)

    # --- GLUCOSE ---
    g = GLUCOSE_SCENARIOS[glucose_scenario]
    tir_pct = _rand_in_range(g["tir_range"]) if g["tir_range"] else None
    tir_prev_day = round(tir_pct + random.uniform(-10, 5), 1) if tir_pct else None
    glucose_high_pct = _rand_in_range(g["high_pct_range"]) if g["high_pct_range"] else None
    glucose_low_pct = _rand_in_range(g["low_pct_range"]) if g["low_pct_range"] else None
    effective_has_cgm = has_cgm and glucose_scenario != "No CGM Data"

    # --- STEPS ---
    s = STEP_SCENARIOS[step_scenario]
    daily_step_count = _rand_int_in_range(s["range"]) if s["range"] else None
    prev_day_steps = _rand_int_in_range((max(500, (s["range"][0] - 2000)), s["range"][1])) if s["range"] else None
    effective_has_step_tracker = has_step_tracker and step_scenario != "No Step Data"

    # --- ACTIVITY ---
    a = ACTIVITY_SCENARIOS[activity_scenario]
    active_minutes = _rand_in_range(a["daily_range"]) if a["daily_range"] else None
    prev_day_active_minutes = _rand_in_range(a["daily_range"]) if a["daily_range"] else None
    weekly_active_minutes = _rand_in_range(a["weekly_range"]) if a["weekly_range"] else None
    exercise_video_completion_pct = random.uniform(50, 100) if active_minutes and active_minutes > 10 else None

    # --- SLEEP ---
    sl = SLEEP_SCENARIOS[sleep_scenario]
    sleep_hours = _rand_in_range(sl["hours_range"]) if sl["hours_range"] else None
    prev_sleep_hours = _rand_in_range(sl["hours_range"]) if sl["hours_range"] else None
    sleep_rating = _rand_int_in_range(sl["rating_range"]) if sl["rating_range"] else None
    prev_sleep_rating = _rand_int_in_range(sl["rating_range"]) if sl["rating_range"] else None
    avg_sleep_7d = round(sleep_hours + random.uniform(-0.5, 0.5), 1) if sleep_hours else None
    avg_sleep_rating_7d = round(sleep_rating + random.uniform(-1, 1), 1) if sleep_rating else None

    # --- FOOD ---
    f = FOOD_SCENARIOS[food_scenario]
    meals_logged = _rand_int_in_range(f["meals_range"])
    meal_types = ["breakfast", "lunch", "dinner", "snack"]
    last_meal_type = random.choice(meal_types) if meals_logged > 0 else None
    nutrient_met = f["nutrient_met"]
    nutrient_name = random.choice(["protein", "carbs", "fiber", "calories"]) if nutrient_met else None
    has_nutrient_goals = food_scenario != "No Food Logging"
    total_nutrient_targets = random.randint(2, 4) if has_nutrient_goals else 0

    # --- MEDICATIONS ---
    m = MED_SCENARIOS[med_scenario]
    effective_has_meds = has_medications and med_scenario != "No Medications"
    took_all_meds = m["took_all"]
    med_adherence_7d = _rand_in_range(m["adherence_range"]) if m["adherence_range"] else None

    # --- WEIGHT ---
    w = WEIGHT_SCENARIOS[weight_scenario]
    has_weight_goal = weight_goal_type != "None"
    weight_logged_yesterday = w["logged_recently"]
    weight_change_lbs_14d = _rand_in_range(w["change_lbs"]) if w["change_lbs"] else None
    weight_change_pct = _rand_in_range(w["change_pct"]) if w["change_pct"] else None
    weight_change_pct_14d = weight_change_pct
    days_since_weight = random.randint(0, 3) if w["logged_recently"] else random.randint(7, 30)
    weight_last_7d = days_since_weight <= 7
    weight_last_14d = days_since_weight <= 14
    weight_last_30d = days_since_weight <= 30
    is_within_maintenance = weight_scenario == "Stable (within ±1 lb)"

    # --- MENTAL WELLBEING ---
    mw = MENTAL_WELLBEING_SCENARIOS[mental_scenario]

    # --- JOURNEY ---
    j = JOURNEY_SCENARIOS[journey_scenario]

    # --- EXERCISE VIDEO ---
    ev = EXERCISE_VIDEO_SCENARIOS[exercise_video_scenario]

    # --- EXERCISE PROGRAM ---
    ep = EXERCISE_PROGRAM_SCENARIOS[exercise_program_scenario]

    # Map weight goal dropdown
    wgt_map = {"None": None, "Lose": "lose", "Maintain": "maintain", "Gain": "gain"}
    wg_type = wgt_map.get(weight_goal_type)

    # Build the UserContext
    ctx = UserContext(
        patient_id=f"SYNTH-{random.randint(10000, 99999)}",
        report_date=datetime.now() - timedelta(days=1),

        # Profile flags
        has_cgm=effective_has_cgm,
        has_step_tracker=effective_has_step_tracker,
        has_medications=effective_has_meds,
        has_weight_goal=has_weight_goal,
        weight_goal_type=wg_type,
        has_active_journey=j["active"],
        has_exercise_program=ep["active"],
        user_focus=focus_val,
        a1c_target_group=a1c_group,
        med_reminders_enabled=random.choice([True, False]) if effective_has_meds else False,

        # Glucose
        tir_pct=tir_pct,
        tir_prev_day=tir_prev_day,
        glucose_high_pct=glucose_high_pct,
        glucose_low_pct=glucose_low_pct,

        # Steps
        daily_step_count=daily_step_count,
        prev_day_steps=prev_day_steps,

        # Activity
        active_minutes=active_minutes,
        prev_day_active_minutes=prev_day_active_minutes,
        weekly_active_minutes=weekly_active_minutes,
        exercise_video_completion_pct=exercise_video_completion_pct,

        # Sleep
        sleep_duration_hours=sleep_hours,
        prev_day_sleep_hours=prev_sleep_hours,
        sleep_rating=sleep_rating,
        prev_day_sleep_rating=prev_sleep_rating,
        avg_sleep_hours_7d=avg_sleep_7d,
        avg_sleep_rating_7d=avg_sleep_rating_7d,

        # Weight
        weight_logged_yesterday=weight_logged_yesterday,
        weight_change_pct=weight_change_pct,
        weight_change_lbs_14d=weight_change_lbs_14d,
        weight_change_pct_14d=weight_change_pct_14d,
        days_since_last_weight=days_since_weight,
        weight_last_logged_7d=weight_last_7d,
        weight_last_logged_14d=weight_last_14d,
        weight_last_logged_30d=weight_last_30d,
        is_within_maintenance_range=is_within_maintenance,

        # Food
        meals_logged_count=meals_logged,
        last_meal_type=last_meal_type,
        any_nutrient_target_met=nutrient_met,
        nutrient_name_met=nutrient_name,
        days_with_meals_7d=f["days_with_meals_7d"],
        has_nutrient_goals=has_nutrient_goals,
        total_nutrient_targets=total_nutrient_targets,
        num_nutrient_targets_90_110=total_nutrient_targets if nutrient_met else 0,
        num_nutrient_targets_60_plus=total_nutrient_targets if nutrient_met else random.randint(0, total_nutrient_targets),
        num_nutrient_targets_30_plus=total_nutrient_targets if total_nutrient_targets > 0 else 0,

        # Medications
        took_all_meds=took_all_meds,
        med_adherence_7d_avg=med_adherence_7d,

        # Mental wellbeing
        meditation_opened_30d=mw["meditation"],
        journal_entry_30d=mw["journal"],
        action_plan_progress_30d=mw["action_plan"],
        action_plan_active=mw["action_plan"],
        journal_entry_7d=mw["journal"],
        meditation_opened_7d=mw["meditation"],

        # Journey
        journey_task_completed=j["task_completed"],
        has_completed_journey=j["completed_journey"],
        active_journey_count=1 if j["active"] else 0,
        completed_journey_count=1 if j["completed_journey"] else 0,

        # Exercise Video
        exercise_video_completed_today=ev["completed_today"],
        exercise_video_completed_7d=ev["completed_7d"],
        has_exercise_video_activity=ev["has_activity"],
        has_completed_video_ever=ev["completed_7d"],
        total_videos_completed=random.randint(1, 10) if ev["has_activity"] else 0,

        # Exercise Program
        has_active_exercise_program=ep["active"],
        has_completed_exercise_program=ep["completed_today"],
        exercise_program_started_today=False,
        exercise_program_completed_today=ep["completed_today"],
        exercise_program_progress_today=ep["progress_today"],
        exercise_program_progress_7d=ep["progress_today"] or ep["active"],
        active_program_count=1 if ep["active"] else 0,

        # Bonus
        bonus_exercise_video_completed=ev["completed_today"],
        bonus_exercise_program_started=ep["progress_today"],
        bonus_grocery_online=random.choice([True, False]),

        app_login_yesterday=True,
    )

    return ctx


# =============================================================================
# PIPELINE RUNNER
# =============================================================================

def run_pipeline(
    user_context: UserContext,
    config_path: str = None,
    temperature: float = 0.7,
    max_tokens: int = 180,
    max_positive_actions: int = 2,
    greeting_override: str = "Auto",
    databricks_host: Optional[str] = None,
    databricks_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the full insight generation pipeline on a UserContext.
    
    Returns a rich result dict with all intermediate data for display.
    
    When databricks_host + databricks_token are provided the LLM endpoint is
    called via the REST API (works locally with a PAT).  Otherwise the
    InsightGenerator falls back to built-in mock responses.
    """
    if config_path is None:
        config_path = os.path.join(PARENT_DIR, "prompts.yml")

    # Set Databricks env vars so the REST client can authenticate
    if databricks_host:
        os.environ["DATABRICKS_HOST"] = databricks_host
    if databricks_token:
        os.environ["DATABRICKS_TOKEN"] = databricks_token

    # Initialize engines
    logic_engine = LogicEngine(config_path)
    insight_gen = InsightGenerator(config_path)

    # Determine auth mode — priority: explicit SP creds > PAT > app-injected OAuth
    _sp_client_id = os.environ.get("LLM_SP_CLIENT_ID")
    _sp_client_secret = os.environ.get("LLM_SP_CLIENT_SECRET")
    _has_sp_creds = bool(_sp_client_id and _sp_client_secret)
    _has_token = bool(databricks_token)
    _has_oauth = bool(os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET"))

    if _has_sp_creds:
        # Use an explicitly configured service principal that has CAN_QUERY on the endpoint.
        # The SDK reads ALL Databricks env vars when building Config, so we must temporarily
        # remove any conflicting auth vars (PAT, app-injected OAuth) before constructing it.
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config
        _host = databricks_host or os.environ.get("DATABRICKS_HOST", "")
        _conflicting = ["DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]
        _saved = {k: os.environ.pop(k) for k in _conflicting if k in os.environ}
        try:
            explicit_client = WorkspaceClient(
                config=Config(
                    host=_host,
                    client_id=_sp_client_id,
                    client_secret=_sp_client_secret,
                )
            )
        finally:
            os.environ.update(_saved)  # always restore, even if Config() raises
        insight_gen._client = explicit_client
        insight_gen._endpoint = insight_gen.model_config["endpoint_name"]
    elif _has_token:
        # Local dev with PAT: force REST API path
        import types
        insight_gen._endpoint = insight_gen.model_config["endpoint_name"]
        insight_gen._call_llm = types.MethodType(
            lambda self, sys_p, usr_p: self._call_llm_with_openai_format(sys_p, usr_p),
            insight_gen,
        )
    elif _has_oauth:
        # Databricks Apps app-injected OAuth — just pre-set the endpoint
        insight_gen._endpoint = insight_gen.model_config["endpoint_name"]

    # Override model params
    insight_gen.model_config["temperature"] = temperature
    insight_gen.model_config["max_tokens"] = max_tokens
    logic_engine.config["message_constraints"]["max_positive_actions"] = max_positive_actions

    # Empty history (synthetic — no real history)
    history = MessageHistory(patient_id=user_context.patient_id)

    # Get all eligible actions & opportunities (for display)
    all_actions = logic_engine.get_eligible_positive_actions(user_context, history)
    all_opportunities = logic_engine.get_eligible_opportunities(user_context, history)

    # Run full content selection
    selected_content = logic_engine.select_content(user_context, history)

    # Override greeting if requested
    if greeting_override != "Auto":
        greeting_map = {
            "Morning": "Good morning.",
            "Afternoon": "Good afternoon.",
            "Evening": "Good evening.",
        }
        selected_content = SelectedContent(
            daily_rating=selected_content.daily_rating,
            rating_description=selected_content.rating_description,
            greeting=greeting_map.get(greeting_override, selected_content.greeting),
            positive_actions=selected_content.positive_actions,
            opportunity=selected_content.opportunity,
        )

    # Calculate the score details for display
    rating_name, rating_desc = logic_engine.calculate_daily_rating(user_context)

    # Generate the insight message
    generation_result = insight_gen.generate_insight(
        daily_rating=selected_content.daily_rating,
        rating_description=selected_content.rating_description,
        positive_actions=selected_content.positive_actions,
        opportunity=selected_content.opportunity,
        greeting=selected_content.greeting,
    )

    # Build the system + user prompts for display (Prompt Inspector)
    system_prompt = insight_gen._format_system_prompt()
    user_prompt = insight_gen._format_user_prompt(
        daily_rating=selected_content.daily_rating,
        rating_description=selected_content.rating_description,
        positive_actions=selected_content.positive_actions,
        opportunity=selected_content.opportunity,
        greeting=selected_content.greeting,
    )

    return {
        "success": generation_result.get("success", False),
        "message": generation_result.get("message", ""),
        "rating": selected_content.daily_rating,
        "rating_description": selected_content.rating_description,
        "greeting": selected_content.greeting,
        "character_count": generation_result.get("character_count", 0),
        "word_count": generation_result.get("word_count", 0),
        # Selected content
        "positive_actions_used": selected_content.positive_actions,
        "opportunity_used": selected_content.opportunity,
        # All eligible (for expandable display)
        "all_eligible_actions": all_actions,
        "all_eligible_opportunities": all_opportunities,
        # Prompts sent to LLM
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        # Model params used
        "temperature": temperature,
        "max_tokens": max_tokens,
        "max_positive_actions": max_positive_actions,
    }


# =============================================================================
# FEATURE SNAPSHOT — for display
# =============================================================================

def get_feature_snapshot(ctx: UserContext) -> Dict[str, Any]:
    """Extract key features from UserContext for display as a table."""
    return {
        "Patient ID": ctx.patient_id,
        "Report Date": ctx.report_date.strftime("%Y-%m-%d"),
        "A1C Target Group": ctx.a1c_target_group.value,
        "User Focus": ctx.user_focus or "None",
        # Devices
        "Has CGM": ctx.has_cgm,
        "Has Step Tracker": ctx.has_step_tracker,
        "Has Medications": ctx.has_medications,
        "Has Weight Goal": ctx.has_weight_goal,
        "Weight Goal Type": ctx.weight_goal_type or "None",
        "Has Active Journey": ctx.has_active_journey,
        "Has Exercise Program": ctx.has_exercise_program,
        # Glucose
        "TIR %": ctx.tir_pct,
        "TIR Prev Day": ctx.tir_prev_day,
        "Glucose High %": ctx.glucose_high_pct,
        "Glucose Low %": ctx.glucose_low_pct,
        # Steps
        "Daily Steps": ctx.daily_step_count,
        "Prev Day Steps": ctx.prev_day_steps,
        # Activity
        "Active Minutes": ctx.active_minutes,
        "Weekly Active Min": ctx.weekly_active_minutes,
        # Sleep
        "Sleep Hours": ctx.sleep_duration_hours,
        "Sleep Rating": ctx.sleep_rating,
        "Avg Sleep 7d": ctx.avg_sleep_hours_7d,
        # Food
        "Meals Logged": ctx.meals_logged_count,
        "Nutrient Target Met": ctx.any_nutrient_target_met,
        "Days with Meals (7d)": ctx.days_with_meals_7d,
        # Meds
        "Took All Meds": ctx.took_all_meds,
        "Med Adherence 7d": ctx.med_adherence_7d_avg,
        # Weight
        "Weight Logged Yesterday": ctx.weight_logged_yesterday,
        "Weight Change (lbs 14d)": ctx.weight_change_lbs_14d,
        "Days Since Weight": ctx.days_since_last_weight,
        # Mental
        "Meditation (30d)": ctx.meditation_opened_30d,
        "Journal (30d)": ctx.journal_entry_30d,
        "Action Plan Active": ctx.action_plan_active,
        # Journey
        "Journey Active": ctx.has_active_journey,
        "Journey Task Completed": ctx.journey_task_completed,
        # Exercise Video
        "Video Completed Today": ctx.exercise_video_completed_today,
        "Video Completed 7d": ctx.exercise_video_completed_7d,
        # Exercise Program  
        "Program Active": ctx.has_active_exercise_program,
        "Program Progress Today": ctx.exercise_program_progress_today,
    }
