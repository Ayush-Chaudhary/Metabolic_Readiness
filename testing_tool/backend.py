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

# Add src/ directory to path so we can import logic_engine & insight_generator
SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

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

# ---------------------------------------------------------------------------
# Column B — Weight
# ---------------------------------------------------------------------------
WEIGHT_SCENARIOS = {
    "Weight loss goal: logged & decreased": {
        "has_goal": True, "goal_type": "lose",
        "change_lbs": (-4.0, -0.5), "change_pct": (-3.0, -0.5),
        "logged_recently": True, "within_maintenance": False,
    },
    "Weight loss goal: logged & increased": {
        "has_goal": True, "goal_type": "lose",
        "change_lbs": (0.5, 4.0), "change_pct": (0.5, 3.0),
        "logged_recently": True, "within_maintenance": False,
    },
    "Weight maintenance: +/-3% goal": {
        "has_goal": True, "goal_type": "maintain",
        "change_lbs": (-1.5, 1.5), "change_pct": (-3.0, 3.0),
        "logged_recently": True, "within_maintenance": True,
    },
    "Weight maintenance: >3% goal": {
        "has_goal": True, "goal_type": "maintain",
        "change_lbs": (3.0, 8.0), "change_pct": (3.0, 8.0),
        "logged_recently": True, "within_maintenance": False,
    },
    "No goal: logged & decreased": {
        "has_goal": False, "goal_type": None,
        "change_lbs": (-4.0, -0.5), "change_pct": (-3.0, -0.5),
        "logged_recently": True, "within_maintenance": False,
    },
    "No goal: logged & increased": {
        "has_goal": False, "goal_type": None,
        "change_lbs": (0.5, 4.0), "change_pct": (0.5, 3.0),
        "logged_recently": True, "within_maintenance": False,
    },
    "No weight entry > 6 days": {
        "has_goal": False, "goal_type": None,
        "change_lbs": None, "change_pct": None,
        "logged_recently": False, "days_since": (7, 30), "within_maintenance": False,
    },
    "No weight data": {
        "has_goal": False, "goal_type": None,
        "change_lbs": None, "change_pct": None,
        "logged_recently": False, "days_since": (30, 90), "within_maintenance": False,
    },
}

# ---------------------------------------------------------------------------
# Column C — Glucose
# ---------------------------------------------------------------------------
GLUCOSE_SCENARIOS = {
    "No CGM": {
        "has_cgm": False,
        "tir_range": None, "high_pct_range": None, "low_pct_range": None,
    },
    "DM (A1C <7): TIR >= 70%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7",
        "tir_range": (70, 95), "high_pct_range": (2, 15), "low_pct_range": (0, 3),
    },
    "DM (A1C <8): TIR >= 50%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8",
        "tir_range": (50, 95), "high_pct_range": (2, 20), "low_pct_range": (0, 4),
    },
    "DIP User: TIR >= 70%": {
        "has_cgm": True, "a1c_hint": "DIP",
        "tir_range": (70, 95), "high_pct_range": (2, 10), "low_pct_range": (0, 3),
    },
    "Non-DM User: TIR >= 90%": {
        "has_cgm": True, "a1c_hint": "NON_DM",
        "tir_range": (90, 98), "high_pct_range": (0, 5), "low_pct_range": (0, 2),
    },
    "TIR improved from prev day": {
        "has_cgm": True,
        "tir_range": (50, 80), "high_pct_range": (10, 30), "low_pct_range": (1, 5),
        "improved": True,
    },
    "DM (A1C <7): TIR <70%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7",
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 4),
    },
    "DM (A1C <7): TIR <70% + No meals": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7", "no_meals": True,
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 4),
    },
    # Branch 2: take your medication (low_pct capped at 3 so safety guard doesn't suppress)
    "DM (A1C <7): TIR <70% + Glycemic med (not taking)": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7",
        "has_glycemic_med": True, "glycemic_not_taking": True,
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 3),
    },
    # Branch 3: contact provider (taking med but TIR still off; min_meals=2 ensures condition fires)
    "DM (A1C <7): TIR <70% + Glycemic med (taking)": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7",
        "has_glycemic_med": True, "glycemic_not_taking": False, "min_meals": 2,
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 8),
    },
    "DM (A1C <8): TIR <50%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8",
        "tir_range": (10, 49), "high_pct_range": (30, 60), "low_pct_range": (0, 1),
    },
    "DM (A1C <8): TIR <50% + No meals": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8", "no_meals": True,
        "tir_range": (10, 49), "high_pct_range": (30, 60), "low_pct_range": (0, 1),
    },
    # Branch 2: take your medication (low_pct at most 1 — DM_TARGET_8 threshold is 1%, safe_gt is strict)
    "DM (A1C <8): TIR <50% + Glycemic med (not taking)": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8",
        "has_glycemic_med": True, "glycemic_not_taking": True,
        "tir_range": (10, 49), "high_pct_range": (30, 60), "low_pct_range": (0, 1),
    },
    # Branch 3: contact provider
    "DM (A1C <8): TIR <50% + Glycemic med (taking)": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8",
        "has_glycemic_med": True, "glycemic_not_taking": False, "min_meals": 2,
        "tir_range": (10, 49), "high_pct_range": (30, 60), "low_pct_range": (0, 1),
    },
    "DIP: TIR <70%": {
        "has_cgm": True, "a1c_hint": "DIP",
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 5),
    },
    "DIP: TIR <70% + No meals": {
        "has_cgm": True, "a1c_hint": "DIP", "no_meals": True,
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 5),
    },
    # Branch 2: take your medication (low_pct capped at 4 — DIP threshold is 5%)
    "DIP: TIR <70% + Glycemic med (not taking)": {
        "has_cgm": True, "a1c_hint": "DIP",
        "has_glycemic_med": True, "glycemic_not_taking": True,
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 4),
    },
    # Branch 3: contact provider
    "DIP: TIR <70% + Glycemic med (taking)": {
        "has_cgm": True, "a1c_hint": "DIP",
        "has_glycemic_med": True, "glycemic_not_taking": False, "min_meals": 2,
        "tir_range": (30, 69), "high_pct_range": (20, 40), "low_pct_range": (1, 4),
    },
    "Non-DM: TIR <90%": {
        "has_cgm": True, "a1c_hint": "NON_DM",
        "tir_range": (60, 89), "high_pct_range": (5, 20), "low_pct_range": (0, 1),
    },
    "Non-DM: TIR <90% + No meals": {
        "has_cgm": True, "a1c_hint": "NON_DM", "no_meals": True,
        "tir_range": (60, 89), "high_pct_range": (5, 20), "low_pct_range": (0, 1),
    },
    "DM (A1C <7): High >25%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7",
        "tir_range": (30, 60), "high_pct_range": (26, 50), "low_pct_range": (1, 4),
    },
    "DM (A1C <8): High >50%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8",
        "tir_range": (10, 40), "high_pct_range": (51, 70), "low_pct_range": (1, 4),
    },
    "DIP: High >25%": {
        "has_cgm": True, "a1c_hint": "DIP",
        "tir_range": (30, 60), "high_pct_range": (26, 50), "low_pct_range": (1, 4),
    },
    "Non-DM: Above Target >5%": {
        "has_cgm": True, "a1c_hint": "NON_DM",
        "tir_range": (60, 89), "high_pct_range": (6, 20), "low_pct_range": (1, 3),
    },
    "DM (A1C <7): Low/V-Low >4%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_7",
        "tir_range": (40, 70), "high_pct_range": (5, 20), "low_pct_range": (5, 15),
    },
    "DM (A1C <8): Low/V-Low >1%": {
        "has_cgm": True, "a1c_hint": "DM_TARGET_8",
        "tir_range": (40, 70), "high_pct_range": (5, 20), "low_pct_range": (2, 10),
    },
    "DIP: Low/V-Low >5%": {
        "has_cgm": True, "a1c_hint": "DIP",
        "tir_range": (40, 70), "high_pct_range": (5, 20), "low_pct_range": (6, 15),
    },
    "Non-DM: Low >1%": {
        "has_cgm": True, "a1c_hint": "NON_DM",
        "tir_range": (70, 89), "high_pct_range": (3, 10), "low_pct_range": (2, 8),
    },
}

# ---------------------------------------------------------------------------
# Column D — Activity (consolidated: active minutes + video + program)
# ---------------------------------------------------------------------------
ACTIVITY_SCENARIOS = {
    "Logged active minutes": {
        "daily_range": (15, 45), "weekly_range": (100, 200),
        "vid_today": False, "vid_7d": False,
        "prog_active": False, "prog_progress": False,
    },
    "Did not log active minutes": {
        "daily_range": None, "weekly_range": (0, 30),
        "vid_today": False, "vid_7d": False,
        "prog_active": False, "prog_progress": False,
    },
    "Logged active min >= prev day": {
        "daily_range": (20, 60), "weekly_range": (120, 250),
        "vid_today": False, "vid_7d": False,
        "prog_active": False, "prog_progress": False,
        "ge_prev_day": True,
    },
    "Completed 90% video": {
        "daily_range": (20, 45), "weekly_range": (100, 200),
        "vid_today": True, "vid_7d": True,
        "prog_active": False, "prog_progress": False,
    },
    "Started exercise program": {
        "daily_range": (15, 40), "weekly_range": (80, 180),
        "vid_today": False, "vid_7d": False,
        "prog_active": True, "prog_progress": True,
    },
    "Met 150 min goal in last month but not last week": {
        "daily_range": (0, 15), "weekly_range": (0, 50),
        "vid_today": False, "vid_7d": False,
        "prog_active": False, "prog_progress": False,
        "monthly_goal": True,
    },
    "Total activity < 150 min": {
        "daily_range": (0, 10), "weekly_range": (0, 90),
        "vid_today": False, "vid_7d": False,
        "prog_active": False, "prog_progress": False,
    },
    "Active program: uncompleted": {
        "daily_range": (10, 30), "weekly_range": (50, 120),
        "vid_today": False, "vid_7d": False,
        "prog_active": True, "prog_progress": False,
    },
}

# ---------------------------------------------------------------------------
# Column E — Steps
# ---------------------------------------------------------------------------
STEP_SCENARIOS = {
    "Logged >= 10000 steps": {"range": (10000, 18000), "more_than_prev": False},
    "Logged more steps (than previous day)": {"range": (5000, 14000), "more_than_prev": True},
    "Avg steps < 6,000": {"range": (1000, 5999), "more_than_prev": False},
    "Steps 6k-9.9k & decreased": {"range": (6000, 9999), "more_than_prev": False, "decreased": True},
    "No step data": {"range": None},
}

# ---------------------------------------------------------------------------
# Column F — Food
# ---------------------------------------------------------------------------
FOOD_SCENARIOS = {
    "Logged at least one meal": {
        "meals_range": (1, 3), "nutrient_met": False, "days_7d": 3,
        "logged_yesterday": False, "ai_plan": False, "has_target": True,
    },
    "Logged food prev day": {
        "meals_range": (2, 4), "nutrient_met": False, "days_7d": 5,
        "logged_yesterday": True, "ai_plan": False, "has_target": True,
    },
    "Reached 90-110% target": {
        "meals_range": (3, 4), "nutrient_met": True, "days_7d": 6,
        "logged_yesterday": True, "ai_plan": False, "has_target": True,
    },
    "Generated AI meal plan": {
        "meals_range": (1, 3), "nutrient_met": False, "days_7d": 3,
        "logged_yesterday": False, "ai_plan": True, "has_target": True,
    },
    "Has target; not meeting": {
        "meals_range": (1, 2), "nutrient_met": False, "days_7d": 2,
        "logged_yesterday": False, "ai_plan": False, "has_target": True,
    },
    "Did not log yesterday (month ok)": {
        "meals_range": (0, 0), "nutrient_met": False, "days_7d": 4,
        "logged_yesterday": False, "ai_plan": False, "has_target": True,
    },
    "No log yesterday/month": {
        "meals_range": (0, 0), "nutrient_met": False, "days_7d": 0,
        "logged_yesterday": False, "ai_plan": False, "has_target": False,
    },
}

# ---------------------------------------------------------------------------
# Column G — Sleep
# ---------------------------------------------------------------------------
SLEEP_SCENARIOS = {
    "Slept >= 7 hours": {
        "hours_range": (7.0, 9.0), "rating_range": (5, 8),
        "ge_prev": False, "rating_ge_prev": False,
    },
    "Slept > prev day": {
        "hours_range": (6.5, 8.5), "rating_range": (5, 8),
        "ge_prev": True, "rating_ge_prev": False,
    },
    "Slept >= 7h & Rating >= 7": {
        "hours_range": (7.0, 9.0), "rating_range": (7, 10),
        "ge_prev": False, "rating_ge_prev": False,
    },
    "Slept >= 7h & Rating > prev": {
        "hours_range": (7.0, 8.5), "rating_range": (6, 9),
        "ge_prev": False, "rating_ge_prev": True,
    },
    "Avg duration >= 7h & Rating >= 7": {
        "hours_range": (7.0, 9.0), "rating_range": (7, 10),
        "ge_prev": False, "rating_ge_prev": False,
    },
    "Avg duration >= 7h; Rating < 7": {
        "hours_range": (7.0, 9.0), "rating_range": (3, 6),
        "ge_prev": False, "rating_ge_prev": False,
    },
    "Avg duration < 7h": {
        "hours_range": (4.0, 6.9), "rating_range": (3, 6),
        "ge_prev": False, "rating_ge_prev": False,
    },
    "No sleep entries": {
        "hours_range": None, "rating_range": None,
        "ge_prev": False, "rating_ge_prev": False,
    },
}

# ---------------------------------------------------------------------------
# Column H — Medications
# ---------------------------------------------------------------------------
MED_SCENARIOS = {
    "Took all meds": {"adherence_range": (1.0, 1.0), "took_all": True, "has_meds": True},
    "Taken 50-99% meds": {"adherence_range": (0.50, 0.99), "took_all": False, "has_meds": True},
    "Taken < 50% meds": {"adherence_range": (0.05, 0.49), "took_all": False, "has_meds": True},
    "No meds on Med List": {"adherence_range": None, "took_all": False, "has_meds": False},
}

# ---------------------------------------------------------------------------
# Column I — Mental Well-being
# ---------------------------------------------------------------------------
MENTAL_WELLBEING_SCENARIOS = {
    "Opened a meditation": {"meditation": True, "journal": False, "action_plan": False},
    "Made a journal entry": {"meditation": False, "journal": True, "action_plan": False},
    "Progress on Action Plan": {"meditation": False, "journal": False, "action_plan": True},
    "No meditation (30 days)": {"meditation": False, "journal": False, "action_plan": False},
    "No journal (30 days)": {"meditation": False, "journal": False, "action_plan": False},
    "No Action Plan progress": {"meditation": False, "journal": False, "action_plan": False},
    "Took no actions": {"meditation": False, "journal": False, "action_plan": False},
}

# ---------------------------------------------------------------------------
# Column J — Explore
# ---------------------------------------------------------------------------
EXPLORE_SCENARIOS = {
    "Read an article": {"vid_completed_7d": False, "lesson_completed": False, "content_opened": True},
    "Watched Learn video": {"vid_completed_7d": True, "lesson_completed": False, "content_opened": True},
    "Completed lesson": {"vid_completed_7d": False, "lesson_completed": True, "content_opened": True},
    "No content opened": {"vid_completed_7d": False, "lesson_completed": False, "content_opened": False},
    "Did not read article / watch video / complete lesson": {
        "vid_completed_7d": False, "lesson_completed": False, "content_opened": False,
    },
}

# ---------------------------------------------------------------------------
# Column K — Journey
# ---------------------------------------------------------------------------
JOURNEY_SCENARIOS = {
    "Completed task (within an active journey)": {
        "active": True, "task_completed": True, "completed_journey": False,
    },
    "No journey task / Did not complete any task": {
        "active": True, "task_completed": False, "completed_journey": False,
    },
    "No active journey": {
        "active": False, "task_completed": False, "completed_journey": False,
    },
}

# Keep these as aliases so existing imports don't break
EXERCISE_VIDEO_SCENARIOS = {k: {"completed_today": v["vid_completed_7d"], "completed_7d": v["vid_completed_7d"], "has_activity": v["content_opened"]} for k, v in EXPLORE_SCENARIOS.items()}
EXERCISE_PROGRAM_SCENARIOS = {k: {"active": v.get("prog_active", False), "progress_today": v.get("prog_progress", False), "completed_today": False} for k, v in ACTIVITY_SCENARIOS.items()}


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
    a1c_target_group: str = "DM (A1C <7): TIR >= 70%",  # can be overridden by glucose scenario hint
    user_focus: list = None,
    # Column-aligned scenario dropdowns
    weight_scenario: str = "No goal: logged & decreased",
    glucose_scenario: str = "DM (A1C <7): TIR >= 70%",
    activity_scenario: str = "Logged active minutes",
    step_scenario: str = "Logged >= 10000 steps",
    food_scenario: str = "Logged at least one meal",
    sleep_scenario: str = "Slept >= 7 hours",
    med_scenario: str = "Took all meds",
    mental_scenario: str = "Opened a meditation",
    explore_scenario: str = "Read an article",
    journey_scenario: str = "No active journey",
) -> UserContext:
    """
    Generate a synthetic UserContext based on tester-selected scenarios.
    
    Each scenario dropdown maps to a set of realistic value ranges.  Values
    within those ranges are randomized to add variety between runs.
    """
    # --- GLUCOSE ---
    g = GLUCOSE_SCENARIOS[glucose_scenario]
    effective_has_cgm = g["has_cgm"]

    # Derive a1c_group: prefer glucose scenario hint, fall back to a1c_target_group dropdown
    _hint_map = {
        "DM_TARGET_7": A1CTargetGroup.DM_TARGET_7,
        "DM_TARGET_8": A1CTargetGroup.DM_TARGET_8,
        "DIP":         A1CTargetGroup.DIP,
        "NON_DM":      A1CTargetGroup.NON_DM,
    }
    _dropdown_map = {
        "DM (A1C <7): TIR >= 70%": A1CTargetGroup.DM_TARGET_7,
        "DM (A1C <8): TIR >= 50%": A1CTargetGroup.DM_TARGET_8,
        "DIP (Diabetes in Pregnancy)": A1CTargetGroup.DIP,
        "Non-DM": A1CTargetGroup.NON_DM,
    }
    a1c_group = _hint_map.get(g.get("a1c_hint", ""), _dropdown_map.get(a1c_target_group, A1CTargetGroup.DM_TARGET_7))

    tir_pct = _rand_in_range(g["tir_range"]) if g["tir_range"] else None
    # If TIR improved, previous day's TIR should be lower
    if g.get("improved") and tir_pct is not None:
        tir_prev_day = round(max(0.0, tir_pct - random.uniform(5, 15)), 1)
    else:
        tir_prev_day = round(tir_pct + random.uniform(-10, 5), 1) if tir_pct else None

    glucose_high_pct = _rand_in_range(g["high_pct_range"]) if g["high_pct_range"] else None
    glucose_low_pct  = _rand_in_range(g["low_pct_range"])  if g["low_pct_range"]  else None

    # Glucose "no_meals" / "no_meds" / glycemic med flags (affect food/med sections below)
    _glucose_no_meals      = g.get("no_meals",           False)
    _glucose_no_meds       = g.get("no_meds",            False)
    _has_glycemic_med      = g.get("has_glycemic_med",   False)
    _glycemic_not_taking   = g.get("glycemic_not_taking", False)
    _glucose_min_meals     = g.get("min_meals",           0)

    # --- STEPS ---
    s = STEP_SCENARIOS[step_scenario]
    effective_has_step_tracker = s["range"] is not None
    daily_step_count = _rand_int_in_range(s["range"]) if s["range"] else None
    if s.get("more_than_prev") and daily_step_count is not None:
        prev_day_steps = max(500, daily_step_count - random.randint(500, 2000))
    elif s.get("decreased") and daily_step_count is not None:
        prev_day_steps = daily_step_count + random.randint(500, 2000)
    else:
        prev_day_steps = _rand_int_in_range((max(500, (s["range"][0] - 2000)), s["range"][1])) if s["range"] else None

    # --- ACTIVITY ---
    a = ACTIVITY_SCENARIOS[activity_scenario]
    active_minutes = _rand_in_range(a["daily_range"]) if a["daily_range"] else None
    if a.get("ge_prev_day") and active_minutes is not None:
        prev_day_active_minutes = max(0.0, active_minutes - random.uniform(5, 20))
    else:
        prev_day_active_minutes = _rand_in_range(a["daily_range"]) if a["daily_range"] else None
    weekly_active_minutes = _rand_in_range(a["weekly_range"]) if a["weekly_range"] else None
    exercise_video_completion_pct = random.uniform(50, 100) if active_minutes and active_minutes > 10 else None

    # --- SLEEP ---
    sl = SLEEP_SCENARIOS[sleep_scenario]
    sleep_hours = _rand_in_range(sl["hours_range"]) if sl["hours_range"] else None
    if sl.get("ge_prev") and sleep_hours is not None:
        prev_sleep_hours = max(0.0, sleep_hours - random.uniform(0.5, 1.5))
    else:
        prev_sleep_hours = _rand_in_range(sl["hours_range"]) if sl["hours_range"] else None
    sleep_rating = _rand_int_in_range(sl["rating_range"]) if sl["rating_range"] else None
    if sl.get("rating_ge_prev") and sleep_rating is not None:
        prev_sleep_rating = max(1, sleep_rating - random.randint(1, 2))
    else:
        prev_sleep_rating = _rand_int_in_range(sl["rating_range"]) if sl["rating_range"] else None
    avg_sleep_7d       = round(sleep_hours  + random.uniform(-0.5, 0.5), 1) if sleep_hours  else None
    avg_sleep_rating_7d = round(sleep_rating + random.uniform(-1,   1  ), 1) if sleep_rating else None

    # --- FOOD ---
    f = FOOD_SCENARIOS[food_scenario]
    # _glucose_min_meals lets a glucose scenario guarantee ≥N meals are logged
    # (needed so the "taking med but TIR off → contact provider" branch condition fires)
    meals_logged = 0 if _glucose_no_meals else max(_glucose_min_meals, _rand_int_in_range(f["meals_range"]))
    meal_types   = ["breakfast", "lunch", "dinner", "snack"]
    last_meal_type = random.choice(meal_types) if meals_logged > 0 else None
    nutrient_met = f["nutrient_met"]
    nutrient_name = random.choice(["protein", "carbs", "fiber", "calories"]) if nutrient_met else None
    has_nutrient_goals = f["has_target"]
    total_nutrient_targets = random.randint(2, 4) if has_nutrient_goals else 0
    days_with_meals = 0 if _glucose_no_meals else f["days_7d"]

    # --- MEDICATIONS ---
    m = MED_SCENARIOS[med_scenario]
    effective_has_meds = m["has_meds"]
    took_all_meds  = m["took_all"] and not _glucose_no_meds
    med_adherence_7d = (_rand_in_range(m["adherence_range"]) if m["adherence_range"] else None)
    if _glucose_no_meds and med_adherence_7d is not None:
        med_adherence_7d = round(random.uniform(0, 30), 1)

    # --- WEIGHT ---
    w = WEIGHT_SCENARIOS[weight_scenario]
    has_weight_goal      = w["has_goal"]
    wg_type              = w.get("goal_type")           # "lose" / "maintain" / None
    weight_logged_recently = w["logged_recently"]
    weight_change_lbs_14d  = _rand_in_range(w["change_lbs"]) if w["change_lbs"] else None
    weight_change_pct      = _rand_in_range(w["change_pct"]) if w["change_pct"] else None
    weight_change_pct_14d  = weight_change_pct
    # Prefer explicit days_since from the scenario; if absent, infer from logged_recently
    _days_since_range      = w.get("days_since") or ((0, 3) if weight_logged_recently else (7, 30))
    days_since_weight      = random.randint(*_days_since_range)
    weight_last_7d  = days_since_weight <= 7
    weight_last_14d = days_since_weight <= 14
    weight_last_30d = days_since_weight <= 30
    is_within_maintenance = w.get("within_maintenance", False)

    # --- MENTAL WELLBEING ---
    mw = MENTAL_WELLBEING_SCENARIOS[mental_scenario]

    # --- EXPLORE ---
    ex = EXPLORE_SCENARIOS[explore_scenario]

    # --- JOURNEY ---
    j = JOURNEY_SCENARIOS[journey_scenario]

    # --- MAP USER FOCUS ---
    focus_val = user_focus if user_focus else None

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
        has_exercise_program=a["prog_active"],
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
        weight_logged_yesterday=weight_logged_recently,
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
        days_with_meals_7d=days_with_meals,
        has_nutrient_goals=has_nutrient_goals,
        total_nutrient_targets=total_nutrient_targets,
        num_nutrient_targets_90_110=total_nutrient_targets if nutrient_met else 0,
        num_nutrient_targets_60_plus=total_nutrient_targets if nutrient_met else random.randint(0, total_nutrient_targets),
        num_nutrient_targets_30_plus=total_nutrient_targets if total_nutrient_targets > 0 else 0,

        # Medications
        took_all_meds=took_all_meds,
        med_adherence_7d_avg=med_adherence_7d,

        # Glycemic-lowering medication flags (driven by glucose scenario)
        takes_glycemic_lowering_med=_has_glycemic_med,
        glycemic_med_adherent=(not _glycemic_not_taking) if _has_glycemic_med else False,

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

        # Exercise Video (driven by Explore + Activity scenarios)
        exercise_video_completed_today=a["vid_today"],
        exercise_video_completed_7d=a["vid_7d"] or ex["vid_completed_7d"],
        has_exercise_video_activity=a["vid_today"] or a["vid_7d"],
        has_completed_video_ever=a["vid_7d"] or ex["vid_completed_7d"],
        total_videos_completed=random.randint(1, 10) if (a["vid_7d"] or ex["vid_completed_7d"]) else 0,

        # Exercise Program (driven by Activity scenario)
        has_active_exercise_program=a["prog_active"],
        has_completed_exercise_program=a["prog_progress"] == 1.0,
        exercise_program_started_today=a["prog_active"] and not a["prog_progress"],
        exercise_program_completed_today=a["prog_progress"] == 1.0,
        exercise_program_progress_today=a["prog_progress"],
        exercise_program_progress_7d=a["prog_progress"] or a["prog_active"],
        active_program_count=1 if a["prog_active"] else 0,

        # Bonus (includes explore / learn signals)
        bonus_exercise_video_completed=a["vid_today"],
        bonus_ai_meal_plan=f["ai_plan"],
        bonus_exercise_program_started=bool(a["prog_active"] and not a["prog_progress"]),
        bonus_grocery_online=random.choice([True, False]),
        bonus_article_read=ex["content_opened"] and not ex["vid_completed_7d"] and not ex["lesson_completed"],
        bonus_lesson_completed=ex["lesson_completed"],
        bonus_video_watched=ex["vid_completed_7d"],

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
        config_path = os.path.join(SRC_DIR, "prompts.yml")

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
        "User Focus": ", ".join(ctx.user_focus) if ctx.user_focus else "None",
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
        "Takes Glycemic Med": ctx.takes_glycemic_lowering_med,
        "Glycemic Med Adherent": ctx.glycemic_med_adherent,
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
