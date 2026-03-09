# ============================================================================
# SIMON Health Habits - Logic Engine
# ============================================================================
# This module handles the deterministic business logic for selecting
# positive actions and opportunities based on user features and rules.
# ============================================================================

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import yaml
import random


class Category(Enum):
    """Health habit categories for message prioritization."""
    GLUCOSE = "glucose"
    WEIGHT = "weight"
    ACTIVITY = "activity"
    STEPS = "steps"
    FOOD = "food"
    SLEEP = "sleep"
    MEDICATIONS = "medications"
    MENTAL_WELLBEING = "mental_wellbeing"
    EXPLORE = "explore"
    JOURNEY = "journey"


class A1CTargetGroup(Enum):
    """A1C target groups for glucose thresholds."""
    DM_TARGET_7 = "dm_target_7"
    DM_TARGET_8 = "dm_target_8"
    DIP = "dip"
    NON_DM = "non_dm"


# Canonical mapping: focus display name → internal category names.
# Internal names must match what _get_action_category() returns.
FOCUS_CATEGORY_MAP: Dict[str, List[str]] = {
    'Weight':        ['weight'],
    'Glucose':       ['glucose'],
    'Activity':      ['activity', 'steps'],
    'Eating Habits': ['food'],
    'Sleep':         ['sleep'],
    'Medications':   ['medications'],
    'Anxiety':       ['mental_wellbeing'],
}


# ============================================================================
# SAFE COMPARISON HELPERS - Handle None values gracefully
# ============================================================================

def safe_gte(value, threshold, default=False):
    """
    Safely check if value >= threshold, handling None.
    
    Args:
        value: The value to compare (may be None)
        threshold: The threshold to compare against
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if value >= threshold, otherwise default
    """
    return value >= threshold if value is not None else default


def safe_gt(value, threshold, default=False):
    """
    Safely check if value > threshold, handling None.
    
    Args:
        value: The value to compare (may be None)
        threshold: The threshold to compare against
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if value > threshold, otherwise default
    """
    return value > threshold if value is not None else default


def safe_lte(value, threshold, default=False):
    """
    Safely check if value <= threshold, handling None.
    
    Args:
        value: The value to compare (may be None)
        threshold: The threshold to compare against
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if value <= threshold, otherwise default
    """
    return value <= threshold if value is not None else default


def safe_lt(value, threshold, default=False):
    """
    Safely check if value < threshold, handling None.
    
    Args:
        value: The value to compare (may be None)
        threshold: The threshold to compare against
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if value < threshold, otherwise default
    """
    return value < threshold if value is not None else default


def safe_eq(value, target, default=False):
    """
    Safely check equality, handling None.
    
    Args:
        value: The value to compare (may be None)
        target: The target to compare against
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if value == target, otherwise default
    """
    return value == target if value is not None else default


def safe_range(value, min_val, max_val, default=False):
    """
    Safely check if value is within range [min_val, max_val], handling None.
    
    Args:
        value: The value to check (may be None)
        min_val: Minimum value (inclusive)
        max_val: Maximum value (inclusive)
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if min_val <= value <= max_val, otherwise default
    """
    return min_val <= value <= max_val if value is not None else default


def safe_abs_lte(value, threshold, default=False):
    """
    Safely check if abs(value) <= threshold, handling None.
    
    Args:
        value: The value to check (may be None)
        threshold: The threshold for absolute value
        default: What to return if value is None (default: False)
    
    Returns:
        bool: True if abs(value) <= threshold, otherwise default
    """
    return abs(value) <= threshold if value is not None else default


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class UserContext:
    """User profile and feature data for message generation."""
    patient_id: str
    report_date: datetime
    
    # Profile flags
    has_cgm: bool = False
    has_step_tracker: bool = False
    has_medications: bool = False
    has_weight_goal: bool = False
    weight_goal_type: Optional[str] = None  # "lose", "maintain", "gain"
    has_active_journey: bool = False
    has_exercise_program: bool = False
    user_focus: Optional[List[str]] = None  # ["Weight", "Glucose", ...] or None
    a1c_target_group: Optional[A1CTargetGroup] = None  # None means no A1C target on file
    med_reminders_enabled: bool = False

    @property
    def effective_a1c_group(self) -> A1CTargetGroup:
        """Returns the A1C target group, falling back to DM_TARGET_7 when None."""
        return self.a1c_target_group if self.a1c_target_group is not None else A1CTargetGroup.DM_TARGET_7
    
    # Yesterday's features (from Gold table)
    tir_pct: Optional[float] = None
    tir_prev_day: Optional[float] = None
    glucose_high_pct: Optional[float] = None
    glucose_low_pct: Optional[float] = None
    
    daily_step_count: Optional[int] = None
    prev_day_steps: Optional[int] = None
    
    active_minutes: Optional[float] = None
    prev_day_active_minutes: Optional[float] = None
    weekly_active_minutes: Optional[float] = None
    exercise_video_completion_pct: Optional[float] = None
    
    sleep_duration_hours: Optional[float] = None
    prev_day_sleep_hours: Optional[float] = None
    sleep_rating: Optional[float] = None
    prev_day_sleep_rating: Optional[float] = None
    avg_sleep_hours_7d: Optional[float] = None
    avg_sleep_rating_7d: Optional[float] = None
    
    weight_logged_yesterday: bool = False
    weight_change_lbs_14d: Optional[float] = None  # Absolute weight change in lbs over 14 days
    weight_change_pct: Optional[float] = None
    weight_change_pct_14d: Optional[float] = None  # Percentage change over 14 days for maintenance goal
    weight_last_logged_7d: bool = False
    weight_last_logged_14d: bool = False
    weight_last_logged_30d: bool = False
    days_since_last_weight: Optional[int] = None
    is_within_maintenance_range: bool = False
    
    meals_logged_count: Optional[int] = None
    last_meal_type: Optional[str] = None
    any_nutrient_target_met: bool = False
    nutrient_name_met: Optional[str] = None
    days_with_meals_7d: Optional[int] = None
    has_nutrient_goals: bool = False
    
    # Nutrient target percentages (for scoring category 6)
    protein_target_met_pct: Optional[float] = None  # % of protein target met (0-150+)
    carbs_target_met_pct: Optional[float] = None    # % of carbs target met (0-150+)
    fat_target_met_pct: Optional[float] = None      # % of fat target met (0-150+)
    calories_target_met_pct: Optional[float] = None # % of calories target met (0-150+)
    num_nutrient_targets_90_110: int = 0            # Count of nutrients in 90-110% range
    num_nutrient_targets_60_plus: int = 0           # Count of nutrients >= 60%
    num_nutrient_targets_30_plus: int = 0           # Count of nutrients >= 30%
    total_nutrient_targets: int = 0                 # Total number of nutrient goals set
    
    took_all_meds: bool = False
    med_adherence_7d_avg: Optional[float] = None
    
    meditation_opened_30d: bool = False
    journal_entry_30d: bool = False
    action_plan_progress_30d: bool = False
    
    # Mental wellbeing (category 10) - updated fields for 7-day lookback
    action_plan_active: bool = False          # Has an active action plan
    journal_entry_7d: bool = False            # Journaled at least once in last 7 days
    meditation_opened_7d: bool = False        # Opened meditation at least once in last 7 days
    
    # Journey tracking - from GuidedJourneyWeeksAndTasksDetail table
    # has_active_journey is already defined in profile flags above
    journey_task_completed: bool = False       # User completed a journey task
    has_completed_journey: bool = False        # User has completed at least one journey
    active_journey_count: int = 0              # Number of active journeys
    completed_journey_count: int = 0           # Number of completed journeys
    
    # Exercise Video tracking - from curatedvideositemdetail table
    exercise_video_completed_today: bool = False   # Completed a video today
    exercise_video_completed_7d: bool = False      # Completed a video in last 7 days
    has_exercise_video_activity: bool = False      # Has any video activity
    has_completed_video_ever: bool = False         # Has completed at least one video
    total_videos_completed: int = 0                # Count of completed videos
    
    # Exercise Program tracking - from curatedvideosprogramdetail table
    # has_exercise_program is already defined in profile flags above
    has_active_exercise_program: bool = False      # Has at least one active program
    has_completed_exercise_program: bool = False   # Has completed at least one program
    exercise_program_started_today: bool = False   # Started a program today
    exercise_program_completed_today: bool = False # Completed a program today
    exercise_program_progress_today: bool = False  # Any program progress today (started or completed)
    exercise_program_progress_7d: bool = False     # Any program progress in last 7 days
    active_program_count: int = 0                  # Number of active programs
    
    # Bonus activity tracking (updated with actual data sources)
    bonus_exercise_video_completed: bool = False  # Completed exercise video (from curatedvideositemdetail)
    bonus_ai_meal_plan: bool = False              # Generated AI meal plan
    bonus_exercise_program_started: bool = False  # Started exercise program (from curatedvideosprogramdetail)
    bonus_grocery_online: bool = False            # From grocerydetails table
    bonus_article_read: bool = False              # PLACEHOLDER: Requires content_interaction table
    bonus_lesson_completed: bool = False          # PLACEHOLDER: Requires content_interaction table
    bonus_video_watched: bool = False             # PLACEHOLDER: Requires content_interaction table
    
    app_login_yesterday: bool = True
    
    # Eligibility arrays from Gold table (pre-calculated)
    eligible_positive_actions: List[str] = field(default_factory=list)
    eligible_opportunities: List[str] = field(default_factory=list)


@dataclass
class MessageHistory:
    """Tracks what messages have been shown to user recently."""
    patient_id: str
    categories_shown_last_6d: List[str] = field(default_factory=list)
    weight_messages_this_week: int = 0
    weight_shown_yesterday: bool = False
    category_streaks: Dict[str, int] = field(default_factory=dict)  # category -> consecutive days


@dataclass
class SelectedContent:
    """Output of the selection logic - what to show the user."""
    daily_rating: str
    rating_description: str
    greeting: str
    positive_actions: List[Dict[str, Any]]  # List of action dicts with text and metadata
    opportunity: Dict[str, Any]  # Single opportunity dict with text and metadata
    

class LogicEngine:
    """
    Deterministic logic engine for selecting positive actions and opportunities.
    
    This engine applies the business rules defined in the requirements to:
    1. Filter eligible actions based on user context
    2. Apply priority rules
    3. Apply frequency caps and variety rules
    4. Select final actions and opportunities
    """
    
    def __init__(self, config_path: str = "prompts.yml"):
        """Initialize the logic engine with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.thresholds = self.config['clinical_thresholds']
        self.priority_rules = self.config['priority_rules']
        self.message_history_rules = self.config['message_history']
        self.focus_mappings = self.config['focus_area_mappings']
        self.ratings = self.config['daily_ratings']
        self.greetings = self.config['greetings']
        self.positive_templates = self.config['prompt_segments']['positive_actions']
        self.opportunity_templates = self.config['prompt_segments']['opportunities']
    
    def calculate_daily_rating(self, user: UserContext) -> Tuple[str, str]:
        """
        Calculate the daily rating based on 10-category health management scoring.
        
        Total possible: 50 base points (10 categories × 5 points each) + bonus points
        Categories are included based on user focus area and device/feature availability.
        
        Returns:
            Tuple of (rating_name, rating_description)
        """
        # Debug: Show key data fields
        print(f"    📊 User Data Summary:")
        print(f"       has_cgm={user.has_cgm}, tir_pct={user.tir_pct}")
        print(f"       has_step_tracker={user.has_step_tracker}, daily_step_count={user.daily_step_count}")
        print(f"       has_weight_goal={user.has_weight_goal}, weight_change_lbs_14d={user.weight_change_lbs_14d}")
        print(f"       weekly_active_minutes={user.weekly_active_minutes}")
        print(f"       meals_logged_count={user.meals_logged_count}, total_nutrient_targets={user.total_nutrient_targets}")
        print(f"       sleep_duration_hours={user.sleep_duration_hours}, sleep_rating={user.sleep_rating}")
        
        score = 0
        max_score = 0
        
        # Get scoring criteria from config
        scoring = self.config.get('scoring_criteria', {})
        
        # Helper function to check if category should be included
        def should_include_category(category_name: str, has_device: bool = True) -> bool:
            """Check if category should be included based on focus area(s) and device availability."""
            if not has_device:
                return False
            
            # If no focus set, include all available categories
            if not user.user_focus:
                return True
            
            # Map focus areas to category names
            focus_to_category = {
                'Weight': ['weight'],
                'Glucose': ['glucose'],
                'Activity': ['activity', 'steps'],
                'Eating Habits': ['food_logging', 'nutrient_targets'],
                'Sleep': ['sleep_duration', 'sleep_rating'],
                'Medications': ['medications'],
                'Anxiety': ['mental_wellbeing']
            }
            
            # Check if category is included for ANY of the user's active focuses
            for active_focus in user.user_focus:
                categories = focus_to_category.get(active_focus, [])
                if category_name in categories:
                    return True
            
            return False
        
        # === CATEGORY 1: WEIGHT (5 points) ===
        if should_include_category('weight'):
            max_score += 5
            weight_rules = scoring.get('weight', {}).get('scoring_rules', {})
            
            if user.has_weight_goal and user.weight_goal_type == 'lose':
                # Weight loss goal
                if safe_lte(user.weight_change_lbs_14d, -1.0):  # Decreased by at least 1 lb
                    score += weight_rules.get('loss_goal', {}).get('decreased_1lb_14d', 5)
                elif safe_range(user.weight_change_lbs_14d, -1.0, 1.0):  # Stable within +/- 1 lb
                    score += weight_rules.get('loss_goal', {}).get('stable_1lb_14d', 3)
                # else gained 1+ lb: 0 points
            
            elif user.has_weight_goal and user.weight_goal_type == 'maintain':
                # Maintenance goal
                if safe_abs_lte(user.weight_change_pct_14d, 3.0):  # Within +/- 3%
                    score += weight_rules.get('maintenance_goal', {}).get('within_3pct_14d', 5)
                # else outside range: 0 points
            
            else:
                # No weight goal - score based on logging frequency
                if user.weight_last_logged_7d:
                    score += weight_rules.get('no_goal', {}).get('logged_7d', 5)
                elif user.weight_last_logged_14d:
                    score += weight_rules.get('no_goal', {}).get('logged_14d', 3)
                elif user.weight_last_logged_30d:
                    score += weight_rules.get('no_goal', {}).get('logged_30d', 1)
                # else not logged: 0 points
        
        # === CATEGORY 2: GLUCOSE/CGM (5 points) ===
        if should_include_category('glucose', has_device=user.has_cgm):
            max_score += 5
            glucose_rules = scoring.get('glucose', {}).get('scoring_rules', {})
            
            # Get target group key (falls back to dm_target_7 when no A1C target on file)
            target_key = user.effective_a1c_group.value  # 'a1c_target_7', 'a1c_target_8', etc.
            target_rules = glucose_rules.get(target_key, {})
            
            if target_key == 'a1c_target_7':
                if safe_gte(user.tir_pct, 70):
                    score += target_rules.get('tir_70_pct', 5)
                elif safe_gte(user.tir_pct, 60):
                    score += target_rules.get('tir_60_pct', 3)
                elif safe_lte(user.tir_pct, 50):
                    score += target_rules.get('tir_50_or_less', 1)
            
            elif target_key == 'a1c_target_8':
                if safe_gte(user.tir_pct, 50):
                    score += target_rules.get('tir_50_pct', 5)
                elif safe_gte(user.tir_pct, 40):
                    score += target_rules.get('tir_40_pct', 3)
                elif safe_lte(user.tir_pct, 30):
                    score += target_rules.get('tir_30_or_less', 1)
        
        # === CATEGORY 3: ACTIVITY (5 points) ===
        if should_include_category('activity'):
            max_score += 5
            activity_rules = scoring.get('activity', {}).get('scoring_rules', {})
            
            # Use weekly_active_minutes (7-day total)
            if safe_gte(user.weekly_active_minutes, 150):
                score += activity_rules.get('minutes_150_7d', 5)
            elif safe_gte(user.weekly_active_minutes, 120):
                score += activity_rules.get('minutes_120_7d', 4)
            elif safe_gte(user.weekly_active_minutes, 90):
                score += activity_rules.get('minutes_90_7d', 3)
            elif safe_gte(user.weekly_active_minutes, 60):
                score += activity_rules.get('minutes_60_7d', 2)
            elif safe_gte(user.weekly_active_minutes, 30):
                score += activity_rules.get('minutes_30_7d', 1)
            # else no activity: 0 points
        
        # === CATEGORY 4: STEPS (5 points) ===
        if should_include_category('steps', has_device=user.has_step_tracker):
            max_score += 5
            steps_rules = scoring.get('steps', {}).get('scoring_rules', {})
            
            if safe_gte(user.daily_step_count, 10000):
                score += steps_rules.get('steps_10000', 5)
            elif safe_gte(user.daily_step_count, 6000):
                score += steps_rules.get('steps_6000', 3)
            elif safe_gte(user.daily_step_count, 2000):
                score += steps_rules.get('steps_2000', 1)
            # else no steps: 0 points
        
        # === CATEGORY 5: FOOD LOGGING (5 points) ===
        if should_include_category('food_logging'):
            max_score += 5
            food_rules = scoring.get('food_logging', {}).get('scoring_rules', {})
            
            if safe_gte(user.meals_logged_count, 3):
                score += food_rules.get('meals_3', 5)
            elif safe_gte(user.meals_logged_count, 2):
                score += food_rules.get('meals_2', 3)
            elif safe_gte(user.meals_logged_count, 1):
                score += food_rules.get('meals_1', 1)
            # else no meals: 0 points
        
        # === CATEGORY 6: NUTRIENT TARGETS (5 points) ===
        if should_include_category('nutrient_targets') and user.has_nutrient_goals:
            max_score += 5
            nutrient_rules = scoring.get('nutrient_targets', {}).get('scoring_rules', {})
            
            # Check if user met all nutrient targets in 90-110% range
            if safe_gt(user.total_nutrient_targets, 0):
                if safe_eq(user.num_nutrient_targets_90_110, user.total_nutrient_targets):
                    score += nutrient_rules.get('met_100_pct_all', 5)
                elif safe_eq(user.num_nutrient_targets_60_plus, user.total_nutrient_targets):
                    score += nutrient_rules.get('met_60_pct_all', 3)
                elif safe_eq(user.num_nutrient_targets_30_plus, user.total_nutrient_targets):
                    score += nutrient_rules.get('met_30_pct_all', 1)
                # else no targets met: 0 points
        
        # === CATEGORY 7: SLEEP DURATION (5 points) ===
        if should_include_category('sleep_duration'):
            max_score += 5
            sleep_dur_rules = scoring.get('sleep_duration', {}).get('scoring_rules', {})
            
            if safe_gte(user.sleep_duration_hours, 7):
                score += sleep_dur_rules.get('hours_7_plus', 5)
            elif safe_gte(user.sleep_duration_hours, 6):
                score += sleep_dur_rules.get('hours_6_plus', 3)
            elif safe_gte(user.sleep_duration_hours, 5):
                score += sleep_dur_rules.get('hours_5_plus', 1)
            # else less than 5 hours: 0 points
        
        # === CATEGORY 8: SLEEP RATING (5 points) ===
        if should_include_category('sleep_rating'):
            max_score += 5
            sleep_rating_rules = scoring.get('sleep_rating', {}).get('scoring_rules', {})
            
            if safe_gte(user.sleep_rating, 10):
                score += sleep_rating_rules.get('rating_10', 5)
            elif safe_gte(user.sleep_rating, 7):
                score += sleep_rating_rules.get('rating_7_plus', 3)
            elif safe_gte(user.sleep_rating, 4):
                score += sleep_rating_rules.get('rating_4_plus', 1)
            # else rating 3 or less: 0 points
        
        # === CATEGORY 9: MEDICATIONS (5 points) ===
        if should_include_category('medications', has_device=user.has_medications):
            max_score += 5
            med_rules = scoring.get('medications', {}).get('scoring_rules', {})
            
            if user.med_adherence_7d_avg is not None:
                adherence_pct = user.med_adherence_7d_avg * 100
                if adherence_pct >= 100:
                    score += med_rules.get('adherence_100_pct', 5)
                elif adherence_pct >= 75:
                    score += med_rules.get('adherence_75_pct', 3)
                elif adherence_pct >= 50:
                    score += med_rules.get('adherence_50_pct', 1)
                # else below 50%: 0 points
        
        # === CATEGORY 10: MENTAL WELL-BEING (5 points) ===
        if should_include_category('mental_wellbeing'):
            max_score += 5
            mwb_rules = scoring.get('mental_wellbeing', {}).get('scoring_rules', {})
            
            # Composite scoring (cumulative)
            mwb_score = 0
            if user.action_plan_active:
                mwb_score += mwb_rules.get('action_plan_active', 3)
            if user.journal_entry_7d:
                mwb_score += mwb_rules.get('journal_7d', 1)
            if user.meditation_opened_7d:
                mwb_score += mwb_rules.get('meditation_7d', 1)
            
            score += min(mwb_score, 5)  # Cap at 5 points
        
        # === BONUS POINTS ===
        bonus_rules = scoring.get('bonus_points', {})
        if user.bonus_exercise_video_completed:
            score += bonus_rules.get('exercise_video_completed', 3)
        if user.bonus_ai_meal_plan:
            score += bonus_rules.get('ai_meal_plan_generated', 1)
        if user.bonus_exercise_program_started:
            score += bonus_rules.get('exercise_program_started', 1)
        if user.bonus_grocery_online:
            score += bonus_rules.get('grocery_shopping_online', 1)
        if user.bonus_article_read:
            score += bonus_rules.get('article_read', 1)
        if user.bonus_lesson_completed:
            score += bonus_rules.get('lesson_completed', 1)
        if user.bonus_video_watched:
            score += bonus_rules.get('learn_video_watched', 1)
        
        # Calculate percentage (handle case where no categories available)
        if max_score == 0:
            percentage = 0
        else:
            percentage = (score / max_score) * 100
        
        # Map to rating tier
        for rating_key in ['committed', 'strong', 'consistent', 'building', 'ready']:
            rating = self.ratings[rating_key]
            if percentage >= rating['min_score']:
                return rating['name'], rating['description']
        
        # Default fallback
        return self.ratings['ready']['name'], self.ratings['ready']['description']
    
    def get_greeting(self, hour: int = None) -> str:
        """Get appropriate greeting based on time of day."""
        if hour is None:
            hour = datetime.now().hour
        
        for period, data in self.greetings.items():
            if hour in data['hours']:
                return data['options'][0]
        
        return "Hello."
    
    def _get_focus_weights(self, user: UserContext) -> Dict[str, float]:
        """
        Returns a weight for each active focus area.

        Strategy: 'uniform' — all active focuses share equal weight.
        Future strategies (e.g. primary-focus-heavy, recency-based) can be
        swapped in here without touching any other code.

        Returns:
            Dict mapping focus_name → weight.  Weights sum to 1.0.
        """
        if not user.user_focus:
            return {}
        # STRATEGY: uniform
        n = len(user.user_focus)
        return {focus: 1.0 / n for focus in user.user_focus}

    def _is_category_allowed(self, category: str, user: UserContext) -> bool:
        """Check if category is allowed based on user's focus area(s)."""
        if not user.user_focus:
            return True

        # A category is allowed when it appears in ANY of the user's active focuses.
        # Uses FOCUS_CATEGORY_MAP (internal names) so 'Eating Habits' → 'food', etc.
        for focus_name in user.user_focus:
            if category in FOCUS_CATEGORY_MAP.get(focus_name, []):
                return True

        return False
    
    def _check_device_requirements(self, action_key: str, user: UserContext) -> bool:
        """Check if user has required devices for an action."""
        template = self.positive_templates.get(action_key, {})
        
        if template.get('requires_cgm', False) and not user.has_cgm:
            return False
        if template.get('requires_step_tracker', False) and not user.has_step_tracker:
            return False
        if template.get('requires_medications', False) and not user.has_medications:
            return False
        if template.get('requires_weight_goal', False) and not user.has_weight_goal:
            return False
        if template.get('requires_journey', False) and not user.has_active_journey:
            return False
        if template.get('requires_exercise_program', False) and not user.has_exercise_program:
            return False
        
        return True
    
    def _check_weight_goal_type(self, action_key: str, user: UserContext) -> bool:
        """Check if action matches user's weight goal type."""
        template = self.positive_templates.get(action_key, {})
        required_goal_type = template.get('goal_type')
        
        if required_goal_type is None:
            return True
        
        return user.weight_goal_type == required_goal_type
    
    def _calculate_action_priority(
        self, 
        action_key: str, 
        user: UserContext, 
        history: MessageHistory
    ) -> int:
        """
        Calculate the priority score for a positive action.
        Higher score = higher priority.
        """
        template = self.positive_templates.get(action_key, {})
        base_priority = 100 - template.get('priority', 50)  # Invert so lower number = higher priority
        
        # Journey boost
        if action_key == 'journey_task_completed' and user.has_active_journey:
            base_priority += self.priority_rules['journey_active']
        
        # CGM with good TIR boost
        if action_key.startswith('glucose_') and user.has_cgm:
            thresholds = self.thresholds['glucose'][user.effective_a1c_group.value]
            if safe_gte(user.tir_pct, thresholds['tir_positive_threshold']):
                base_priority += self.priority_rules['cgm_with_good_tir']
        
        # Weight goal boost for related categories
        if user.has_weight_goal:
            category = self._get_action_category(action_key)
            if category in ['food', 'activity', 'steps', 'sleep']:
                base_priority += self.priority_rules['weight_goal_active']
        
        # Medication boost
        if action_key == 'medication_adherence' and user.has_medications:
            base_priority += self.priority_rules['medications_on_list']
        
        # User focus area boost — weighted by focus relevance.
        # With uniform weighting each matching focus contributes equally;
        # the total boost equals user_focus_area regardless of how many
        # focuses the user has (i.e. matching ANY focus gives the full boost).
        category = self._get_action_category(action_key)
        if user.user_focus:
            weights = self._get_focus_weights(user)
            focus_weight_total = sum(
                weights.get(focus, 0.0)
                for focus in user.user_focus
                if category in FOCUS_CATEGORY_MAP.get(focus, [])
            )
            if focus_weight_total > 0:
                # Scale boost: full boost when at least one focus matches.
                # Multiply back by n so uniform weights cancel out → always
                # adds the full 'user_focus_area' point value.
                n = len(user.user_focus)
                base_priority += int(self.priority_rules['user_focus_area'] * focus_weight_total * n)
        
        # Not shown in 6 days boost
        if category not in history.categories_shown_last_6d:
            base_priority += self.priority_rules['not_shown_6_days']
        
        # Streak penalty (if excelling for 3+ days, reduce priority)
        streak_days = history.category_streaks.get(category, 0)
        if streak_days >= self.priority_rules['streak_override_days']:
            base_priority -= self.priority_rules['streak_priority_penalty']
        
        return base_priority
    
    def _get_action_category(self, action_key: str) -> str:
        """Get the category for an action key."""
        category_mapping = {
            'glucose_': 'glucose',
            'steps_': 'steps',
            'activity_': 'activity',
            'sleep_': 'sleep',
            'medication_': 'medications',
            'meal_': 'food',
            'nutrient_': 'food',
            'weight_': 'weight',
            'journey_': 'journey',
            'meditation_': 'mental_wellbeing',
            'journal_': 'mental_wellbeing',
            'action_plan_': 'mental_wellbeing',
            'app_': 'explore',
        }
        
        for prefix, category in category_mapping.items():
            if action_key.startswith(prefix):
                return category
        
        return 'explore'
    
    def get_eligible_positive_actions(
        self, 
        user: UserContext, 
        history: MessageHistory
    ) -> List[Dict[str, Any]]:
        """
        Get list of eligible positive actions based on user's data.
        
        This is the core logic that evaluates all conditions.
        """
        eligible = []
        thresholds_glucose = self.thresholds['glucose'][user.effective_a1c_group.value]
        
        # === GLUCOSE ACTIONS ===
        if user.has_cgm:
            # TIR met target
            if safe_gte(user.tir_pct, thresholds_glucose['tir_positive_threshold']):
                eligible.append({
                    'key': 'glucose_tir_met',
                    'category': 'glucose',
                    'data': {'tir_pct': round(user.tir_pct)}
                })
            
            # TIR improved
            if user.tir_prev_day is not None and safe_gt(user.tir_pct, user.tir_prev_day):
                eligible.append({
                    'key': 'glucose_tir_improved',
                    'category': 'glucose',
                    'data': {'tir_pct': round(user.tir_pct), 'prev_tir': round(user.tir_prev_day)}
                })
        
        # === STEPS ACTIONS ===
        if user.has_step_tracker:
            # Met 10,000 steps target
            if safe_gte(user.daily_step_count, self.thresholds['steps']['daily_target']):
                eligible.append({
                    'key': 'steps_target_met',
                    'category': 'steps',
                    'data': {'step_count': f"{int(user.daily_step_count):,}"}
                })
            
            # More steps than previous day
            elif user.prev_day_steps is not None and safe_gt(user.daily_step_count, user.prev_day_steps):
                prev_day_name = self._get_previous_day_name(user.report_date)
                eligible.append({
                    'key': 'steps_improved',
                    'category': 'steps',
                    'data': {'step_count': f"{int(user.daily_step_count):,}", 'prev_day_name': prev_day_name}
                })
        
        # === ACTIVITY ACTIONS ===
        if safe_gt(user.active_minutes, 0):
            eligible.append({
                'key': 'activity_logged',
                'category': 'activity',
                'data': {'active_minutes': int(user.active_minutes)}
            })
            
            # Maintained or improved (within tolerance)
            if user.prev_day_active_minutes is not None:
                tolerance = self.thresholds['activity']['daily_tolerance_minutes']
                if safe_gte(user.active_minutes, user.prev_day_active_minutes - tolerance):
                    eligible.append({
                        'key': 'activity_maintained',
                        'category': 'activity',
                        'data': {'active_minutes': int(user.active_minutes)}
                    })
        
        # Exercise video completion
        if safe_gte(user.exercise_video_completion_pct, self.thresholds['activity']['exercise_completion_pct']):
            eligible.append({
                'key': 'activity_video_completed',
                'category': 'activity',
                'data': {}
            })
        
        # === SLEEP ACTIONS ===
        # Met 7 hours
        if safe_gte(user.sleep_duration_hours, self.thresholds['sleep']['hours_target']):
            eligible.append({
                'key': 'sleep_hours_met',
                'category': 'sleep',
                'data': {'sleep_hours': round(user.sleep_duration_hours, 1)}
            })
        
        # More than previous day
        elif user.prev_day_sleep_hours is not None and safe_gt(user.sleep_duration_hours, user.prev_day_sleep_hours):
            eligible.append({
                'key': 'sleep_improved',
                'category': 'sleep',
                'data': {}
            })
        
        # Rating >= 7
        if safe_gte(user.sleep_rating, self.thresholds['sleep']['rating_target']):
            eligible.append({
                'key': 'sleep_rating_met',
                'category': 'sleep',
                'data': {'sleep_rating': int(user.sleep_rating)}
            })
        
        # === MEDICATION ACTIONS ===
        if user.has_medications and user.took_all_meds:
            eligible.append({
                'key': 'medication_adherence',
                'category': 'medications',
                'data': {}
            })
        
        # === FOOD ACTIONS ===
        if safe_gte(user.meals_logged_count, 1):
            eligible.append({
                'key': 'meal_logged',
                'category': 'food',
                'data': {'meal_type': user.last_meal_type or 'breakfast'}
            })
        
        if user.any_nutrient_target_met:
            eligible.append({
                'key': 'nutrient_target_met',
                'category': 'food',
                'data': {'nutrient_name': user.nutrient_name_met or 'nutrient'}
            })
        
        # === WEIGHT ACTIONS ===
        if user.has_weight_goal:
            # Check weight frequency cap
            can_show_weight = (
                history.weight_messages_this_week < self.message_history_rules['weight_max_per_week']
                and not history.weight_shown_yesterday
            )
            
            if can_show_weight:
                if user.weight_logged_yesterday:
                    eligible.append({
                        'key': 'weight_logged',
                        'category': 'weight',
                        'data': {}
                    })
                
                if user.weight_goal_type == 'lose' and safe_lt(user.weight_change_pct, 0):
                    eligible.append({
                        'key': 'weight_decreased',
                        'category': 'weight',
                        'data': {}
                    })
                
                if user.weight_goal_type == 'maintain' and user.is_within_maintenance_range:
                    eligible.append({
                        'key': 'weight_maintained',
                        'category': 'weight',
                        'data': {}
                    })
        
        # === JOURNEY ACTIONS ===
        if user.has_active_journey and user.journey_task_completed:
            eligible.append({
                'key': 'journey_task_completed',
                'category': 'journey',
                'data': {}
            })
        
        # === MENTAL WELL-BEING ACTIONS ===
        if user.meditation_opened_30d:
            eligible.append({
                'key': 'meditation_opened',
                'category': 'mental_wellbeing',
                'data': {}
            })
        
        if user.journal_entry_30d:
            eligible.append({
                'key': 'journal_entry',
                'category': 'mental_wellbeing',
                'data': {}
            })
        
        if user.action_plan_progress_30d:
            eligible.append({
                'key': 'action_plan_progress',
                'category': 'mental_wellbeing',
                'data': {}
            })
        
        # === FALLBACK: APP LOGIN ===
        if len(eligible) == 0:
            eligible.append({
                'key': 'app_login',
                'category': 'explore',
                'data': {}
            })
        
        return eligible
    
    def get_eligible_opportunities(
        self, 
        user: UserContext, 
        history: MessageHistory
    ) -> List[Dict[str, Any]]:
        """
        Get list of eligible opportunities (suggestions) based on user's data.
        """
        eligible = []
        thresholds_glucose = self.thresholds['glucose'][user.effective_a1c_group.value]
        
        # === WEIGHT OPPORTUNITIES ===
        if user.has_weight_goal:
            if safe_gt(user.days_since_last_weight, self.thresholds['weight']['log_prompt_days']):
                eligible.append({
                    'key': 'weight_log_prompt',
                    'category': 'weight',
                    'priority': 80 if user.has_weight_goal else 40
                })
        
        # === GLUCOSE OPPORTUNITIES ===
        if user.has_cgm:
            # TIR below target
            if safe_lt(user.tir_pct, thresholds_glucose['tir_opportunity_threshold']):
                eligible.append({
                    'key': 'glucose_improve_tir',
                    'category': 'glucose',
                    'priority': 90
                })
                
                # Suggest food logging if not logging
                if safe_lt(user.days_with_meals_7d, 1):
                    eligible.append({
                        'key': 'glucose_log_food',
                        'category': 'glucose',
                        'priority': 85
                    })
            
            # Too much time in high
            if safe_gt(user.glucose_high_pct, thresholds_glucose['high_time_max_pct']):
                eligible.append({
                    'key': 'glucose_high_protein_fiber',
                    'category': 'glucose',
                    'priority': 85
                })
                eligible.append({
                    'key': 'glucose_post_meal_walk',
                    'category': 'glucose',
                    'priority': 80
                })
                
                if not user.has_nutrient_goals:
                    eligible.append({
                        'key': 'glucose_set_carb_goal',
                        'category': 'glucose',
                        'priority': 75
                    })
            
            # Too much time in low
            if safe_gt(user.glucose_low_pct, thresholds_glucose['low_time_max_pct']):
                eligible.append({
                    'key': 'glucose_contact_provider',
                    'category': 'glucose',
                    'priority': 95  # Clinical priority
                })
        
        # === ACTIVITY OPPORTUNITIES ===
        if safe_lt(user.weekly_active_minutes, self.thresholds['activity']['weekly_target_minutes']):
            eligible.append({
                'key': 'activity_be_active',
                'category': 'activity',
                'priority': 70
            })
            
            if user.has_exercise_program:
                eligible.append({
                    'key': 'activity_exercise_program',
                    'category': 'activity',
                    'priority': 75
                })
        
        # === STEPS OPPORTUNITIES ===
        if user.has_step_tracker:
            # Calculate average steps (simplified - should come from Gold table)
            if safe_lt(user.daily_step_count, self.thresholds['steps']['weekly_avg_min']):
                eligible.append({
                    'key': 'steps_increase',
                    'category': 'steps',
                    'priority': 65
                })
        
        # === FOOD OPPORTUNITIES ===
        if safe_gte(user.meals_logged_count, 1):
            # Logged yesterday - encourage continuation
            eligible.append({
                'key': 'food_continue_logging',
                'category': 'food',
                'priority': 50
            })
        elif safe_eq(user.days_with_meals_7d, 0):
            # No logging in past week
            eligible.append({
                'key': 'food_start_logging',
                'category': 'food',
                'priority': 60
            })
        
        if user.has_nutrient_goals and not user.any_nutrient_target_met:
            eligible.append({
                'key': 'food_nutrient_attention',
                'category': 'food',
                'priority': 55
            })
        
        # === SLEEP OPPORTUNITIES ===
        if safe_gte(user.avg_sleep_hours_7d, self.thresholds['sleep']['hours_target']):
            if safe_gte(user.avg_sleep_rating_7d, self.thresholds['sleep']['rating_target']):
                eligible.append({
                    'key': 'sleep_continue',
                    'category': 'sleep',
                    'priority': 40
                })
        elif user.avg_sleep_hours_7d is not None:  # Has data but below target
            # Need more sleep - add random suggestion
            eligible.append({
                'key': 'sleep_improvement',
                'category': 'sleep',
                'priority': 70
            })
        elif user.sleep_duration_hours is None:  # No sleep logged
            eligible.append({
                'key': 'sleep_log_prompt',
                'category': 'sleep',
                'priority': 60
            })
        
        # === MEDICATION OPPORTUNITIES ===
        if user.has_medications:
            if safe_lt(user.med_adherence_7d_avg, self.thresholds['medication']['adherence_opportunity_pct']):
                if not user.med_reminders_enabled:
                    eligible.append({
                        'key': 'medication_reminders',
                        'category': 'medications',
                        'priority': 80
                    })
        
        # === MENTAL WELL-BEING OPPORTUNITIES ===
        if not user.meditation_opened_30d:
            eligible.append({
                'key': 'mental_try_meditation',
                'category': 'mental_wellbeing',
                'priority': 45
            })
        
        if not user.journal_entry_30d:
            eligible.append({
                'key': 'mental_journaling',
                'category': 'mental_wellbeing',
                'priority': 40
            })
        
        if not user.action_plan_progress_30d:
            eligible.append({
                'key': 'mental_action_plan',
                'category': 'mental_wellbeing',
                'priority': 45
            })
        
        # === FALLBACK: EXPLORE ===
        if len(eligible) == 0:
            eligible.append({
                'key': 'explore_browse',
                'category': 'explore',
                'priority': 10
            })
        
        return eligible
    
    def select_content(
        self, 
        user: UserContext, 
        history: MessageHistory
    ) -> SelectedContent:
        """
        Main selection method - applies all rules and returns final content.
        
        Args:
            user: User context with all feature data
            history: Message history for frequency capping
            
        Returns:
            SelectedContent with rating, greeting, actions, and opportunity
        """
        # Get rating and greeting
        print("  → Calculating daily rating...")
        try:
            rating_name, rating_desc = self.calculate_daily_rating(user)
            print(f"  ✓ Rating calculated: {rating_name}")
        except Exception as e:
            print(f"  ❌ Error in calculate_daily_rating: {e}")
            raise
        
        print("  → Getting greeting...")
        greeting = self.get_greeting()
        print(f"  ✓ Greeting: {greeting[:30]}...")
        
        # Get all eligible actions and opportunities
        print("  → Getting eligible positive actions...")
        try:
            all_actions = self.get_eligible_positive_actions(user, history)
            print(f"  ✓ Found {len(all_actions)} eligible actions")
        except Exception as e:
            print(f"  ❌ Error in get_eligible_positive_actions: {e}")
            raise
        
        print("  → Getting eligible opportunities...")
        try:
            all_opportunities = self.get_eligible_opportunities(user, history)
            print(f"  ✓ Found {len(all_opportunities)} eligible opportunities")
        except Exception as e:
            print(f"  ❌ Error in get_eligible_opportunities: {e}")
            raise
        
        # Filter by focus area
        filtered_actions = [
            a for a in all_actions 
            if self._is_category_allowed(a['category'], user)
        ]
        
        filtered_opportunities = [
            o for o in all_opportunities 
            if self._is_category_allowed(o['category'], user)
        ]
        
        # Calculate priorities for actions
        for action in filtered_actions:
            action['priority'] = self._calculate_action_priority(
                action['key'], user, history
            )
        
        # Sort by priority (higher = better).
        # Random jitter breaks ties so equal-priority categories rotate across days.
        filtered_actions.sort(
            key=lambda x: (x['priority'], random.random()), reverse=True
        )
        filtered_opportunities.sort(
            key=lambda x: (x.get('priority', 0), random.random()), reverse=True
        )
        
        # Special case: CGM with good TIR gets glucose + 1 additional action
        selected_actions = []
        if user.has_cgm:
            thresholds = self.thresholds['glucose'][user.effective_a1c_group.value]
            if safe_gte(user.tir_pct, thresholds['tir_positive_threshold']):
                # Find glucose action and add it
                glucose_actions = [a for a in filtered_actions if a['category'] == 'glucose']
                if glucose_actions:
                    selected_actions.append(glucose_actions[0])
                    # Remove glucose from remaining
                    filtered_actions = [a for a in filtered_actions if a['category'] != 'glucose']
        
        # Select remaining actions (up to max of 2 total)
        max_actions = self.config['message_constraints']['max_positive_actions']
        remaining_slots = max_actions - len(selected_actions)
        
        # Prioritize variety - don't pick from same category
        used_categories = {a['category'] for a in selected_actions}
        
        for action in filtered_actions:
            if len(selected_actions) >= max_actions:
                break
            
            # Prefer different categories for variety
            if action['category'] not in used_categories:
                selected_actions.append(action)
                used_categories.add(action['category'])
        
        # If still have slots, fill with any remaining
        if len(selected_actions) < max_actions:
            for action in filtered_actions:
                if len(selected_actions) >= max_actions:
                    break
                if action not in selected_actions:
                    selected_actions.append(action)
        
        # Select opportunity (just top 1)
        selected_opportunity = filtered_opportunities[0] if filtered_opportunities else {
            'key': 'explore_browse',
            'category': 'explore',
            'priority': 0
        }
        
        # Build final output with template text
        final_actions = []
        for action in selected_actions:
            template = self.positive_templates.get(action['key'], {})
            text_template = template.get('template', '')
            text = text_template.format(**action.get('data', {}))
            final_actions.append({
                'key': action['key'],
                'category': action['category'],
                'text': text,
                'data': action.get('data', {})
            })
        
        # Get opportunity text
        opp_template = self.opportunity_templates.get(selected_opportunity['key'], {})
        if isinstance(opp_template, dict):
            opp_text = opp_template.get('template', '')
        else:
            opp_text = str(opp_template)
        
        return SelectedContent(
            daily_rating=rating_name,
            rating_description=rating_desc,
            greeting=greeting,
            positive_actions=final_actions,
            opportunity={
                'key': selected_opportunity['key'],
                'category': selected_opportunity['category'],
                'text': opp_text
            }
        )
    
    def _get_previous_day_name(self, current_date: datetime) -> str:
        """Get the name of the previous day (e.g., 'Tuesday')."""
        prev_date = current_date - timedelta(days=1)
        return prev_date.strftime('%A')


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_user_context_from_gold(
    spark,
    patient_id: str,
    gold_table: str,
    report_date: datetime = None
) -> UserContext:
    """
    Load user context from Gold feature table.
    
    Args:
        spark: SparkSession
        patient_id: The patient ID to lookup
        gold_table: Full path to gold table
        report_date: Date to get features for (default: yesterday)
    
    Returns:
        UserContext populated with feature data
    """
    if report_date is None:
        report_date = datetime.now() - timedelta(days=1)
    
    report_date_str = report_date.strftime('%Y-%m-%d')
    
    # Query the Gold table
    query = f"""
    SELECT * FROM {gold_table}
    WHERE patientid = '{patient_id}'
    AND report_date = '{report_date_str}'
    """
    
    row = spark.sql(query).collect()
    
    if not row:
        # Return empty context
        return UserContext(
            patient_id=patient_id,
            report_date=report_date
        )
    
    row = row[0].asDict()
    
    # Map Gold table columns to UserContext
    return UserContext(
        patient_id=patient_id,
        report_date=report_date,
        
        has_cgm=bool(row.get('has_cgm_connected', False)),
        has_step_tracker=bool(row.get('has_step_tracker', False)),
        has_medications=bool((row.get('active_prescription_count') or 0) > 0),
        has_weight_goal=bool(row.get('has_weight_goal', False)),
        weight_goal_type=row.get('weight_goal_type'),
        
        tir_pct=row.get('tir_pct'),
        tir_prev_day=row.get('tir_pct_delta_1d'),
        glucose_high_pct=row.get('glucose_high_pct'),
        glucose_low_pct=row.get('glucose_low_pct'),
        
        daily_step_count=row.get('daily_step_count'),
        prev_day_steps=row.get('daily_step_count_delta_1d'),
        
        active_minutes=row.get('active_minutes'),
        prev_day_active_minutes=row.get('active_minutes_delta_1d'),
        weekly_active_minutes=row.get('active_minutes_7d_sum'),
        
        sleep_duration_hours=row.get('sleep_duration_hours'),
        sleep_rating=row.get('sleep_rating'),
        avg_sleep_hours_7d=row.get('sleep_duration_hours_avg_7d'),
        avg_sleep_rating_7d=row.get('sleep_rating_avg_7d'),
        
        weight_logged_yesterday=bool(row.get('weight_logged_today', False)),
        weight_change_pct=row.get('weight_change_pct'),
        days_since_last_weight=row.get('days_since_last_weight'),
        
        meals_logged_count=row.get('unique_meals_logged'),
        any_nutrient_target_met=bool(row.get('any_nutrient_target_met', False)),
        days_with_meals_7d=row.get('days_with_meals_7d'),
        
        took_all_meds=bool(row.get('took_all_meds', False)),
        med_adherence_7d_avg=row.get('med_adherence_7d_avg'),
        
        eligible_positive_actions=row.get('eligible_positive_actions', []),
        eligible_opportunities=row.get('eligible_opportunities', [])
    )


def load_message_history(
    spark,
    patient_id: str,
    history_table: str,
    lookback_days: int = 6
) -> MessageHistory:
    """
    Load message history for frequency capping.
    
    Args:
        spark: SparkSession
        patient_id: The patient ID
        history_table: Full path to message history table
        lookback_days: Number of days to look back
    
    Returns:
        MessageHistory with recent message data
    """
    from datetime import datetime, timedelta
    
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    
    # Query message history
    query = f"""
    SELECT 
        category,
        message_date,
        COUNT(*) as count
    FROM {history_table}
    WHERE patientid = '{patient_id}'
    AND message_date >= '{start_date}'
    GROUP BY category, message_date
    ORDER BY message_date DESC
    """
    
    try:
        rows = spark.sql(query).collect()
    except Exception:
        # Table might not exist yet
        return MessageHistory(patient_id=patient_id)
    
    categories_shown = list(set(row['category'] for row in rows))
    
    # Count weight messages this week
    week_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    weight_count = sum(1 for row in rows if row['category'] == 'weight' and row['message_date'] >= week_start)
    
    # Check if weight was shown yesterday
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    weight_yesterday = any(row['category'] == 'weight' and str(row['message_date']) == yesterday for row in rows)
    
    return MessageHistory(
        patient_id=patient_id,
        categories_shown_last_6d=categories_shown,
        weight_messages_this_week=weight_count,
        weight_shown_yesterday=weight_yesterday
    )
