from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from logic_engine import UserContext, MessageHistory, A1CTargetGroup


def get_full_table_name(table_config: dict) -> str:
    return f"{table_config['catalog']}.{table_config['schema']}.{table_config['table_name']}"


def get_user_profile(patient_id: str) -> Dict[str, Any]:
    """Fetch user profile information."""
    return {
        'patient_id': patient_id,
        'user_focus': None,
        'a1c_target_group': None
    }


def _derive_weight_logged_flags(features: Dict[str, Any]) -> Dict[str, bool]:
    """Derive weight_last_logged_7d/14d/30d from days_since_last_weight."""
    days = features.get('days_since_last_weight')
    return {
        'weight_last_logged_7d': days is not None and days <= 7,
        'weight_last_logged_14d': days is not None and days <= 14,
        'weight_last_logged_30d': days is not None and days <= 30,
    }


def _derive_nutrient_fields(features: Dict[str, Any]) -> Dict[str, Any]:
    """Derive nutrient scoring fields from individual *_target_pct columns."""
    # The Gold table has: protein_target_pct, carbs_target_pct, fat_target_pct,
    # calories_target_pct (% of goal, e.g. 95.0 means 95% of goal met).
    # Also: goal_protein, goal_carbs, goal_fat, goal_calories (the raw goals).
    nutrient_pcts = {}
    nutrient_names = ['protein', 'carbs', 'fat', 'calories']
    first_met_name = None

    for name in nutrient_names:
        pct = features.get(f'{name}_target_pct')
        has_goal = features.get(f'goal_{name}') is not None and (features.get(f'goal_{name}') or 0) > 0
        if has_goal:
            nutrient_pcts[name] = pct
            if first_met_name is None and pct is not None and 90 <= pct <= 110:
                first_met_name = name

    total = len(nutrient_pcts)
    count_90_110 = sum(1 for p in nutrient_pcts.values() if p is not None and 90 <= p <= 110)
    count_60_plus = sum(1 for p in nutrient_pcts.values() if p is not None and p >= 60)
    count_30_plus = sum(1 for p in nutrient_pcts.values() if p is not None and p >= 30)

    return {
        'has_nutrient_goals': total > 0,
        'total_nutrient_targets': total,
        'num_nutrient_targets_90_110': count_90_110,
        'num_nutrient_targets_60_plus': count_60_plus,
        'num_nutrient_targets_30_plus': count_30_plus,
        'nutrient_name_met': first_met_name,
    }


def build_user_context(features: Dict[str, Any], profile: Dict[str, Any]) -> UserContext:
    """Build UserContext object from features and profile data."""
    a1c_mapping = {
        'dm_target_7': A1CTargetGroup.DM_TARGET_7,
        'dm_target_8': A1CTargetGroup.DM_TARGET_8,
        'dip': A1CTargetGroup.DIP,
        'non_dm': A1CTargetGroup.NON_DM,
    }

    a1c_group = a1c_mapping.get(
        features.get('a1c_target_group'),
        None
    )

    weight_flags = _derive_weight_logged_flags(features)
    nutrient_fields = _derive_nutrient_fields(features)

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
        days_since_last_weight=features.get('days_since_last_weight'),
        weight_last_logged_7d=weight_flags['weight_last_logged_7d'],
        weight_last_logged_14d=weight_flags['weight_last_logged_14d'],
        weight_last_logged_30d=weight_flags['weight_last_logged_30d'],
        is_within_maintenance_range=bool(features.get('is_within_maintenance_range', False)),
        meals_logged_count=features.get('unique_meals_logged'),
        last_meal_type=features.get('last_meal_type'),
        any_nutrient_target_met=bool(features.get('any_nutrient_target_met', False)),
        total_nutrient_targets=nutrient_fields['total_nutrient_targets'],
        num_nutrient_targets_90_110=nutrient_fields['num_nutrient_targets_90_110'],
        num_nutrient_targets_60_plus=nutrient_fields['num_nutrient_targets_60_plus'],
        num_nutrient_targets_30_plus=nutrient_fields['num_nutrient_targets_30_plus'],
        nutrient_name_met=nutrient_fields['nutrient_name_met'],
        days_with_meals_7d=features.get('days_with_meals_7d'),
        has_nutrient_goals=nutrient_fields['has_nutrient_goals'],
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


def create_history_table(spark, history_table: str) -> None:
    """Create the message history table if it doesn't exist."""
    catalog, schema, _ = history_table.split(".")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    spark.sql(f"""
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
            created_at TIMESTAMP,
            last_modified_at TIMESTAMP,
            overwrite_count INT
        )
        USING DELTA
        PARTITIONED BY (message_date)
        COMMENT 'Message history for Metabolic Readiness feature with frequency capping'
    """)

    print(f"✓ Message history table created: {history_table}")


def get_message_history(spark, patient_id: str, history_table: str,
                        reference_date: datetime = None) -> MessageHistory:
    """Fetch message history for frequency capping.

    Args:
        reference_date: The "today" anchor (defaults to datetime.now()).
                        Weight uses a 7-day window; categories/keys use 6 days.
    """
    from collections import defaultdict

    history = MessageHistory(patient_id=patient_id)
    if reference_date is None:
        reference_date = datetime.now()

    try:
        # 7-day lookback captures both the 6-day category window and 7-day weight window
        lookback_date_7d = (reference_date - timedelta(days=7)).strftime('%Y-%m-%d')
        lookback_date_6d = (reference_date - timedelta(days=6)).strftime('%Y-%m-%d')
        yesterday = (reference_date - timedelta(days=1)).strftime('%Y-%m-%d')

        query = f"""
        SELECT category, message_date, positive_actions_used, opportunity_used
        FROM {history_table}
        WHERE patientid = '{patient_id}'
        AND message_date >= '{lookback_date_7d}'
        """

        rows = spark.sql(query).collect()

        # categories_shown_last_6d — only within the 6-day window (all appearances,
        # positive actions AND opportunities, per clinical decision Q2-B).
        history.categories_shown_last_6d = list(set(
            row['category'] for row in rows
            if str(row['message_date']) >= lookback_date_6d
        ))

        # keys_shown_last_6d — specific opportunity keys within 6-day window
        keys = set()
        for row in rows:
            if str(row['message_date']) >= lookback_date_6d:
                opp = row['opportunity_used']
                if opp:
                    keys.add(opp)
        history.keys_shown_last_6d = list(keys)

        # weight_messages_this_week — count ONLY goal-progress messages (weight_decreased /
        # weight_maintained). The simple weight_logged acknowledgment is exempt from the
        # 2×/week cap (clinical decision Q1-B).
        WEIGHT_GOAL_PROGRESS_KEYS = {'weight_decreased', 'weight_maintained'}
        history.weight_messages_this_week = sum(
            1 for row in rows
            if row['category'] == 'weight'
            and any(k in WEIGHT_GOAL_PROGRESS_KEYS for k in (row['positive_actions_used'] or []))
        )

        # weight_shown_yesterday — same filter: only goal-progress messages count.
        history.weight_shown_yesterday = any(
            row['category'] == 'weight'
            and str(row['message_date']) == yesterday
            and any(k in WEIGHT_GOAL_PROGRESS_KEYS for k in (row['positive_actions_used'] or []))
            for row in rows
        )

        # category_streaks — consecutive days ending at yesterday
        category_dates: Dict[str, set] = defaultdict(set)
        for row in rows:
            category_dates[row['category']].add(str(row['message_date']))

        yesterday_dt = reference_date - timedelta(days=1)
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

    except Exception as e:
        print(f"Info: Could not load message history (table may not exist yet): {e}")

    return history


def _extract_categories_from_actions(action_keys: List[str]) -> set:
    categories = set()

    category_mapping = {
        'glucose': ['glucose_'],
        'steps': ['steps_'],
        'activity': ['activity_', 'exercise_'],
        'sleep': ['sleep_'],
        'weight': ['weight_'],
        'food': ['meal_', 'nutrient_', 'food_'],
        'medications': ['medication_', 'med_'],
        'mental_wellbeing': ['mental_', 'meditation_', 'journal_'],
        'journey': ['journey_'],
        'explore': ['explore_']
    }

    for action_key in action_keys:
        for category, prefixes in category_mapping.items():
            if any(action_key.startswith(prefix) for prefix in prefixes):
                categories.add(category)
                break

    return categories


def write_patient_history(spark, patient_id: str, message_date: str, result: Dict[str, Any], history_table: str) -> None:
    """Write message history for a single patient using a DataFrame MERGE (no string-interpolated SQL)."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

    history_schema = StructType([
        StructField("patientid",             StringType(),            True),
        StructField("category",              StringType(),            True),
        StructField("message_date",          StringType(),            True),
        StructField("message_text",          StringType(),            True),
        StructField("rating",                StringType(),            True),
        StructField("rating_description",    StringType(),            True),
        StructField("positive_actions_used", ArrayType(StringType()), True),
        StructField("opportunity_used",      StringType(),            True),
        StructField("character_count",       IntegerType(),           True),
        StructField("word_count",            IntegerType(),           True),
    ])

    try:
        action_keys = result.get("positive_actions_used", [])
        opp_key     = result.get("opportunity_used", "")
        categories  = _extract_categories_from_actions(action_keys + ([opp_key] if opp_key else []))

        rows = [
            {
                "patientid"            : patient_id,
                "category"             : category,
                "message_date"         : message_date,
                "message_text"         : result.get("message", result.get("insight", "")),
                "rating"               : result.get("rating", result.get("score_name", "")),
                "rating_description"   : result.get("rating_description", ""),
                "positive_actions_used": action_keys,
                "opportunity_used"     : opp_key,
                "character_count"      : result.get("character_count", 0),
                "word_count"           : result.get("word_count", 0),
            }
            for category in categories
        ]

        if not rows:
            return

        spark.createDataFrame(rows, schema=history_schema).createOrReplaceTempView("tmp_patient_history_write")

        spark.sql(f"""
            MERGE INTO {history_table} AS target
            USING tmp_patient_history_write AS source
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
                    target.last_modified_at      = current_timestamp(),
                    target.overwrite_count       = coalesce(target.overwrite_count, 0) + 1
            WHEN NOT MATCHED THEN
                INSERT (patientid, category, message_date, message_text, rating,
                        rating_description, positive_actions_used, opportunity_used,
                        character_count, word_count, created_at, last_modified_at, overwrite_count)
                VALUES (source.patientid, source.category, source.message_date,
                        source.message_text, source.rating, source.rating_description,
                        source.positive_actions_used, source.opportunity_used,
                        source.character_count, source.word_count,
                        current_timestamp(), current_timestamp(), 0)
        """)

    except Exception as e:
        print(f"Warning: Could not write message history: {e}")
