# SIMON Health Habits - Personalized Messaging Feature Store

## Table of Contents
1. [Project Overview](#project-overview)
2. [System Architecture](#system-architecture)
3. [Data Sources](#data-sources)
4. [Feature Engineering](#feature-engineering)
5. [Health Management Scoring System](#health-management-scoring-system)
6. [Message Generation Pipeline](#message-generation-pipeline)
7. [Databricks Deployment](#databricks-deployment)
8. [Configuration Management](#configuration-management)
9. [Usage Examples](#usage-examples)
10. [Development Guide](#development-guide)
11. [Troubleshooting](#troubleshooting)

---

## Project Overview

### Purpose
The SIMON Health Habits Personalized Messaging System generates daily, personalized health insights for users based on their health data. The system combines deterministic business logic with LLM-based natural language generation to deliver concise, actionable messages (approximately 250 words) that acknowledge achievements and suggest opportunities.

### Key Features
- **Logic-First, LLM-Second Architecture**: Python business logic selects content; LLM generates natural language
- **10-Category Health Scoring**: Comprehensive scoring across weight, glucose, activity, steps, food, nutrients, sleep, medications, and mental well-being (0-50 base points + bonus)
- **Focus Area Filtering**: Only includes relevant health categories based on user's selected focus
- **Priority-Based Content Selection**: Applies complex rules for message hierarchy and variety
- **Frequency Capping**: Prevents message repetition (6-day lookback, 3-day streak detection)
- **Clinical Rigor**: Implements clinical thresholds for A1C targets, TIR ranges, and adherence metrics
- **Batch Processing & Real-Time Serving**: Supports both batch generation and API-based serving via MLflow Model Serving

### Technology Stack
- **Platform**: Databricks (Azure)
- **Processing Engine**: PySpark (Delta Live Tables)
- **LLM Model**: Meta Llama 3.3 70B Instruct (Databricks Foundation Models)
- **Storage**: Unity Catalog (Delta Lake)
- **Serving**: MLflow Model Serving
- **Configuration**: YAML (prompts.yml)

---

## System Architecture

### High-Level Flow
```
Bronze/Silver Tables → Gold Feature Table → Logic Engine → Insight Generator → Message Storage
                     (Feature Engineering)  (Content Selection)  (LLM Generation)    (History)
```

### Architecture Pattern: Logic-First, LLM-Second

**Why This Design?**
- **Deterministic Control**: Clinical business rules are explicitly coded, not subject to LLM variation
- **Auditability**: All content selection decisions are traceable through Python code
- **Cost Efficiency**: LLM only used for final language generation, not decision-making
- **Consistency**: Same inputs always produce same content selection (LLM only varies phrasing)

**Two-Stage Process:**

#### Stage 1: Logic Engine (Deterministic)
- Calculates 10-category health score with focus area filtering
- Evaluates eligibility for 25+ positive action types
- Evaluates eligibility for 20+ opportunity suggestions
- Applies priority hierarchy (clinical urgency → positive reinforcement → long-term goals)
- Applies frequency caps and variety rules
- Selects final content: 1 daily rating + 2 positive actions + 1 opportunity

#### Stage 2: Insight Generator (LLM)
- Receives selected facts as structured input
- Generates natural language with clinical tone
- Aims for approximately 250 words (advisory, not enforced)

### Component Structure

```
Metablic_Readiness/
├── prompts.yml                 # Central configuration (thresholds, prompts, scoring)
├── logic_engine.py            # Deterministic business logic
├── insight_generator.py       # LLM integration for natural language generation
├── main_pipeline.py           # Orchestration (batch + API serving)
├── PROJECT_DOCUMENTATION.md   # This comprehensive documentation
├── Feature_store_Creation/
│   ├── notebook.py            # Feature engineering (Bronze → Gold)
│   └── Scoring_Criteria.csv   # Source of truth for scoring rules
├── simon/                     # Python 3.12 virtual environment
└── databricks.yml             # Databricks sync configuration
```

---

## Data Sources

### Databricks Environment
- **Workspace**: adb-2008955168844352.12.azuredatabricks.net
- **Sync Path**: `/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Mealbolic_Readiness`
- **Catalogs**: 
  - `bronz_als_azdev24` (Development)
  - `bronz_als_azuat2` (UAT)

### Bronze/Silver Tables

#### 1. elogbgentry (Glucose Readings)
```sql
-- Schema
patient_id, entrydate, glucosevalue, timezoneoffset, ...

-- Key Metrics
- Time in Range (TIR) calculation
- High blood glucose % (>180 mg/dL)
- Low blood glucose % (<70 mg/dL)
```

#### 2. elogexerciseentry (Activity Minutes)
**Note**: Activity and Steps are SEPARATE in Bronze schema
```sql
-- Schema
patient_id, entrydate, durationminutes, activitytype, ...

-- Key Metrics
- Daily active minutes
- 7-day total active minutes (weekly_active_minutes)
- Day-over-day change
```

#### 3. stepentry (Step Counts)
```sql
-- Schema
patient_id, entrydate, stepcount, ...

-- Key Metrics
- Daily step count
- 7-day average steps
- Day-over-day change
```

#### 4. elogweightentry (Weight Logs)
```sql
-- Schema
patient_id, entrydate, weightvalue, ...

-- Key Metrics
- 14-day weight change (lbs and %)
- Maintenance range evaluation (+/- 3%)
- Last logged date (7d, 14d, 30d flags)
```

#### 5. foodmoduleitem (Food Logging)
```sql
-- Schema
patient_id, entrydate, mealtype, itemname, protein, carbs, fat, calories, ...

-- Key Metrics
- Meals logged per day (count)
- Nutrient totals vs. goals (% of target)
- 7-day meal logging frequency
```

#### 6. sleepentry (Sleep Tracking)
```sql
-- Schema
patient_id, entrydate, durationhours, rating, ...

-- Key Metrics
- Sleep duration (hours)
- Sleep rating (0-10 scale)
- 7-day averages
```

#### 7. medadministration + medprescription (Medications)
```sql
-- Schema
patient_id, administrationdate, prescriptionid, taken_flag, ...

-- Key Metrics
- Daily adherence (took all prescribed)
- 7-day adherence % (avg)
```

#### 8. patientgoaldetails (Goals & Focus)
```sql
-- Schema
patient_id, goal_type, target_value, focus_area, ...

-- Key Metrics
- Weight goal type (lose, maintain, gain)
- Nutrient goals (protein, carbs, fat, calories)
- User focus area (Weight, Glucose, Activity, etc.)
```

### PLACEHOLDER Tables (Not Yet Implemented)
These tables are referenced in bonus scoring but not currently available:
- **content_interaction**: Article reads, lesson completions, video watches
- **ecommerce_activity**: Grocery shopping online events
- **program_enrollment**: Exercise program starts
- **video_progress**: Exercise video completion %
- **journey_tasks**: Detailed journey task tracking

---

## Feature Engineering

### Gold Feature Table Design

The Gold layer feature table is created by `notebook.py` and contains **yesterday's aggregated features** for all users, updated daily.

#### Key Design Principles
1. **Timezone-Aware Aggregations**: Uses `timezoneoffset` to ensure "yesterday" is user's local day
2. **Window Functions**: 7-day rolling metrics, lag functions for day-over-day comparisons
3. **Pre-Calculated Eligibility**: Eligibility arrays generated in SQL for efficiency
4. **Full Outer Joins**: Ensures all patients included even if missing some data categories

### Feature Categories in Gold Table

#### Glucose Features (from elogbgentry)
```python
tir_pct                    # Time in Range % for yesterday
tir_prev_day              # TIR for day before yesterday
glucose_high_pct          # % readings > 180 mg/dL
glucose_low_pct           # % readings < 70 mg/dL
avg_tir_7d                # 7-day rolling average TIR
```

#### Activity Features (from elogexerciseentry)
```python
active_minutes            # Total minutes yesterday
prev_day_active_minutes   # Total minutes day before
weekly_active_minutes     # Sum of last 7 days (for scoring)
avg_active_minutes_7d     # 7-day rolling average
```

#### Steps Features (from stepentry)
```python
daily_step_count          # Steps yesterday
prev_day_steps            # Steps day before
avg_steps_7d              # 7-day rolling average
```

#### Weight Features (from elogweightentry)
```python
weight_logged_yesterday   # Boolean flag
weight_change_lbs_14d     # Absolute change in lbs (14-day lookback)
weight_change_pct_14d     # Percentage change (14-day lookback)
weight_last_logged_7d     # Logged in last 7 days
weight_last_logged_14d    # Logged in last 14 days
weight_last_logged_30d    # Logged in last 30 days
is_within_maintenance_range  # Within +/- 3% of goal
```

#### Food Features (from foodmoduleitem + patientgoaldetails)
```python
meals_logged_count        # Number of meals logged yesterday
days_with_meals_7d        # Count of days with any meal in last 7 days
last_meal_type            # Type of last meal (breakfast, lunch, dinner, snack)

# Nutrient target percentages (yesterday's total / daily goal)
protein_target_met_pct    # % of protein goal met (0-150+)
carbs_target_met_pct      # % of carbs goal met
fat_target_met_pct        # % of fat goal met
calories_target_met_pct   # % of calories goal met

# Nutrient scoring helpers
num_nutrient_targets_90_110  # Count of nutrients in 90-110% range
num_nutrient_targets_60_plus # Count of nutrients >= 60%
num_nutrient_targets_30_plus # Count of nutrients >= 30%
total_nutrient_targets       # Total number of goals set
```

#### Sleep Features (from sleepentry)
```python
sleep_duration_hours      # Hours slept yesterday
prev_day_sleep_hours      # Hours slept day before
sleep_rating              # User rating (0-10) yesterday
prev_day_sleep_rating     # Rating day before
avg_sleep_hours_7d        # 7-day rolling average
avg_sleep_rating_7d       # 7-day rolling average
```

#### Medication Features (from medadministration + medprescription)
```python
took_all_meds             # Boolean: took 100% of prescribed meds yesterday
med_adherence_7d_avg      # 7-day adherence % (0.0-1.0)
```

#### Mental Well-Being Features
```python
action_plan_active        # Has active action plan
journal_entry_7d          # Journaled at least once in last 7 days
meditation_opened_7d      # Opened meditation in last 7 days
```

#### Bonus Activity Features (PLACEHOLDERS)
```python
bonus_exercise_video_completed  # Completed exercise video
bonus_ai_meal_plan              # Generated AI meal plan
bonus_exercise_program_started  # Started exercise program
bonus_grocery_online            # PLACEHOLDER: Requires ecommerce_activity table
bonus_article_read              # PLACEHOLDER: Requires content_interaction table
bonus_lesson_completed          # PLACEHOLDER: Requires content_interaction table
bonus_video_watched             # PLACEHOLDER: Requires content_interaction table
```

#### Profile Flags (from patientgoaldetails + device registrations)
```python
has_cgm                   # Has CGM device connected
has_step_tracker          # Has step tracker connected
has_medications           # Has medication prescriptions
has_weight_goal           # Has weight loss/maintenance goal
weight_goal_type          # "lose", "maintain", "gain"
has_nutrient_goals        # Has nutrient targets set
user_focus                # "Weight", "Glucose", "Activity", "Eating Habits", "Sleep", "Medications", "Anxiety"
a1c_target_group          # "a1c_target_7", "a1c_target_8", "dip", "non_dm"
```

### SQL Example: Activity Minutes with 7-Day Total
```sql
SELECT 
    patient_id,
    entrydate,
    durationminutes as active_minutes,
    LAG(durationminutes, 1) OVER (PARTITION BY patient_id ORDER BY entrydate) as prev_day_active_minutes,
    SUM(durationminutes) OVER (
        PARTITION BY patient_id 
        ORDER BY entrydate 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as weekly_active_minutes_7d,
    AVG(durationminutes) OVER (
        PARTITION BY patient_id 
        ORDER BY entrydate 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as avg_active_minutes_7d
FROM bronz_als_azdev24.elogexerciseentry
WHERE entrydate >= current_date() - INTERVAL 30 DAYS
```

---

## Health Management Scoring System

### Overview
**Total Possible**: 50 base points (10 categories × 5 points each) + up to 10 bonus points

**Scoring Philosophy**:
- **Focus Area Filtering**: Only score categories relevant to user's selected focus
- **No Focus Set**: Include all 10 categories if user hasn't selected a focus
- **Device Availability**: Skip categories requiring unavailable devices (CGM, step tracker)
- **Tiered Scoring**: Most categories use 3-5 tiers (0, 1, 3, 5 points)

### Scoring Categories

#### Category 1: Weight (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Weight" is a focus area

**Scoring Rules**:

**Loss Goal**:
- 5 points: Decreased by ≥1 lb in last 14 days
- 3 points: Stayed within ±1 lb in last 14 days
- 0 points: Gained ≥1 lb in last 14 days

**Maintenance Goal**:
- 5 points: Stayed within ±3% of goal weight in last 14 days
- 0 points: Outside ±3% range

**No Goal**:
- 5 points: Logged once in last 7 days
- 3 points: Logged once in last 14 days
- 1 point: Logged once in last 30 days
- 0 points: Not logged

---

#### Category 2: Glucose/CGM (5 points)
**Inclusion Criteria**:
- Include if user has CGM connected

**Scoring Rules**:

**A1C Target <7% (Diabetes Management)**:
- 5 points: TIR ≥70% yesterday
- 3 points: TIR ≥60% yesterday
- 1 point: TIR ≤50% yesterday

**A1C Target <8% (Diabetes Management)**:
- 5 points: TIR ≥50% yesterday
- 3 points: TIR ≥40% yesterday
- 1 point: TIR ≤30% yesterday

*Note: DIP and non-DM use same thresholds as <7% target*

---

#### Category 3: Activity (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Activity" is a focus area

**Scoring Rules** (based on 7-day total):
- 5 points: ≥150 minutes in last 7 days
- 4 points: ≥120 minutes in last 7 days
- 3 points: ≥90 minutes in last 7 days
- 2 points: ≥60 minutes in last 7 days
- 1 point: ≥30 minutes in last 7 days
- 0 points: No activity logged

---

#### Category 4: Steps (5 points)
**Inclusion Criteria**:
- Include if user has step tracker connected
- If user has focus set, include if "Activity" is a focus area

**Scoring Rules** (based on yesterday):
- 5 points: ≥10,000 steps
- 3 points: ≥6,000 steps
- 1 point: ≥2,000 steps
- 0 points: No steps recorded

---

#### Category 5: Food Logging (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Eating Habits" is a focus area

**Scoring Rules** (based on yesterday):
- 5 points: Logged ≥3 meals
- 3 points: Logged ≥2 meals
- 1 point: Logged ≥1 meal
- 0 points: No food logged

---

#### Category 6: Daily Nutrient Targets (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Eating Habits" is a focus area
- Only included if user has nutrient goals set

**Scoring Rules** (based on yesterday):
- 5 points: Met 90-110% of ALL nutrient targets (protein, carbs, fat, calories)
- 3 points: Met ≥60% of ALL nutrient targets
- 1 point: Met ≥30% of ALL nutrient targets
- 0 points: No targets set or none met

---

#### Category 7: Sleep Duration (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Sleep" is a focus area

**Scoring Rules** (based on yesterday):
- 5 points: ≥7 hours slept
- 3 points: ≥6 hours slept
- 1 point: ≥5 hours slept
- 0 points: <5 hours or no sleep logged

---

#### Category 8: Sleep Rating (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Sleep" is a focus area

**Scoring Rules** (based on yesterday):
- 5 points: Rating of 10
- 3 points: Rating ≥7
- 1 point: Rating ≥4
- 0 points: Rating ≤3 or no rating logged

---

#### Category 9: Medications (5 points)
**Inclusion Criteria**:
- Include if user has medications prescribed
- If user has focus set, include if "Medications" is a focus area

**Scoring Rules** (based on 7-day adherence):
- 5 points: 100% adherence in last 7 days
- 3 points: ≥75% adherence in last 7 days
- 1 point: ≥50% adherence in last 7 days
- 0 points: <50% adherence

---

#### Category 10: Mental Well-Being (5 points)
**Inclusion Criteria**:
- Include if user has no focus set
- If user has focus set, include if "Anxiety" is a focus area

**Scoring Rules** (CUMULATIVE, max 5 points):
- 3 points: Has an active Action Plan
- 1 point: Journaled ≥1 time in last 7 days
- 1 point: Opened meditation ≥1 time in last 7 days

---

### Bonus Points (Up to 10 points)
Not subject to focus area filtering - always included if activity occurred:

- **+3 points**: Completed an exercise video
- **+1 point**: Generated an AI meal plan
- **+1 point**: Started an exercise program
- **+1 point**: Shopped for groceries online (PLACEHOLDER)
- **+1 point**: Read an article (PLACEHOLDER)
- **+1 point**: Completed a lesson (PLACEHOLDER)
- **+1 point**: Watched a Learn video (PLACEHOLDER)

---

### Rating Tier Mapping
```
Percentage Range → Daily Rating
---------------------------------------------
81-100%          → Committed  ("You are committed to your healthy habits and seeing results.")
61-80%           → Strong     ("Your healthy habits are solid and dependable.")
41-60%           → Consistent ("You are making a great effort to make your healthy habits a routine.")
21-40%           → Building   ("You are working to create new healthy habits.")
1-20%            → Ready      ("You are ready to choose your next healthy steps.")
```

**Percentage Calculation**:
```python
percentage = (achieved_score / max_available_score) * 100
```

**max_available_score** = sum of max points for all included categories (not always 50!)

**Example**:
- User focus: "Weight" only
- Categories included: Weight (5 pts)
- User achieved: 3 points (stable within ±1 lb)
- Percentage: (3 / 5) * 100 = 60% → "Consistent" rating

---

## Message Generation Pipeline

### Logic Engine (logic_engine.py)

#### UserContext Dataclass
Contains all user profile and feature data:
```python
@dataclass
class UserContext:
    patient_id: str
    report_date: datetime
    
    # Profile flags
    has_cgm: bool
    has_step_tracker: bool
    has_medications: bool
    user_focus: str  # "Weight", "Glucose", etc.
    
    # Yesterday's features (50+ fields from Gold table)
    tir_pct: float
    daily_step_count: int
    active_minutes: float
    weekly_active_minutes: float  # 7-day total for scoring
    # ... (see full dataclass in code)
```

#### Core Methods

**1. calculate_daily_rating(user: UserContext) → Tuple[str, str]**
- Implements 10-category scoring with focus area filtering
- Returns: (rating_name, rating_description)
- See "Health Management Scoring System" section for detailed logic

**2. get_eligible_positive_actions(user: UserContext, history: MessageHistory) → List[str]**
Evaluates 25+ positive action types:

**Glucose Actions**:
- `glucose_tir_high`: TIR improved and above target
- `glucose_tir_improved`: TIR improved but not above target
- `glucose_tir_maintained`: TIR maintained above target
- `glucose_no_low`: No low readings

**Activity Actions**:
- `activity_goal_met`: Met weekly activity goal (150+ min)
- `activity_increased`: Increased from previous day
- `activity_maintained`: Maintained active minutes
- `steps_goal_met`: Met steps goal (10,000+)
- `steps_increased`: Increased from previous day

**Weight Actions**:
- `weight_lost`: Lost weight (for loss goal)
- `weight_maintained`: Maintained weight (for maintenance goal)
- `weight_logged`: Logged weight

**Food Actions**:
- `meals_logged_all`: Logged all 3+ meals
- `nutrient_target_met`: Met specific nutrient target
- `healthy_pattern`: 7-day meal logging pattern

**Sleep Actions**:
- `sleep_duration_met`: Met 7+ hour target
- `sleep_quality_high`: Sleep rating 8+
- `sleep_improved`: Duration or rating improved

**Medication Actions**:
- `meds_perfect_7d`: 100% adherence for 7 days
- `meds_taken_yesterday`: Took all meds yesterday

**Mental Wellbeing Actions**:
- `journal_entry`: Journaled yesterday
- `meditation_completed`: Meditation session completed
- `action_plan_progress`: Made progress on action plan

**Journey Actions**:
- `journey_task_completed`: Completed journey task

**3. get_eligible_opportunities(user: UserContext, history: MessageHistory) → List[str]**
Evaluates 20+ opportunity types:

**Clinical Urgency (Priority 1)**:
- `glucose_low_readings`: Had low glucose readings
- `glucose_high_readings`: Had extended high readings
- `meds_missed_consecutive`: Missed meds 2+ consecutive days

**Positive Reinforcement (Priority 2)**:
- `activity_increase_opportunity`: Can increase activity
- `steps_increase_opportunity`: Can increase steps
- `meal_logging_opportunity`: Can log more meals
- `sleep_improvement_opportunity`: Can improve sleep duration/quality

**Long-Term Goals (Priority 3)**:
- `journey_next_task`: Next journey task available
- `exercise_program_suggestion`: Exercise program recommendation
- `weight_goal_tracking`: Weight goal check-in

**4. select_content(user: UserContext, history: MessageHistory) → SelectedContent**
Main orchestration method applying all rules:

**Priority Hierarchy**:
1. **Clinical Urgency**: Glucose lows, extended highs, medication gaps
2. **Positive Reinforcement**: Acknowledge progress, suggest improvements
3. **Long-Term Goals**: Journey tasks, behavior change programs

**Variety & Frequency Rules**:
- **6-Day Lookback**: Check `categories_shown_last_6d` to avoid repetition
- **3-Day Streak Prevention**: Don't show same category 3+ consecutive days
- **Weight Special Rules**: Max 2 weight messages per week, never 2 consecutive days
- **Balanced Selection**: Select 2 positive actions from different categories when possible
- **Single Opportunity**: Select 1 opportunity (highest priority)

**Greeting Selection**:
- Time-based: "Good morning" (5-11am), "Good afternoon" (12-5pm), "Good evening" (6-10pm)

**Output Example**:
```python
SelectedContent(
    daily_rating="Strong",
    rating_description="Your healthy habits are solid and dependable.",
    greeting="Good morning.",
    positive_actions=[
        {"type": "glucose_tir_high", "value": 78, "improvement": 5},
        {"type": "activity_goal_met", "minutes": 165}
    ],
    opportunity={
        "type": "meal_logging_opportunity", 
        "meals_logged": 1, 
        "meals_target": 3
    }
)
```

---

### Insight Generator (insight_generator.py)

#### LLM Integration Architecture

**Model**: `databricks-meta-llama-3-3-70b-instruct` (Databricks Foundation Models)

**Configuration** (from prompts.yml):
```yaml
model:
  endpoint_name: "llama-3-3-70b"
  temperature: 0.7
  max_tokens: 100
  top_p: 0.9
```

#### Core Methods

**1. generate_insight(selected_content: SelectedContent, user: UserContext) → str**
Main entry point:
1. Assembles prompt from templates
2. Calls LLM API
3. Returns final message

**2. _call_llm(prompt: str) → str**
Databricks SDK integration:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name=self.endpoint_name,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt}
    ],
    temperature=self.temperature,
    max_tokens=self.max_tokens
)
return response.choices[0].message.content
```

**3. _validate_length(message: str) → bool**
```python
length = len(message)
return 200 <= length <= 220
```

**4. _correct_length(message: str, target_length: int = 210) → str**
Iterative retry logic:
- If too long: Ask LLM to shorten to target
- If too short: Ask LLM to expand to target
- Max 3 correction attempts

#### Prompt Assembly

**System Prompt** (from prompts.yml):
```yaml
system_prompt: |
  You are a health coach assistant generating personalized daily health insights.
  
  Guidelines:
  - Use a warm, encouraging, non-judgmental tone
  - Be concise and specific with numbers
  - Focus on progress and actionable next steps
  - Avoid medical advice or diagnosis
  - Use "you" and "your" (second person)
  - Aim for approximately 250 words
```

**User Prompt Structure**:
```
Date: {report_date}
Daily Rating: {rating_name} - {rating_description}

Positive Actions:
- {action_1_template with values}
- {action_2_template with values}

Opportunity:
- {opportunity_template with values}

Generate a personalized message that:
1. Starts with "{greeting}"
2. Acknowledges the positive actions with specific numbers
3. Suggests the opportunity in an encouraging way
4. Is approximately 250 words
```

**Example Assembled Prompt**:
```
Date: 2024-01-15
Daily Rating: Strong - Your healthy habits are solid and dependable.

Positive Actions:
- Your blood sugar was in range 78% of the time yesterday, up 5% from the day before.
- You completed 165 minutes of activity this week, exceeding your 150-minute goal.

Opportunity:
- You logged 1 meal yesterday. Try logging all 3 meals today to track your nutrition more completely.

Generate a personalized message...
```

---

## Databricks Deployment

### Batch Processing (main_pipeline.py)

#### Pipeline Configuration
```python
PIPELINE_CONFIG = {
    'gold_table': 'bronz_als_azdev24.gold_feature_table',
    'history_table': 'bronz_als_azdev24.message_history',
    'user_profile_table': 'bronz_als_azdev24.patient_profiles',
    'output_table': 'bronz_als_azdev24.generated_messages'
}
```

#### run_batch_generation()
```python
def run_batch_generation(report_date: str):
    """
    Generate messages for all active users for a given date.
    
    Args:
        report_date: Date in 'YYYY-MM-DD' format
    
    Process:
        1. Load Gold features for report_date
        2. Load message history (last 6 days)
        3. Load user profiles
        4. For each user:
            - Build UserContext
            - Run Logic Engine → select_content()
            - Run Insight Generator → generate_insight()
            - Log result to output_table
        5. Update message_history table
    """
```

**Spark Optimization**:
- Broadcast small tables (profiles, history)
- Repartition Gold table by patient_id
- Use UDF for Python logic engine
- Batch LLM calls (avoid per-row API calls)

#### Databricks Job Schedule
```yaml
# Recommended schedule: Run daily at 6 AM user local time
schedule:
  quartz_cron_expression: "0 0 6 * * ?"
  timezone_id: "America/New_York"
  
cluster:
  node_type_id: "Standard_DS3_v2"
  num_workers: 2
  spark_version: "13.3.x-scala2.12"
  
libraries:
  - pypi:
      package: "databricks-sdk>=0.12.0"
  - pypi:
      package: "pyyaml>=6.0"
```

---

### Model Serving API

#### MLflow Model Creation
```python
class InsightGeneratorMLflow(mlflow.pyfunc.PythonModel):
    """MLflow wrapper for real-time serving."""
    
    def load_context(self, context):
        self.logic_engine = LogicEngine(context.artifacts['config'])
        self.generator = InsightGenerator(context.artifacts['config'])
    
    def predict(self, context, model_input: pd.DataFrame):
        """
        Input DataFrame columns:
            - patient_id
            - report_date
            - All Gold feature columns
        
        Returns:
            DataFrame with columns: patient_id, message, rating, timestamp
        """
```

#### Model Registration
```python
import mlflow

with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="insight_generator",
        python_model=InsightGeneratorMLflow(),
        artifacts={
            'config': 'prompts.yml'
        },
        pip_requirements=[
            'databricks-sdk>=0.12.0',
            'pyyaml>=6.0'
        ],
        registered_model_name="simon_insight_generator"
    )
```

#### Serving Endpoint Setup
```bash
# Create endpoint (REST API)
databricks model-serving create \
  --name simon-insights \
  --model-name simon_insight_generator \
  --model-version 1 \
  --workload-size Small \
  --scale-to-zero-enabled true
```

#### API Usage Example
```python
import requests

endpoint = "https://adb-2008955168844352.12.azuredatabricks.net/serving-endpoints/simon-insights/invocations"
token = "dapi..."

payload = {
    "dataframe_records": [
        {
            "patient_id": "12345",
            "report_date": "2024-01-15",
            "tir_pct": 78,
            "daily_step_count": 8500,
            "active_minutes": 35,
            "weekly_active_minutes": 165,
            # ... all features
        }
    ]
}

response = requests.post(
    endpoint,
    json=payload,
    headers={"Authorization": f"Bearer {token}"}
)

print(response.json())
# {"predictions": ["Good morning. Your blood sugar was in range 78% yesterday, up 5%. You hit 165 active minutes this week. Try logging all 3 meals today."]}
```

---

## Configuration Management

### prompts.yml Structure

#### Section 1: Model Configuration
```yaml
model:
  endpoint_name: "llama-3-3-70b"  # Databricks endpoint name
  temperature: 0.7                 # LLM temperature (0.0-1.0)
  max_tokens: 100                  # Max response tokens
  top_p: 0.9                       # Nucleus sampling parameter
```

#### Section 2: Message Constraints
```yaml
message_constraints:
  min_length: 200
  max_length: 220
  target_length: 210
```

#### Section 3: Daily Ratings
```yaml
daily_ratings:
  committed:
    name: "Committed"
    description: "You are committed to your healthy habits and seeing results."
    min_score: 81
```

#### Section 4: Scoring Criteria (678 lines)
**See "Health Management Scoring System" section for full details**

Key structure:
```yaml
scoring_criteria:
  max_base_points: 50
  max_bonus_points: 10
  
  weight:
    max_points: 5
    inclusion_criteria: [...]
    scoring_rules:
      loss_goal:
        decreased_1lb_14d: 5
        stable_1lb_14d: 3
        # ...
```

#### Section 5: Clinical Thresholds
```yaml
clinical_thresholds:
  glucose:
    a1c_target_7:
      tir_positive_threshold: 70
      tir_opportunity_threshold: 60
      high_threshold: 180
      low_threshold: 70
    a1c_target_8:
      tir_positive_threshold: 50
      # ...
  
  activity:
    weekly_goal_minutes: 150
    daily_goal_minutes: 30
    increase_threshold: 5
  
  sleep:
    hours_target: 7
    hours_minimum: 5
    rating_excellent: 8
    rating_good: 6
  
  weight:
    maintenance_range_pct: 3
    loss_threshold_lbs: 1
```

#### Section 6: Priority Rules
```yaml
priority_rules:
  tier_1_clinical_urgency:
    - glucose_low_readings
    - glucose_extended_high
    - meds_missed_consecutive
  
  tier_2_positive_reinforcement:
    - glucose_tir_improved
    - activity_goal_met
    - sleep_quality_high
  
  tier_3_long_term_goals:
    - journey_next_task
    - exercise_program_suggestion
```

#### Section 7: Message History Rules
```yaml
message_history:
  lookback_days: 6
  max_streak_days: 3
  weight_max_per_week: 2
  weight_no_consecutive: true
```

#### Section 8: Prompt Segments
```yaml
prompt_segments:
  system_prompt: |
    You are a health coach assistant generating personalized daily health insights.
    ...
  
  positive_actions:
    glucose_tir_high: "Your blood sugar was in range {tir_pct}% of the time yesterday, up {improvement}% from the day before."
    activity_goal_met: "You completed {minutes} minutes of activity this week, exceeding your {goal} minute goal."
    # ... 25+ templates
  
  opportunities:
    meal_logging_opportunity: "You logged {meals_logged} meal yesterday. Try logging all {meals_target} meals today to track your nutrition more completely."
    # ... 20+ templates
```

### How to Update Configuration

**Scenario 1: Change LLM Model**
```yaml
# In prompts.yml, update:
model:
  endpoint_name: "claude-3-sonnet"  # Change from llama-3-3-70b
  temperature: 0.8                   # Adjust parameters
```

**Scenario 2: Adjust Scoring Thresholds**
```yaml
# In prompts.yml, update:
scoring_criteria:
  activity:
    scoring_rules:
      minutes_150_7d: 5    # Keep
      minutes_120_7d: 4    # Keep
      minutes_100_7d: 3    # NEW: Add intermediate tier
      minutes_90_7d: 2     # Changed from 3
      minutes_60_7d: 1     # Changed from 2
```

**Scenario 3: Add New Positive Action**
```yaml
# In prompts.yml, add:
prompt_segments:
  positive_actions:
    hydration_goal_met: "You logged {glasses} glasses of water yesterday, hitting your {goal} glass goal."
```

Then in logic_engine.py, add eligibility logic:
```python
def get_eligible_positive_actions(self, user, history):
    # ... existing code ...
    
    # Hydration (NEW)
    if user.water_glasses_logged >= user.water_goal:
        eligible.append('hydration_goal_met')
```

**No code redeployment needed for prompts.yml changes!** Just update the config file and restart the Databricks job.

---

## Usage Examples

### Example 1: Single User Message Generation (Interactive)

```python
from logic_engine import LogicEngine, UserContext, MessageHistory
from insight_generator import InsightGenerator
from datetime import datetime

# Initialize engines
logic = LogicEngine("prompts.yml")
generator = InsightGenerator("prompts.yml")

# Build user context (from Gold table features)
user = UserContext(
    patient_id="12345",
    report_date=datetime(2024, 1, 15),
    
    # Profile
    has_cgm=True,
    has_step_tracker=True,
    user_focus="Glucose",
    a1c_target_group=A1CTargetGroup.DM_TARGET_7,
    
    # Yesterday's features
    tir_pct=78,
    tir_prev_day=73,
    daily_step_count=8500,
    active_minutes=35,
    weekly_active_minutes=165,
    sleep_duration_hours=7.2,
    sleep_rating=8,
    meals_logged_count=2,
    took_all_meds=True
)

# Load message history
history = MessageHistory(
    patient_id="12345",
    categories_shown_last_6d=["glucose", "activity", "sleep"],
    weight_messages_this_week=1
)

# Run logic engine
selected = logic.select_content(user, history)

print(f"Daily Rating: {selected.daily_rating}")
print(f"Positive Actions: {selected.positive_actions}")
print(f"Opportunity: {selected.opportunity}")

# Generate natural language message
message = generator.generate_insight(selected, user)

print(f"\nFinal Message ({len(message)} chars):")
print(message)
```

**Output**:
```
Daily Rating: Strong
Positive Actions: [
    {'type': 'glucose_tir_high', 'value': 78, 'improvement': 5},
    {'type': 'activity_goal_met', 'minutes': 165}
]
Opportunity: {'type': 'meal_logging_opportunity', 'meals_logged': 2, 'meals_target': 3}

Final Message (207 chars):
Good morning. Your blood sugar was in range 78% yesterday, up 5%. You hit your 165-minute activity goal this week. Try logging all 3 meals today to better track your nutrition.
```

---

### Example 2: Batch Processing All Users

```python
from pyspark.sql import SparkSession
from main_pipeline import generate_messages_batch

spark = SparkSession.builder.getOrCreate()

# Load Gold features for yesterday
gold_df = spark.table("bronz_als_azdev24.gold_feature_table") \
    .filter(col("report_date") == "2024-01-15")

# Load message history (last 6 days)
history_df = spark.table("bronz_als_azdev24.message_history") \
    .filter(col("message_date") >= "2024-01-09")

# Generate messages for all users
results_df = generate_messages_batch(
    gold_df=gold_df,
    history_df=history_df,
    report_date="2024-01-15"
)

# Save to output table
results_df.write.mode("append").saveAsTable("bronz_als_azdev24.generated_messages")

print(f"Generated {results_df.count()} messages")
```

---

### Example 3: API Call for Real-Time Generation

```python
import requests
import json

# Databricks Model Serving endpoint
endpoint = "https://adb-2008955168844352.12.azuredatabricks.net/serving-endpoints/simon-insights/invocations"
token = "dapi..."

# User features (from Gold table or real-time query)
user_features = {
    "patient_id": "12345",
    "report_date": "2024-01-15",
    "has_cgm": True,
    "has_step_tracker": True,
    "user_focus": "Glucose",
    "a1c_target_group": "a1c_target_7",
    "tir_pct": 78.0,
    "tir_prev_day": 73.0,
    "daily_step_count": 8500,
    "active_minutes": 35.0,
    "weekly_active_minutes": 165.0,
    "sleep_duration_hours": 7.2,
    "sleep_rating": 8.0,
    "meals_logged_count": 2,
    "took_all_meds": True,
    "med_adherence_7d_avg": 1.0,
    # ... (include all required features)
}

# API request
response = requests.post(
    endpoint,
    json={"dataframe_records": [user_features]},
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
)

if response.status_code == 200:
    result = response.json()
    message = result['predictions'][0]
    print(f"Generated Message: {message}")
else:
    print(f"Error: {response.status_code} - {response.text}")
```

---

## Development Guide

### Local Setup

#### 1. Python Environment
```bash
# Create Python 3.12 virtual environment
cd "c:\Users\achaudhary\WelldocProjects\Metablic_Readiness"
python -m venv simon
.\simon\Scripts\Activate

# Install dependencies
pip install databricks-sdk databricks-connect delta-spark pyyaml mlflow pyspark
```

#### 2. Databricks Connection
```python
# databricks.yml
root_path: /Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Mealbolic_Readiness
workspace:
  host: https://adb-2008955168844352.12.azuredatabricks.net
```

#### 3. Local Testing (Mock Data)
```python
# test_logic_engine.py
import pytest
from logic_engine import LogicEngine, UserContext, A1CTargetGroup, MessageHistory

def test_scoring_weight_loss_goal():
    """Test weight scoring for loss goal."""
    logic = LogicEngine("prompts.yml")
    
    user = UserContext(
        patient_id="test",
        report_date=datetime.now(),
        has_weight_goal=True,
        weight_goal_type="lose",
        weight_change_lbs_14d=-2.5,  # Lost 2.5 lbs
        user_focus=None  # No focus = include all categories
    )
    
    rating, desc = logic.calculate_daily_rating(user)
    
    # Should get 5 points for weight (lost >1 lb)
    # With only weight category, percentage = 5/5 = 100%
    assert rating == "Committed"

def test_scoring_focus_area_filtering():
    """Test that focus area filters categories."""
    logic = LogicEngine("prompts.yml")
    
    user = UserContext(
        patient_id="test",
        report_date=datetime.now(),
        user_focus="Glucose",  # Only glucose focus
        has_cgm=True,
        tir_pct=75,  # 75% TIR (meets threshold for 5 pts)
        # Other categories have data but should be ignored
        daily_step_count=10000,
        active_minutes=60,
        meals_logged_count=3
    )
    
    rating, desc = logic.calculate_daily_rating(user)
    
    # Should only include glucose category (5 pts)
    # percentage = 5/5 = 100%
    assert rating == "Committed"

pytest.main([__file__, "-v"])
```

#### 4. Running Tests
```bash
# Unit tests
pytest test_logic_engine.py -v
pytest test_insight_generator.py -v

# Integration test (requires Databricks connection)
python test_end_to_end.py
```

---

### Feature Engineering Updates

#### Adding a New Health Category

**Example**: Add "Hydration" tracking

**Step 1: Update Bronze/Silver Tables**
```sql
-- Assume new table: hydrationentry
CREATE TABLE bronz_als_azdev24.hydrationentry (
    patient_id STRING,
    entrydate DATE,
    glasses_logged INT,
    glasses_goal INT,
    timezoneoffset INT
)
```

**Step 2: Update notebook.py (Gold Layer)**
```python
def create_hydration_features(spark, config):
    """Create hydration features with 7-day tracking."""
    return spark.sql(f"""
        SELECT 
            patient_id,
            entrydate as report_date,
            glasses_logged,
            glasses_goal,
            LAG(glasses_logged, 1) OVER (
                PARTITION BY patient_id ORDER BY entrydate
            ) as prev_day_glasses,
            SUM(glasses_logged) OVER (
                PARTITION BY patient_id 
                ORDER BY entrydate 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as glasses_logged_7d
        FROM {config['source_catalog']}.hydrationentry
        WHERE entrydate >= current_date() - INTERVAL 30 DAYS
    """)

# In create_gold_feature_table(), add:
hydration_df = create_hydration_features(spark, CONFIG)
gold_df = gold_df.join(hydration_df, ["patient_id", "report_date"], "left")
```

**Step 3: Update UserContext (logic_engine.py)**
```python
@dataclass
class UserContext:
    # ... existing fields ...
    
    # Hydration (NEW)
    glasses_logged: Optional[int] = None
    glasses_goal: Optional[int] = None
    glasses_logged_7d: Optional[int] = None
```

**Step 4: Update prompts.yml**
```yaml
scoring_criteria:
  # ... existing categories ...
  
  # === CATEGORY 11: HYDRATION (5 points) ===
  hydration:
    max_points: 5
    inclusion_criteria:
      - "Include if user has no focus set"
      - "If user has focus set, include if Eating Habits is a focus area"
    
    scoring_rules:
      goal_met_7d: 5      # Met goal all 7 days
      goal_met_5d: 3      # Met goal 5+ days in last 7
      goal_met_3d: 1      # Met goal 3+ days in last 7
      no_tracking: 0      # No hydration logged

prompt_segments:
  positive_actions:
    hydration_goal_met: "You logged {glasses} glasses of water yesterday, hitting your {goal} glass goal."
    hydration_streak: "You've met your hydration goal {days} days in a row this week."
  
  opportunities:
    hydration_improve: "You logged {glasses} glasses of water yesterday. Try reaching your {goal} glass goal today."
```

**Step 5: Update Logic Engine Scoring**
```python
# In calculate_daily_rating():
# === CATEGORY 11: HYDRATION (5 points) ===
if should_include_category('hydration'):
    max_score += 5
    hydration_rules = scoring.get('hydration', {}).get('scoring_rules', {})
    
    if user.glasses_logged_7d is not None and user.glasses_goal is not None:
        days_met = sum(1 for d in range(7) if daily_glasses[d] >= user.glasses_goal)
        if days_met == 7:
            score += hydration_rules.get('goal_met_7d', 5)
        elif days_met >= 5:
            score += hydration_rules.get('goal_met_5d', 3)
        elif days_met >= 3:
            score += hydration_rules.get('goal_met_3d', 1)
```

**Step 6: Update Eligibility Functions**
```python
# In get_eligible_positive_actions():
# Hydration (NEW)
if user.glasses_logged is not None and user.glasses_goal is not None:
    if user.glasses_logged >= user.glasses_goal:
        eligible.append('hydration_goal_met')
    
    # Check for streak
    # (requires additional logic to track daily values)
    if hydration_streak_days >= 3:
        eligible.append('hydration_streak')

# In get_eligible_opportunities():
# Hydration opportunity
if user.glasses_logged is not None and user.glasses_goal is not None:
    if user.glasses_logged < user.glasses_goal:
        eligible.append('hydration_improve')
```

**Step 7: Update Focus Area Mappings**
```python
# In calculate_daily_rating(), update:
focus_to_category = {
    'Weight': ['weight'],
    'Glucose': ['glucose'],
    'Activity': ['activity', 'steps'],
    'Eating Habits': ['food_logging', 'nutrient_targets', 'hydration'],  # Add here
    'Sleep': ['sleep_duration', 'sleep_rating'],
    'Medications': ['medications'],
    'Anxiety': ['mental_wellbeing']
}
```

---

### Testing Strategy

#### Unit Tests (logic_engine.py)
```python
def test_hydration_scoring_goal_met_7d():
    user = UserContext(
        ...,
        glasses_logged=8,
        glasses_goal=8,
        glasses_logged_7d=56  # Met goal all 7 days
    )
    rating, _ = logic.calculate_daily_rating(user)
    # Should get 5 points for hydration

def test_hydration_eligibility():
    user = UserContext(
        ...,
        glasses_logged=10,
        glasses_goal=8
    )
    eligible = logic.get_eligible_positive_actions(user, history)
    assert 'hydration_goal_met' in eligible
```

#### Integration Tests (end-to-end)
```python
def test_hydration_message_generation():
    # Full pipeline test with mock LLM
    user = UserContext(...)  # with hydration data
    selected = logic.select_content(user, history)
    
    # Mock LLM call
    message = "Good morning. You hit your 8-glass water goal yesterday. Keep up the great hydration!"
    
    assert len(message) >= 200
    assert "water" in message.lower()
```

---

## Troubleshooting

### Common Issues

#### Issue 1: Scoring Percentage Doesn't Match Expected Rating

**Symptoms**:
- User with good metrics gets "Building" instead of "Committed"
- Scores seem lower than they should be

**Root Causes**:
- Focus area filtering excluding expected categories
- Missing feature data (NULL values treated as 0)
- Mismatched A1C target group

**Debugging Steps**:

1. **Print intermediate scoring**:
```python
# In calculate_daily_rating(), add debug logging:
print(f"[DEBUG] max_score: {max_score}, achieved_score: {score}, percentage: {percentage}")
print(f"[DEBUG] Categories included: {included_categories}")
```

2. **Check focus area logic**:
```python
# Test with no focus set (should include all categories)
user.user_focus = None
rating, _ = logic.calculate_daily_rating(user)
print(f"No focus rating: {rating}")

# Test with specific focus
user.user_focus = "Glucose"
rating, _ = logic.calculate_daily_rating(user)
print(f"Glucose focus rating: {rating}")
```

3. **Verify feature data**:
```sql
-- Check Gold table for missing values
SELECT 
    patient_id,
    SUM(CASE WHEN tir_pct IS NULL THEN 1 ELSE 0 END) as missing_tir,
    SUM(CASE WHEN daily_step_count IS NULL THEN 1 ELSE 0 END) as missing_steps,
    SUM(CASE WHEN active_minutes IS NULL THEN 1 ELSE 0 END) as missing_activity
FROM bronz_als_azdev24.gold_feature_table
WHERE report_date = '2024-01-15'
GROUP BY patient_id
HAVING missing_tir > 0 OR missing_steps > 0 OR missing_activity > 0
```

4. **Validate A1C target group**:
```python
# Ensure correct enum value
assert user.a1c_target_group in [
    A1CTargetGroup.DM_TARGET_7,
    A1CTargetGroup.DM_TARGET_8,
    A1CTargetGroup.DIP,
    A1CTargetGroup.NON_DM
]
```

---

#### Issue 3: Same Message Shown Multiple Days

**Symptoms**:
- User sees glucose message 3+ days in a row
- Weight messages shown on consecutive days

**Root Causes**:
- Message history not being updated
- Frequency cap logic not applied correctly
- Category detection in select_content() failing

**Solutions**:

1. **Verify history table updates**:
```sql
-- Check recent message history
SELECT 
    patient_id,
    message_date,
    categories_shown,
    category_streaks
FROM bronz_als_azdev24.message_history
WHERE patient_id = '12345'
ORDER BY message_date DESC
LIMIT 10
```

2. **Test frequency cap logic**:
```python
# In test_logic_engine.py
def test_weight_frequency_cap():
    history = MessageHistory(
        patient_id="12345",
        categories_shown_last_6d=["weight", "weight", "glucose"],
        weight_messages_this_week=2,  # Already at max
        weight_shown_yesterday=True
    )
    
    user = UserContext(...)  # with weight achievement
    eligible = logic.get_eligible_positive_actions(user, history)
    
    # Weight actions should be filtered out
    weight_actions = [a for a in eligible if 'weight' in a]
    assert len(weight_actions) == 0
```

3. **Add debug logging**:
```python
# In select_content():
print(f"[DEBUG] Categories shown last 6d: {history.categories_shown_last_6d}")
print(f"[DEBUG] Category streaks: {history.category_streaks}")
print(f"[DEBUG] Weight messages this week: {history.weight_messages_this_week}")
print(f"[DEBUG] Eligible after frequency filtering: {eligible_filtered}")
```

---

#### Issue 4: Databricks LLM API Errors

**Symptoms**:
- `databricks.sdk.errors.PermissionDenied`: Token doesn't have access
- `databricks.sdk.errors.ResourceNotFound`: Endpoint not found
- `databricks.sdk.errors.RequestLimitExceeded`: Rate limit hit

**Solutions**:

1. **Permission Denied**:
```python
# Verify token has "Model Serving User" permission
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
print(w.current_user.me())  # Should not error

# Check endpoint permissions
endpoints = w.serving_endpoints.list()
print([e.name for e in endpoints])  # Should include your endpoint
```

2. **Endpoint Not Found**:
```yaml
# In prompts.yml, verify exact endpoint name:
model:
  endpoint_name: "databricks-meta-llama-3-3-70b-instruct"  # Must match exactly
```

```python
# List available endpoints
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for endpoint in w.serving_endpoints.list():
    print(f"Name: {endpoint.name}, State: {endpoint.state.ready}")
```

3. **Rate Limit**:
```python
# Add retry logic with exponential backoff
import time
from databricks.sdk.errors import RequestLimitExceeded

def _call_llm_with_retry(self, prompt, max_retries=5):
    for attempt in range(max_retries):
        try:
            return self._call_llm(prompt)
        except RequestLimitExceeded as e:
            if attempt == max_retries - 1:
                raise
            wait_time = 2 ** attempt  # 1, 2, 4, 8, 16 seconds
            print(f"Rate limited, waiting {wait_time}s...")
            time.sleep(wait_time)
```

4. **Batch API Calls**:
```python
# Instead of calling LLM per user, batch requests
def generate_messages_batch_optimized(users):
    # Group users into batches of 10
    batches = [users[i:i+10] for i in range(0, len(users), 10)]
    
    for batch in batches:
        # Generate prompts for all users in batch
        prompts = [build_prompt(u) for u in batch]
        
        # Call LLM once with multiple prompts (if API supports)
        # Or call sequentially with small delay
        for user, prompt in zip(batch, prompts):
            message = generator.generate_insight(prompt)
            time.sleep(0.1)  # 100ms delay between calls
```

---

#### Issue 5: Feature Engineering Performance Issues

**Symptoms**:
- Gold table creation takes >1 hour for 100k users
- OOM (Out of Memory) errors during aggregation
- Skewed partitions in window functions

**Solutions**:

1. **Optimize Window Functions**:
```python
# Use rangeBetween instead of rowsBetween for date-based windows
from pyspark.sql import Window
from pyspark.sql.functions import sum, avg

window_7d = Window.partitionBy("patient_id") \
    .orderBy(col("entrydate").cast("long")) \
    .rangeBetween(-6 * 86400, 0)  # 6 days in seconds

df = df.withColumn("weekly_active_minutes", sum("durationminutes").over(window_7d))
```

2. **Broadcast Small Tables**:
```python
from pyspark.sql.functions import broadcast

# Broadcast user profiles (usually <1GB)
gold_df = gold_df.join(
    broadcast(user_profiles),
    "patient_id",
    "left"
)
```

3. **Salting for Skewed Keys**:
```python
# If some users have 100x more data than others
from pyspark.sql.functions import rand, floor

df = df.withColumn("salt", floor(rand() * 10))  # Add salt column
df_salted = df.repartition(100, "patient_id", "salt")
```

4. **Incremental Processing**:
```python
# Only process last 30 days, not full history
bronze_df = spark.table("bronz_als_azdev24.elogbgentry") \
    .filter(col("entrydate") >= current_date() - 30)
```

5. **Increase Cluster Resources**:
```yaml
# For 100k+ users, use larger cluster
cluster:
  node_type_id: "Standard_DS4_v2"  # Up from DS3_v2
  num_workers: 8                    # Up from 2
  spark_conf:
    "spark.sql.shuffle.partitions": "200"
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
```

---

### Debugging Checklist

When troubleshooting message generation issues, work through this checklist:

- [ ] **Gold Table**: Verify feature data exists for user and date
- [ ] **Profile Flags**: Check has_cgm, has_step_tracker, user_focus are correct
- [ ] **Scoring Calculation**: Add debug prints to calculate_daily_rating()
- [ ] **Eligibility Logic**: Verify get_eligible_positive_actions() returns expected actions
- [ ] **Priority Rules**: Check select_content() priority ordering
- [ ] **Frequency Caps**: Verify message history is up-to-date
- [ ] **LLM Prompt**: Print full assembled prompt before API call
- [ ] **LLM Response**: Print raw LLM output before length validation
- [ ] **Length Validation**: Check character count matches expected range (200-220)
- [ ] **Error Handling**: Catch and log all exceptions with full context

---

## Appendix

### Glossary

- **TIR**: Time in Range (percentage of glucose readings in target range)
- **CGM**: Continuous Glucose Monitor
- **A1C**: Hemoglobin A1C (3-month average blood sugar)
- **DIP**: Diabetes In Pregnancy
- **DM**: Diabetes Mellitus
- **Bronze/Silver/Gold**: Medallion architecture data layers (raw → cleaned → aggregated)
- **Focus Area**: User-selected health priority (Weight, Glucose, Activity, Eating Habits, Sleep, Medications, Anxiety)
- **Positive Action**: Health achievement to acknowledge (e.g., "TIR improved")
- **Opportunity**: Suggestion for improvement (e.g., "Log more meals")
- **Daily Rating**: Overall health management tier (Committed, Strong, Consistent, Building, Ready)

### Key File Locations

```
c:\Users\achaudhary\WelldocProjects\Metablic_Readiness\
├── simon\                          # Python 3.12 virtual environment
├── prompts.yml                    # Configuration (678 lines)
├── logic_engine.py                # Business logic (1214 lines)
├── insight_generator.py           # LLM integration (~500 lines)
├── main_pipeline.py               # Orchestration
├── PROJECT_DOCUMENTATION.md       # This file
├── Feature_store_Creation\
│   ├── notebook.py                # Feature engineering
│   ├── Scoring_Criteria.csv       # Source of truth for scoring
│   └── Gemini_convo.txt           # Original conversation reference
└── databricks.yml                 # Databricks sync config
```

### Contact & Support

**Project Owner**: achaudhary@welldocinc.com  
**Databricks Workspace**: adb-2008955168844352.12.azuredatabricks.net  
**Sync Path**: /Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Mealbolic_Readiness

---

## Document Version

**Version**: 1.0  
**Last Updated**: January 2024  
**Author**: GitHub Copilot + ML Engineering Team  
**Target Audience**: ML Engineers, Data Engineers, Health Informatics Developers
