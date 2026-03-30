# Metabolic Readiness - Project Status

## Overview

The **SIMON Health Habits** project generates personalized daily health messages for patients using a "Logic-First, LLM-Second" hybrid pipeline. The system:

1. **Feature Store (Gold Layer)**: Aggregates raw health data into daily per-patient features
2. **Logic Engine**: Applies deterministic business rules to select positive actions, opportunities, and a daily rating (1-100% score mapped to 5 tiers)
3. **Insight Generator**: Sends structured facts to an LLM (Llama 3.3 70B via Databricks) to produce a natural-language message
4. **Serving API**: Deployed as a Databricks Model Serving endpoint via MLflow

---

## Architecture

```
Bronze/Silver Tables ──→ Feature Store (Gold) ──→ Logic Engine ──→ LLM ──→ Message
                         (notebook.py)            (logic_engine.py)  (insight_generator.py)
                                                         ↕
                                                  Message History Table
                                                  (frequency capping)
```

**Key Files:**
| File | Purpose |
|------|---------|
| `Feature_store_Creation/notebook.py` | PySpark pipeline to build the Gold feature table |
| `logic_engine.py` | Deterministic business logic for content selection and scoring |
| `insight_generator.py` | LLM prompt formatting and Databricks Foundation Model API calls |
| `main_pipeline.py` | Orchestration notebook with `MessageGenerationModel.predict()` |
| `prompts.yml` | All prompts, thresholds, scoring criteria, and templates |

---

## Scoring System (10 Categories + Bonus)

Total base: **50 points** (10 categories × 5 points). Percentage mapped to rating tier.

| Score Range | Label | Description |
|-------------|-------|-------------|
| 81-100% | Committed | You are committed to your healthy habits and seeing results. |
| 61-80% | Strong | Your healthy habits are solid and dependable. |
| 41-60% | Consistent | You are making a great effort to make your healthy habits a routine. |
| 21-40% | Building | You are working to create new healthy habits. |
| 1-20% | Ready | You are ready to choose your next healthy steps. |

---

## Source Tables - Current Status

### Fully Integrated

| Category | Table | Key Columns | Status |
|----------|-------|-------------|--------|
| **Glucose** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.elogbgentry` | patientid, bgvalue, observationdatetime, timezoneoffset, externalsourceid(18=CGM) | ✅ Done |
| **Activity** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.elogexerciseentry` | patientid, exerciseduration, exercisetype, observationdatetime, timezoneoffset | ✅ Done |
| **Steps** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.stepentry` | patientid, numberofsteps, startdatetime, enddatetime, timezoneoffset | ✅ Done |
| **Weight** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.elogweightentry` | patientid, weight, weightuomid, observationdatetime, timezoneoffset | ✅ Done |
| **Food** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.foodmoduleitem` | patientid, observationdatetime, carbs, protein, fat, calories, fiber, sugar, activitytypeid | ✅ Done |
| **Sleep** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.sleepentry` | patientid, startdatetime, enddatetime, timezoneoffset, sleeprating | ✅ Done |
| **Med Admin** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.medadministration` | patientid, statusid, administrationdate, dose, administrationtimezoneoffset | ✅ Done |
| **Med Rx** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.medprescription` | patientid, frequencytype, frequencyvalue, startdate, statusid | ✅ Done (simplified) |
| **Nutrition Goals** | `bronz_als_azuat2.trxdb_dsmbasedb_user.patientgoaldetails` | patientid, protein, fat, calories, carbs | ✅ Done |
| **Weight Goals** | `bronz_als_azuat2.trxdb_dsmbasedb_observation.weightgoal` | patientid, targetweight, type, status, startdate | ✅ Table registered (join partially implemented) |

### Newly Added (Feb 2026)

| Category | Table | Key Columns | Status |
|----------|-------|-------------|--------|
| **A1C Target** | `bronz_als_azdev24.trxdb_dsmbasedb_observation.patienttargetsegment` | patientid, lastmodifieddatetime, a1ctarget (enum: 1=<7, 2=<8, 3=<6) | ✅ Added to feature store |
| **Meditation** | *(derived from activity table)* | exercisetype = 30045 | ✅ Added - daily/7d/30d flags |
| **Journal** | `bronz_als_azdev24.trxdb_dsmbasedb_userengagement.userjournal` | patientid, createddatetime | ✅ Added - daily/7d/30d flags |
| **Grocery** | `bronz_als_azdev24.trxdb_dsmbasedb_user.grocerydetails` | patientid, entrydatetimeinmills | ✅ Added - daily flag |
| **Action Plan** | `bronz_als_azdev24.trxdb_dsmbasedb_user.actionplanprogress` | patientid, actionplanstatus (1=final, 2=completed, 3=deleted), createddate, timezoneoffset | ✅ Added - active/completed/7d/30d flags |

### Newly Added (23 Mar 2026)

| Category | Table | Key Columns | Status |
|----------|-------|-------------|--------|
| **Journey** | `bronz_als_azdev24.trxdb_dsmbasedb_user.GuidedJourneyWeeksAndTasksDetail` | patientid, isjourneycompleted (enum: 1=active, 2=completed, 3=incomplete) | ✅ Added - active/completed/task flags |
| **Exercise Video** | `bronz_als_azdev24.trxdb_dsmbasedb_user.curatedvideositemdetail` | patientid, createddate, modifieddate, statusid (enum: 1=active, 2=completed) | ✅ Added - daily/7d completion flags |
| **Exercise Program** | `bronz_als_azdev24.trxdb_dsmbasedb_user.curatedvideosprogramdetail` | patientid, statusid (enum: 1=active, 2=completed, 3=stopped), activateddatetime, createddatetime, modifieddatetime | ✅ Added - active/started/completed/progress flags |

### Still Missing (Table Unknown or Not Found)

| Category | What's Needed | Used For | Priority |
|----------|---------------|----------|----------|
| **Exercise Video %** | Percentage completion field in curatedvideositemdetail | Positive action: "completed (90%) an exercise video" - currently tracking completion only | LOW |
| **AI Meal Plan** | Table tracking when user generated an AI meal plan | Positive action + Bonus: 1 pt | MEDIUM |
| **Article/Lesson/Video** | Content interaction table (articles read, lessons completed, videos watched) | Explore positive actions + Bonus: 1 pt each (article, lesson, video) | MEDIUM |
| **Med Reminders** | User settings flag for whether medication reminders are enabled | Opportunity: "Set medication reminders" | LOW |
| **User Focus** | Where the user's focus area preference is stored (Weight, Glucose, Activity, etc.) | Category filtering for scoring and message selection | MEDIUM |
| **Step Tracker Connected** | Definitive flag for wearable connection (beyond just having step data) | Exclusion logic: don't show steps if no tracker | LOW |
| **App Login** | Session/login tracking table | Default fallback positive reinforcement | LOW |

---

## Feature Gaps Within Existing Tables

These are features that the business requirements call for but aren't yet computed or are missing source columns:

### Glucose
| Gap | Detail |
|-----|--------|
| **Per-patient glucose target ranges** | Currently hardcoded to 70-180 mg/dL. The Gemini conversation references per-patient "high" and "low" target columns -- these columns have not been identified in the `elogbgentry` table yet. DIP users should use 70-140. |
| **A1C target for DIP/non-DM** | The `patienttargetsegment` table has enum values 1, 2, 3 (<7, <8, <6). There is no mapping for DIP or non-DM patients. Need a separate table or logic to distinguish these groups. |

### Weight Goals
| Gap | Detail |
|-----|--------|
| **Weight goal join** | The `weightgoal` table is registered in CONFIG but the join logic in `create_weight_features()` is not implemented. Need to join to compute: `is_within_maintenance_range`, `weight_goal_type`, `weight_change_lbs_14d`, `weight_change_pct_14d`, `has_weight_goal`. |

### Food & Nutrition
| Gap | Detail |
|-----|--------|
| **Additional nutrient goal columns** | `patientgoaldetails` currently only has protein, fat, calories, carbs. The `foodmoduleitem` table has fiber, sugar, addedsugar, sodium, cholesterol, potassium -- but no corresponding goal columns for these nutrients. |
| **sodium column name** | The food table has a column named `sodiumobservationstatus` which may actually be a status flag, not the sodium value. Need to verify. |

### Medication
| Gap | Detail |
|-----|--------|
| **Daily expected dose calculation** | Current implementation uses a simplified count of active prescriptions. Need to properly calculate expected daily doses from `frequencytype` × `frequencyvalue` to get accurate adherence percentage. |

---

## Logic Engine & Pipeline Status

### Fully Implemented
- ✅ 10-category scoring system (Weight, Glucose, Activity, Steps, Food Logging, Nutrient Targets, Sleep Duration, Sleep Rating, Medications, Mental Well-being)
- ✅ Bonus point system (framework in place, some data sources missing)
- ✅ Positive action detection for all main categories
- ✅ Opportunity detection for all main categories
- ✅ Focus area filtering (only show relevant categories) — **broadened to match YAML spec (23 Mar 2026)**
- ✅ Focus filter fallback — if filtering removes all qualifying content, unfiltered data is used **(23 Mar 2026)**
- ✅ Priority system with boosts (journey, CGM, weight goal, focus area, variety)
- ✅ Frequency capping (weight max 2x/week, no back-to-back)
- ✅ 3-day streak detection and category switching
- ✅ 6-day lookback for "not recently shown" priority boost
- ✅ Message history upsert with MERGE for idempotency
- ✅ LLM prompt templates in `prompts.yml`
- ✅ MLflow wrapper for Databricks Model Serving
- ✅ Time-based greetings — **randomized from expanded pool (23 Mar 2026)**
- ✅ Journey tracking (has_active_journey, journey_task_completed) - from GuidedJourneyWeeksAndTasksDetail table
- ✅ Exercise video completion tracking - from curatedvideositemdetail table
- ✅ Exercise program tracking (started/completed/progress) - from curatedvideosprogramdetail table
- ✅ Grocery bonus flag - from grocerydetails table
- ✅ CGM force-include expanded — now triggers on TIR meeting target OR improving from previous day **(23 Mar 2026)**
- ✅ Glucose opportunity decision tree — branching logic for food logging, glycemic med adherence, provider contact, pay attention **(23 Mar 2026)**
- ✅ Post-generation validation LLM call — toggleable language quality check **(23 Mar 2026)**
- ✅ Sleep improvement suggestions pre-selected by logic engine (13 specific tips with variety tracking) **(23 Mar 2026)**
- ✅ Food healthy eating suggestions pre-selected by logic engine (8 specific tips with variety tracking) **(23 Mar 2026)**
- ✅ Bonus activities surfaced as positive actions — exercise_program_started, ai_meal_plan_generated, article_read, video_watched, lesson_completed **(23 Mar 2026)**
- ✅ Medication opportunity guard — won't suggest reminders if user took all meds yesterday **(23 Mar 2026)**

### Clinical Coaching Fixes (23 Mar 2026)
Per clinical feedback review, the following changes were made:
- **Removed**: Medication priority boost (`medications_on_list` set to 0) — was causing messages to over-index on medication adherence
- **Removed**: "Prioritize showing positive actions from the Medications category more often than other categories" requirement
- **Added**: System prompt constraints to prevent hyperbolic language ("big step" only for >15% TIR changes)
- **Added**: System prompt rules against "You had a great day yesterday," "you took a big step" for Journey tasks, "doing something right" when below target
- **Added**: Explicit instruction to refer to "guided meditation in the app" not just "sitting quietly"
- **Added**: Grammar/spelling rules for LLM output plus toggleable validation LLM pass
- **Fixed**: `FOCUS_CATEGORY_MAP` was narrower than YAML `focus_area_mappings` spec — now matches (e.g., Weight focus includes food, activity, steps, sleep)
- **Fixed**: `should_include_category()` scoring map also broadened to match
- **Fixed**: CGM force-include only triggered on TIR meeting threshold, not on TIR improving — now covers both
- **Fixed**: Focus filter with no qualifying data gave empty results — now falls back to unfiltered
- **Fixed**: Food healthy suggestions were listed in YAML but never wired as opportunities — now pre-selected by logic engine

### Partially Implemented
- ⚠️ Mental well-being scoring (meditation/journal/action plan flags now computed in feature store, but logic engine still uses 30-day flags from old placeholders -- needs wiring to new 7-day flags)
- ⚠️ A1C target group now available from feature store but `main_pipeline.py` still hardcodes `a1c_target_group: 'dm_target_7'` in `get_user_profile()` -- needs to read from Gold table
- ⚠️ Weight goal features (table exists but join not implemented in feature store)
- ⚠️ Exercise video completion % - table tracks completion (statusid=2) but not % progress toward completion

### Not Yet Wired
- ❌ Bonus scoring fields (`bonus_ai_meal_plan`, `bonus_article_read`, `bonus_lesson_completed`, `bonus_video_watched`) -- logic engine reads these from `UserContext` but no source tables feed them yet
- ❌ `med_reminders_enabled` -- source unknown
- ❌ `user_focus` -- source table unknown
- ❌ `takes_glycemic_lowering_med` / `glycemic_med_adherent` -- placeholder fields in UserContext; need medication-type data source to identify glycemic-lowering meds **(added 23 Mar 2026)**

---

## Out of Scope / Pending Data Source

Items that require data not yet available. Logic engine placeholder code is in place — will activate when data is wired.

| Item | Placeholder | What's Needed | Impact |
|------|-------------|---------------|--------|
| **Glycemic-lowering med identification** | `UserContext.takes_glycemic_lowering_med = False` | Medication-type column or lookup table to distinguish glycemic-lowering meds from other medications | Glucose opportunity decision tree steps 2 & 3 (take med as prescribed / contact provider) won't activate |
| **Glycemic med adherence** | `UserContext.glycemic_med_adherent = False` | Same as above — once med type is known, adherence can be calculated | Same as above |
| **AI meal plan tracking** | `UserContext.bonus_ai_meal_plan = False` | Table/column tracking when user generated an AI meal plan | Won't appear as positive action or bonus point |
| **Article/lesson/video tracking** | `bonus_article_read`, `bonus_lesson_completed`, `bonus_video_watched = False` | Content interaction table | Won't appear as positive actions or Explore-category content |
| **User focus preferences** | `UserContext.user_focus = None` | Where focus area preference is stored | All categories included (no filtering) |
| **Med reminders enabled** | `UserContext.med_reminders_enabled = False` | User settings table | Medication reminders opportunity may fire for users who already have reminders enabled |

---

## What to Do Next (Prioritized)

### P0 - Wire New Features End-to-End
1. **Update `main_pipeline.py`** `get_user_profile()` to read `a1c_target_group` from the Gold table instead of hardcoding `dm_target_7`
2. **Implement weight goal join** in `create_weight_features()` to properly compute `has_weight_goal`, `weight_goal_type`, `is_within_maintenance_range`, `weight_change_lbs_14d`, `weight_change_pct_14d`
3. **Wire mental well-being 7-day flags** in logic engine to use the new `journal_entry_7d`, `meditation_opened_7d`, `action_plan_active` fields

### P1 - Find Missing Tables
4. **Content interaction table** -- needed for article/lesson/video tracking (Explore category + bonus points)
5. **AI meal plan table** -- needed for bonus point + positive action
6. **User focus/preferences table** -- needed for category filtering
7. **Glycemic-lowering medication type table** -- needed for glucose opportunity decision tree steps 2 & 3

### P2 - Improve Existing Logic
8. **Medication adherence** -- implement proper daily expected dose calculation using `frequencytype` and `frequencyvalue` from prescriptions
9. **Per-patient glucose targets** -- find high/low target columns or a separate table for DIP and non-DM classification
10. **Additional nutrient goals** -- once goal columns for fiber, sugar, etc. are available, add to nutrient target scoring
11. **`sodium` column** -- verify if `sodiumobservationstatus` in the food table is a value or a status flag

### P3 - Production Hardening
12. Run end-to-end test with the updated feature store on Databricks
13. Validate scoring output against the `Scoring_Criteria.csv` expected values
14. Deploy updated MLflow model with new feature version
15. Set up Databricks Workflow for daily Gold table refresh

---

## Enum Reference

| Table | Column | Mapping |
|-------|--------|---------|
| `elogbgentry` | `observationstatus` | 1=Active, 2=Completed |
| `elogbgentry` | `externalsourceid` | 18=CGM |
| `elogexerciseentry` | `exercisetype` | 30045=Meditation |
| `patienttargetsegment` | `a1ctarget` | 1=<7, 2=<8, 3=<6 |
| `actionplanprogress` | `actionplanstatus` | 1=Final (active), 2=Completed, 3=Deleted |
| `medadministration` | `statusid` | 1=Active, 2=Completed |
| `medprescription` | `statusid` | 1=Active, 2=Completed |
| `foodmoduleitem` | `activitytypeid` | Breakfast, Lunch, Dinner, Snack (exact values TBD) |
| `weightgoal` | `type` | lose, gain, maintain |
| `GuidedJourneyWeeksAndTasksDetail` | `isjourneycompleted` | 1=Active, 2=Completed, 3=Incomplete |
| `curatedvideositemdetail` | `statusid` | 1=Active, 2=Completed (needs verification) |
| `curatedvideosprogramdetail` | `statusid` | 1=Active, 2=Completed, 3=Stopped |
