# How the Logic Engine Works — Plain English Guide

## The Big Picture

Every morning SIMON needs to send each patient a short, personalized health message. That message has three parts:

1. **A rating** (Committed / Strong / Consistent / Building / Ready) — how well is the patient managing their health overall?
2. **1–2 positive callouts** — things the patient did well *yesterday*
3. **1 suggestion** — one thing they could do *today* to improve

The challenge is that different patients have different devices (CGM, step tracker), different health goals, different medications, and different focus areas. The logic engine is the code that figures out — deterministically, without any guesswork — exactly what facts to put in front of the AI so the AI only has to write the words, not make any decisions.

---

## Step 0 — The Data Foundation (Gold Table)

Before the logic engine runs, a daily batch job reads raw health data from 16+ source tables (glucose readings, steps, weight logs, sleep entries, medication records, etc.) and compresses it into one row per patient per day in the **Gold table**.

Think of the Gold table like a patient's daily report card — every relevant number already calculated and waiting:

| What it stores | Example |
|---|---|
| Time in glucose range yesterday | `tir_pct = 74.2` |
| Steps walked yesterday | `daily_step_count = 8432` |
| Sleep hours logged | `sleep_duration_hours = 6.5` |
| Medication adherence (7-day average) | `med_adherence_7d_avg = 0.87` |
| Active minutes this week | `active_minutes_7d_sum = 145` |
| Active focus areas | `user_focus = "Weight,Glucose"` |
| A1C target group | `a1c_target_group = "dm_target_7"` |
| …and ~80 more fields | |

The logic engine reads this single row and makes all its decisions from it.

---

## Step 1 — Load the Patient's Data (`UserContext`)

The engine packages the Gold table row into a Python object called `UserContext`. This is just a structured holder for all the patient's numbers and flags for that day.

Key things it knows about the patient:
- **Device flags** — does the patient have a CGM? A step tracker? Medications on their list?
- **Goal flags** — do they have a weight loss goal? A maintenance goal? An active Journey?
- **Yesterday's metrics** — glucose TIR, steps, sleep hours, meals logged, meds taken, etc.
- **Trends** — did sleep improve vs the day before? Did steps go up or down?
- **Focus areas** — a list like `["Weight", "Glucose"]` or `None` (meaning no filter, show everything)
- **A1C group** — determines which glucose thresholds apply (`dm_target_7`, `dm_target_8`, `dip`, or `non_dm`)

---

## Step 2 — Calculate the Daily Rating

The engine scores the patient across up to **10 health categories**, each worth up to 5 points (50 points total base, plus bonus points).

### Which categories are scored?

This is where **focus areas** matter. Every category asks: *"Should I include this for this patient today?"*

- If the patient has **no focus set** → all categories are included
- If the patient has **focus(es) set** → only categories linked to those focuses are included

| Focus Area | Categories Included in Scoring |
|---|---|
| Weight | Weight |
| Glucose | Glucose |
| Activity | Activity, Steps |
| Eating Habits | Food Logging, Nutrient Targets |
| Sleep | Sleep Duration, Sleep Rating |
| Medications | Medications |
| Anxiety | Mental Well-being |

A patient with both `Weight` and `Sleep` active would be scored only on Weight, Sleep Duration, and Sleep Rating — the other categories are skipped and don't affect their max score (so the percentage is still fair).

### How is each category scored?

Each category applies the business rules from the requirements document:

| Category | Top Score Condition |
|---|---|
| Weight | Lost 1+ lb in 14 days (loss goal) / stayed within 3% of goal (maintenance) / logged weight in 7 days (no goal) |
| Glucose | TIR ≥ 70% (for A1C <7 patients), ≥ 50% (A1C <8), ≥ 90% (non-DM) |
| Activity | 150+ active minutes logged in the last 7 days |
| Steps | 10,000+ steps yesterday (requires step tracker) |
| Food Logging | 3 meals logged yesterday |
| Nutrient Targets | All nutrient goals hit at 90–110% |
| Sleep Duration | 7+ hours slept |
| Sleep Rating | Rated sleep 7 or higher |
| Medications | 100% adherence over last 7 days |
| Mental Well-being | Active Action Plan + journaled this week + opened meditation this week |

Bonus points are added on top for extra activities like completing an exercise video (+3), generating an AI meal plan (+1), starting an exercise program (+1), etc.

### The final rating

The score is converted to a percentage of the maximum possible score for that patient, then mapped to a label:

| Score % | Label | Description |
|---|---|---|
| 81–100% | Committed | You are committed to your healthy habits and seeing results. |
| 61–80% | Strong | Your healthy habits are solid and dependable. |
| 41–60% | Consistent | You are making a great effort to make your healthy habits a routine. |
| 21–40% | Building | You are working to create new healthy habits. |
| 1–20% | Ready | You are ready to choose your next healthy steps. |

---

## Step 3 — Find All Eligible Positive Actions

The engine then scans through every possible positive action and checks whether the patient *qualifies* for it based on yesterday's data.

Examples of what it checks:

- **Glucose TIR met** — `tir_pct >= 70` (for an A1C <7 patient with a CGM)
- **Steps improved** — `daily_step_count > prev_day_steps` (requires step tracker)
- **Sleep hours met** — `sleep_duration_hours >= 7`
- **Medication adherence** — `took_all_meds == True && has_medications == True`
- **Meal logged** — `meals_logged_count >= 1`
- **Journey task completed** — `has_active_journey == True && journey_task_completed == True`
- **Weight progressing** — weight decreased (loss goal), or is within ±3% (maintenance goal)

If nothing qualifies at all, it falls back to "App login" — a generic encouragement for opening the app.

### Focus filtering

After finding all qualifying actions, any action whose category is **not** in the patient's active focus areas is removed. A patient focused on `Sleep` would not see glucose or step actions, even if they qualified.

---

## Step 4 — Find All Eligible Opportunities

Same process, but for the *suggestion* part of the message. The engine checks conditions like:

- **Weight** — hasn't logged weight in 6+ days
- **Glucose** — TIR below target, or too much time in high/low range
- **Activity** — fewer than 150 active minutes this week
- **Steps** — daily average below 6,000
- **Food** — didn't log food yesterday, or has nutrient goals they're not hitting
- **Sleep** — average under 7 hours or average rating under 7 over the past week
- **Medications** — less than 50% adherence in last 7 days + no reminders set
- **Mental Well-being** — hasn't opened a meditation / journaled / touched an action plan in 30 days

If absolutely nothing qualifies, it defaults to "browse the Explore section."

---

## Step 5 — Rank Everything by Priority

Both the actions and opportunities are ranked so the most important ones rise to the top. The priority score for each action is calculated like this:

```
base priority
+ Journey boost (if action is a Journey task completion)
+ CGM/good TIR boost (if glucose is on track and patient has CGM)
+ Weight goal boost (if patient has weight goal and action is food/activity/sleep/steps)
+ Medication boost (if patient has medications on list)
+ Focus area boost (if action's category matches one of the patient's active focuses)
+ "Not seen in 6 days" boost (variety — surface things the patient hasn't seen recently)
− Streak penalty (if patient is already excelling in this category 3 days in a row, deprioritize it)
```

The focus area boost uses a **weighted** system — currently **uniform** (all active focuses count equally). When the patient matches any active focus, they get the full +60 point focus boost. This design makes it easy to swap in a different strategy later (e.g., primary focus gets 70%, secondary focus gets 30%).

Equal-priority ties are broken with a **random jitter**, so the same patient doesn't always see the exact same action when multiple things qualify on the same day.

---

## Step 6 — Apply Frequency Rules and Select Final Content

After ranking, the engine picks:

- **Up to 2 positive actions** (with **category variety** — it tries not to pick two things from the same category)
- **Special CGM rule**: if the patient has a CGM and TIR is good, glucose is always included as one of the two actions (per requirements)
- **1 opportunity** (just the top-ranked one)

**Weight capping rule** is also enforced here:
- Weight can only appear in the message **2× per week maximum**
- Weight cannot appear on **back-to-back days**

This check is done against the `MessageHistory` object, which is loaded from the message history table before the engine runs.

---

## Step 7 — Hand Off to the AI

The engine returns a `SelectedContent` object containing:
- The rating label + description
- A time-appropriate greeting (Good morning / Good afternoon / etc.)
- 1–2 positive action text templates + the data to fill them in
- 1 opportunity suggestion text template

The AI (LLaMA 3.3 70B) then takes this structured input and writes the final ~100-word, 2-paragraph message. It only does the creative writing — all the decisions about *what* to write about have already been made by the logic engine.

---

## Does This Match the Original Requirements?

| Requirement | Implemented? | Where |
|---|---|---|
| Daily rating (Committed/Strong/Consistent/Building/Ready) | ✅ | `calculate_daily_rating()` |
| Score based on 10 health categories (5 pts each) | ✅ | `calculate_daily_rating()` |
| Focus area filtering — only show relevant categories | ✅ | `_is_category_allowed()`, `should_include_category()` |
| Multiple active focus areas supported | ✅ | `user_focus` is a list; union logic used |
| Anxiety focus maps to Mental Well-being category | ✅ | `FOCUS_CATEGORY_MAP` |
| Exclude CGM actions without CGM | ✅ | `has_device=user.has_cgm` guard |
| Exclude steps actions without step tracker | ✅ | `has_device=user.has_step_tracker` guard |
| Weight shown max 2×/week, never back-to-back | ✅ | `MessageHistory` + `get_eligible_positive_actions()` |
| CGM with good TIR → always include + extra action | ✅ | `select_content()` special CGM block |
| A1C-based glucose thresholds (7%, 8%, DIP, non-DM) | ✅ | `A1CTargetGroup` + `clinical_thresholds` config |
| Prioritize Journey actions | ✅ | `journey_active` priority boost (+100) |
| Prioritize glucose for CGM users | ✅ | `cgm_with_good_tir` boost (+90) |
| Prioritize food/activity/sleep for weight goal users | ✅ | `weight_goal_active` boost (+80) |
| Prioritize medications for patients with meds list | ✅ | `medications_on_list` boost (+70) |
| Prioritize focus-area actions | ✅ | `user_focus_area` boost (+60) |
| Prioritize actions not shown in last 6 days | ✅ | `not_shown_6_days` boost (+50) |
| Deprioritize category if excelling 3 days in a row | ✅ | `streak_override_days` penalty (−30) |
| Vary opportunities shown as much as possible | ✅ | Random jitter tiebreaker + streak penalty |
| Fall back to "browse Explore" if no opportunities | ✅ | `explore_browse` default |
| Fall back to "kudos for logging in" if no actions | ✅ | `app_login` fallback |
| Bonus points for extra activities (video, meal plan, etc.) | ✅ | Bonus section in `calculate_daily_rating()` |

### Gaps / Placeholders

| Item | Status |
|---|---|
| Article read / lesson completed / video watched bonus | ⚠️ Placeholder — requires a content interaction table not yet available |
| AI meal plan generated detection | ⚠️ Placeholder — not yet in Gold table |
| Focus area weighting (primary vs secondary) | ⚠️ Uniform for now — infrastructure to swap strategy is in place in `_get_focus_weights()` |
| 3-day streak detection per category | ⚠️ `category_streaks` is loaded from history but streak population logic in Gold table pipeline needs verification |
