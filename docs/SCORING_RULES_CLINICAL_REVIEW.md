# Daily Readiness Score — Clinical Rule Verification Guide

## How the Score Works

1. Up to **10 categories** are evaluated, each worth a maximum of **5 points** → **50 base points total**.
2. Categories are **included or excluded** based on (a) whether the user has set a focus area and (b) whether a required device/feature is connected.
3. The raw score is divided by the maximum possible score for that user (only included categories count), then converted to a **percentage**.
4. That percentage maps to one of five **rating tiers** shown to the user.
5. **Bonus points** are added on top but do not change the denominator — they can push a user into a higher tier.

---

## Rating Tiers (shown to user; score % is NOT shown)

| % of Max Score | Label | Description shown to user |
|---|---|---|
| 81 – 100% | **Committed** | You are committed to your healthy habits and seeing results. |
| 61 – 80% | **Strong** | Your healthy habits are solid and dependable. |
| 41 – 60% | **Consistent** | You are making a great effort to make your healthy habits a routine. |
| 21 – 40% | **Building** | You are working to create new healthy habits. |
| 1 – 20% | **Ready** | You are ready to choose your next healthy steps. |

---

## Category Inclusion Rules

A category is scored **only when both conditions are true**:

| Condition | Detail |
|---|---|
| **Focus area** | If the user has **no focus set**, all categories are included. If a focus is set, only categories matching that focus are included. |
| **Device / feature** | CGM categories require CGM connected. Steps requires a step tracker. Medications requires medications on file. |

### Focus Area → Category Mapping

| User Focus | Categories included |
|---|---|
| Weight | Weight |
| Glucose | Glucose (CGM) |
| Activity | Activity, Steps |
| Eating Habits | Food Logging, Nutrient Targets |
| Sleep | Sleep Duration, Sleep Rating |
| Medications | Medications |
| Anxiety | Mental Well-being |

---

## Category 1 — Weight (max 5 pts)

**Included when:** No focus set, OR Weight is a focus area.

### Sub-case A: Weight Loss Goal (`weight_goal_type = "lose"`)

| Data condition | Points |
|---|---|
| Weight decreased by **more than 1 lb** in last 14 days | **5** |
| Weight stayed within **±1 lb** in last 14 days | **3** |
| Weight gained **1 lb or more** in last 14 days | **0** |

### Sub-case B: Maintenance Goal (`weight_goal_type = "maintain"`)

| Data condition | Points |
|---|---|
| Weight stayed within **±3% of goal weight** in last 14 days | **5** |
| Weight gained or lost **more than 3%** of goal weight in last 14 days | **0** |

### Sub-case C: No Weight Goal

| Data condition | Points |
|---|---|
| Weight logged **at least once in last 7 days** | **5** |
| Weight logged **at least once in last 14 days** (but not in last 7) | **3** |
| Weight logged **at least once in last 30 days** (but not in last 14) | **1** |
| Not logged at all in last 30 days | **0** |

---

## Category 2 — Glucose / CGM (max 5 pts)

**Included when:** CGM is connected AND (no focus set OR Glucose is a focus area).

Score depends on yesterday's **Time in Range (TIR %)** and the user's A1C target group.

### A1C Target < 7 (`dm_target_7`) — default when no A1C target on file

| TIR yesterday | Points |
|---|---|
| ≥ 70% | **5** |
| ≥ 60% (but < 70%) | **3** |
| ≤ 50% | **1** |
| Between 51–59% | **0** |

### A1C Target < 8 (`dm_target_8`)

| TIR yesterday | Points |
|---|---|
| ≥ 50% | **5** |
| ≥ 40% (but < 50%) | **3** |
| ≤ 30% | **1** |
| Between 31–39% | **0** |

---

## Category 3 — Activity (max 5 pts)

**Included when:** No focus set, OR Activity is a focus area.  
Uses **total active minutes logged in the last 7 days**.

| Active minutes (last 7 days) | Points |
|---|---|
| ≥ 150 min | **5** |
| ≥ 120 min (but < 150) | **4** |
| ≥ 90 min (but < 120) | **3** |
| ≥ 60 min (but < 90) | **2** |
| ≥ 30 min (but < 60) | **1** |
| < 30 min / none logged | **0** |

---

## Category 4 — Steps (max 5 pts)

**Included when:** Step tracker is connected AND (no focus set OR Activity is a focus area).  
Uses **yesterday's step count**.

| Steps yesterday | Points |
|---|---|
| ≥ 10,000 | **5** |
| ≥ 6,000 (but < 10,000) | **3** |
| ≥ 2,000 (but < 6,000) | **1** |
| < 2,000 / none recorded | **0** |

---

## Category 5 — Food Logging (max 5 pts)

**Included when:** No focus set, OR Eating Habits is a focus area.  
Uses **number of meals logged yesterday**.

| Meals logged yesterday | Points |
|---|---|
| ≥ 3 meals | **5** |
| ≥ 2 meals (but < 3) | **3** |
| ≥ 1 meal (but < 2) | **1** |
| 0 meals logged | **0** |

---

## Category 6 — Daily Nutrient Targets (max 5 pts)

**Included when:** No focus set OR Eating Habits is a focus area, AND user has nutrient goals set.  
Looks at how many of the user's nutrient targets (protein, carbs, fat, calories) were met yesterday.

| Condition | Points |
|---|---|
| **All** nutrient targets met at **90–110%** of goal | **5** |
| **All** nutrient targets met at **≥ 60%** of goal | **3** |
| **All** nutrient targets met at **≥ 30%** of goal | **1** |
| Fewer than all targets reached those thresholds | **0** |
| No nutrient targets set | *Category excluded from scoring* |

---

## Category 7 — Sleep Duration (max 5 pts)

**Included when:** No focus set, OR Sleep is a focus area.  
Uses **hours of sleep logged yesterday**.

| Sleep duration yesterday | Points |
|---|---|
| ≥ 7 hours | **5** |
| ≥ 6 hours (but < 7) | **3** |
| ≥ 5 hours (but < 6) | **1** |
| < 5 hours / none logged | **0** |

---

## Category 8 — Sleep Rating (max 5 pts)

**Included when:** No focus set, OR Sleep is a focus area.  
Uses **the sleep rating logged yesterday** (scale 1–10).

| Sleep rating yesterday | Points |
|---|---|
| Rating = 10 | **5** |
| Rating ≥ 7 (but < 10) | **3** |
| Rating ≥ 4 (but < 7) | **1** |
| Rating ≤ 3 | **0** |
| No rating logged | **0** |

---

## Category 9 — Medications (max 5 pts)

**Included when:** Medications are on file AND (no focus set OR Medications is a focus area).  
Uses **average medication adherence over the last 7 days** (as a decimal, e.g. `0.75` = 75%).

| 7-day adherence | Points |
|---|---|
| 100% | **5** |
| ≥ 75% (but < 100%) | **3** |
| ≥ 50% (but < 75%) | **1** |
| < 50% | **0** |

---

## Category 10 — Mental Well-being (max 5 pts)

**Included when:** No focus set, OR Anxiety is a focus area.  
This is a **cumulative / additive** score — all qualifying events add up, capped at 5.

| Event | Points added |
|---|---|
| Has an **active Action Plan** | +3 |
| Journaled **at least once in last 7 days** | +1 |
| Opened **at least 1 meditation in last 7 days** | +1 |
| **Maximum** (cap) | **5** |

> Example: Active Action Plan (+3) + Journal entry (+1) = **4 pts**. With a meditation opened too = 5 pts (capped).

---

## Bonus Points (added on top; do NOT change denominator)

| Event | Points |
|---|---|
| Completed an exercise video | +3 |
| Generated an AI meal plan | +1 |
| Started an exercise program | +1 |
| Shopped for groceries online | +1 |
| Read an article | +1 |
| Completed a lesson | +1 |
| Watched a Learn video | +1 |

---

## Score Calculation Example

**Scenario:** User with no focus set, CGM connected, step tracker connected, medications on file.  
All 10 categories are included → max score = 50 pts.

| Category | Points Earned |
|---|---|
| Weight (no goal, logged 3 days ago) | 5 |
| Glucose (A1C < 7, TIR = 65% yesterday) | 3 |
| Activity (130 min this week) | 4 |
| Steps (7,500 steps yesterday) | 3 |
| Food Logging (2 meals yesterday) | 3 |
| Nutrient Targets (all at ≥ 60%) | 3 |
| Sleep Duration (6.5 hrs yesterday) | 3 |
| Sleep Rating (8 yesterday) | 3 |
| Medications (80% adherence, 7d) | 3 |
| Mental Well-being (active plan + journal) | 4 |
| **Base Total** | **34 / 50** |
| Bonus: completed exercise video | +3 |
| **Final Score** | **37 / 50 = 74%** |

**Rating: Strong** (74% falls in the 61–80% band)

---