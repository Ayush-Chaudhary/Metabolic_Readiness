# Missing Data - Catalog Search Request

Please search the Unity Catalog for tables or columns that provide the following data. For each item, I've described what the data represents and example column names to look for.

---

## 1. AI Meal Plan Generation Tracking
**What we need:** A table that records when a patient generated or used an AI-generated meal plan.
**Look for:** Tables with names like `mealplan`, `ai_meal`, `meal_suggestion`, `generated_meal`
**Key columns:** `patientid`, a timestamp column, possibly a `statusid` or `type` column
**Catalogs to check:** `bronz_als_azdev24`, `bronz_als_azuat2`
**Schemas to check:** `trxdb_dsmbasedb_user`, `trxdb_dsmbasedb_userengagement`

---

## 2. Content Interaction Tracking (Articles / Lessons / Videos)
**What we need:** A table that records when a patient reads an article, completes a lesson, or watches a video (content/education library interactions - NOT the exercise video program which is already found).
**Look for:** Tables with names like `articleread`, `contentview`, `lessonprogress`, `educationcontent`, `contentengagement`, `usercontentinteraction`, `libraryitem`
**Key columns:** `patientid`, `createddatetime` or `vieweddatetime`, a `contenttype` or `itemtype` column (article / lesson / video), possibly `statusid`
**Catalogs to check:** `bronz_als_azdev24`, `bronz_als_azuat2`
**Schemas to check:** `trxdb_dsmbasedb_userengagement`, `trxdb_dsmbasedb_user`

---

## 3. User Focus / Health Goal Preference
**What we need:** Where the patient's selected focus area is stored — e.g., whether they have chosen to focus on Weight, Glucose, Activity, etc. as their primary health goal.
**Look for:** Tables or columns like `userfocus`, `focusarea`, `healthgoal`, `primarygoal`, `userpreference`, `patientpreference`, `patientprofile`
**Key columns:** `patientid`, a focus/goal type column (values like: Weight, Glucose, Activity, Steps, Nutrition, Sleep, Medications, Mental)
**Catalogs to check:** `bronz_als_azdev24`, `bronz_als_azuat2`
**Schemas to check:** `trxdb_dsmbasedb_user`, `trxdb_dsmbasedb_userengagement`

---

## 4. Glycemic-Lowering Medication Identification
**What we need:** A way to identify which medications in a patient's prescription list are glycemic-lowering (e.g., metformin, GLP-1 agonists, insulin, SGLT2 inhibitors, sulfonylureas). This could be a medication type/category column on the existing prescription table, or a separate drug lookup table.
**Existing prescription table:** `bronz_als_azdev24.trxdb_dsmbasedb_observation.medprescription`
**Look for:** A `medicationtype`, `drugcategory`, `therapeuticclass`, or `ndc` column on `medprescription`, OR a separate drug/formulary lookup table
**Tables to check:** `medprescription`, `medication`, `drug`, `formulary`, `rxnorm`, `medcategory`
**Schemas to check:** `trxdb_dsmbasedb_observation`, `trxdb_dsmbasedb_user`

---

## 5. Medication Reminders Setting
**What we need:** A flag or setting that indicates whether a patient has medication reminders enabled in the app.
**Look for:** Tables like `userreminder`, `notificationsetting`, `usernotification`, `reminderpreference`, `usersetting`, `apppreference`
**Key columns:** `patientid`, a reminder enabled/disabled flag (boolean or statusid), possibly a `remindertype` column
**Catalogs to check:** `bronz_als_azdev24`, `bronz_als_azuat2`
**Schemas to check:** `trxdb_dsmbasedb_user`, `trxdb_dsmbasedb_userengagement`

---

## 6. Per-Patient Glucose Target Range
**What we need:** Personalized low and high glucose target values per patient. Currently we hardcode 70–180 mg/dL for all patients. DIP (Diabetes in Pregnancy) patients should use 70–140. There may also be a patient type or condition flag.
**Existing table to check for new columns:** `bronz_als_azdev24.trxdb_dsmbasedb_observation.elogbgentry` — look for `targetlow`, `targethigh`, `glucoselow`, `glucosehigh` columns
**Also look for:** A separate table like `patienttarget`, `glucosetarget`, `patientcondition`, `diabetestype`, `programtype`
**Key columns:** `patientid`, a low threshold column, a high threshold column, possibly a patient type/condition (DM, DIP, pre-DM, non-DM)

---

## 7. DIP / Non-DM Patient Classification
**What we need:** A table or column that identifies a patient's diabetes classification: Type 1 DM, Type 2 DM, DIP (Diabetes in Pregnancy), pre-diabetes, or non-DM. This affects which A1C target applies.
**Existing table:** `bronz_als_azdev24.trxdb_dsmbasedb_observation.patienttargetsegment` — only has A1C enum (1=<7, 2=<8, 3=<6), no DIP or non-DM mapping
**Look for:** Tables like `patientdiagnosis`, `diagnosis`, `patientcondition`, `icd10`, `conditiontype`, `patienttype`, `programenrollment`
**Key columns:** `patientid`, a diagnosis or condition type column (ICD-10 code or program type)

---

## 8. Exercise Video Progress Percentage
**What we need:** How far along (% complete) a patient is through an individual exercise video — not just whether they completed it (statusid=2), but a numeric progress value.
**Existing table:** `bronz_als_azdev24.trxdb_dsmbasedb_user.curatedvideositemdetail`
**Look for:** A `percentcomplete`, `progress`, `watchedpercent`, `completionpct`, `durationwatched` column on that table or a related child table

---

## 9. Sodium Value in Food Log
**What we need:** Verification of which column in the food log table holds the actual sodium value in mg. The column `sodiumobservationstatus` sounds like a status flag, not a value.
**Existing table:** `bronz_als_azdev24.trxdb_dsmbasedb_observation.foodmoduleitem`
**Look for:** Additional columns like `sodium`, `sodiumvalue`, `sodium_mg` — or confirm what `sodiumobservationstatus` actually contains

---

## 10. Nutrient Goal Columns (Fiber, Sugar, Sodium, etc.)
**What we need:** Daily goal targets for nutrients beyond the four we already have (protein, fat, calories, carbs). Specifically: fiber, sugar, added sugar, sodium, cholesterol, potassium.
**Existing table:** `bronz_als_azuat2.trxdb_dsmbasedb_user.patientgoaldetails` — currently only has `protein`, `fat`, `calories`, `carbs`
**Look for:** Additional columns on `patientgoaldetails`, or a separate table like `nutrientgoal`, `patientnutritiongoal`
**Columns needed:** `fiber_goal`, `sugar_goal`, `sodium_goal`, `cholesterol_goal`, `potassium_goal`

---

## Summary Table

| # | Data Needed | Priority | Likely Schema |
|---|-------------|----------|---------------|
| 1 | AI meal plan generation tracking | MEDIUM | trxdb_dsmbasedb_user |
| 2 | Article / lesson / video content interactions | MEDIUM | trxdb_dsmbasedb_userengagement |
| 3 | User's selected focus area / health goal preference | MEDIUM | trxdb_dsmbasedb_user |
| 4 | Glycemic-lowering medication type / category | HIGH | trxdb_dsmbasedb_observation |
| 5 | Medication reminders enabled flag | LOW | trxdb_dsmbasedb_user |
| 6 | Per-patient glucose target range (low/high) | MEDIUM | trxdb_dsmbasedb_observation |
| 7 | Patient diabetes classification (DM / DIP / non-DM) | MEDIUM | trxdb_dsmbasedb_observation |
| 8 | Exercise video % completion progress | LOW | trxdb_dsmbasedb_user |
| 9 | Sodium value column in food log (verify sodiumobservationstatus) | LOW | trxdb_dsmbasedb_observation |
| 10 | Nutrient goal columns for fiber, sugar, sodium, etc. | LOW | trxdb_dsmbasedb_user |
