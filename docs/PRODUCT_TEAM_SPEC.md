# SIMON Health Habits — Daily Insights Table
### Product Team Reference Sheet

---

## What Is This Table?

Every morning, an automated job generates a personalized health message for each SIMON patient and stores it here. You can query this table to retrieve the message and health rating for any patient on any given date.

---

## Table Location

```
<catalog>.<schema>.simon_healthy_habits_insights
```

| Detail | Value |
|--------|-------|
| Catalog | `<catalog>` |
| Schema | `<schema>` |
| Table | `simon_healthy_habits_insights` |
| Format | Delta Lake (queryable via SQL, Databricks, or any connected BI tool) |

> The location for this table is yet to be finalized. 

---

## Table Columns

| Column | Type | What It Contains |
|--------|------|-----------------|
| `patient_id` | STRING | The patient's unique ID |
| `insight_date` | DATE | The date this message was generated for |
| `insight` | STRING | The full personalised health message (~250 words) |
| `score_name` | STRING | The patient's daily health rating (see below) |
| `generated_at` | TIMESTAMP | When the row was written |

### `score_name` Values

| Value | Meaning |
|-------|---------|
| `Committed` | Patient is excelling across their health goals |
| `Strong` | Solid, dependable healthy habits |
| `Consistent` | Making a great effort to build routine |
| `Building` | Working to establish new healthy habits |
| `Ready` | Just getting started |

---

## When Is Data Available?

The job runs daily on a fixed schedule. The table is populated for the current day approximately:

| Task | Estimated Completion |
|------|---------------------|
| Feature store refresh (Notebook 1) | TBC |
| Insight generation + table write (Notebook 2) | TBC |

> Times above are estimates and will be updated after the first production run.

**Rule of thumb:** assume the table is fully populated and safe to read by the start of business each day.

---

## How to Query

### Get today's insight for a specific patient

```sql
SELECT
    patient_id,
    insight_date,
    score_name,
    insight
FROM <catalog>.<schema>.simon_healthy_habits_insights
WHERE patient_id   = '<PATIENT_ID>'
  AND insight_date = CURRENT_DATE()
```

---

## How to Run the Postman Collection

A Postman collection (`SIMON_Insights_Query_API.postman_collection.json`) is provided at the root of this repo. Import it into Postman and follow the steps below.

### Step 1 — Import the Collection

1. Open Postman → **Import** → drag and drop `SIMON_Insights_Query_API.postman_collection.json`
2. The collection will appear with 3 requests: `Get_Bearer_Token`, `Get_Todays_Insight_For_Patient`, and `Check_Statement_Status`

### Step 2 — Fill in the Collection Variables

Go to the collection → **Variables** tab and fill in the following:

| Variable | Where to get it | Example |
|----------|----------------|---------|
| `sql_warehouse_id` | Databricks → SQL Warehouses → your warehouse → **Connection details** → copy the last segment of the HTTP path (after `/sql/1.0/warehouses/`) | `abc123ef456` |
| `catalog` | TBC — provided by engineering team | `bronz_als_azuat2` |
| `schema` | TBC — provided by engineering team | `llm` |
| `patient_id` | The patient ID you want to query | `17014` |

> `databricks_host` and the service principal credentials are pre-filled. Do not change them.

### Step 3 — Get a Bearer Token

Run **`Get_Bearer_Token`** first. The token is automatically saved and valid for ~1 hour. Re-run this request if you start getting `401 Unauthorized` responses.

### Step 4 — Query the Insight

Run **`Get_Todays_Insight_For_Patient`**. Two outcomes:

- **Instant result (most common):** The `insight`, `score_name`, and other fields are printed in the Postman Console (`View → Show Postman Console`).
- **Still running:** The query returns a `RUNNING` or `PENDING` state. The `statement_id` is saved automatically — proceed to Step 5.

### Step 5 — Poll if Needed

If the query was still running, send **`Check_Statement_Status`** every few seconds until the state shows `SUCCEEDED`. The same result is then printed in the Console.

---

## Key Rules to Know

- **One row per patient per day.** If you query for a patient + date combination and get no result, either the job has not run yet for that day, or the patient had no health data recorded the previous day.
- **Data reflects yesterday's activity.** The message generated on March 10 summarises the patient's health data from March 9.
- **The table is safe to re-query.** The job uses upsert logic — running multiple times for the same date updates existing rows rather than creating duplicates.

---