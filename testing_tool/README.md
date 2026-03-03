# Metabolic Readiness — Clinical Testing Tool

A Streamlit-based testing tool for the clinical team to generate and evaluate
SIMON Health Habits insight messages using **synthetic patient data**.

---

## Authentication — How Credentials Work

Databricks credentials (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) are loaded
**automatically** — testers never enter them in the app UI.

| Environment | How credentials are set |
|-------------|------------------------|
| **Databricks App** | Injected automatically by the platform |
| **Local development** | Create a `.env` file (see Step 2 below) |

The sidebar shows a 🟢 green status when the LLM endpoint is reachable, or an
⚠️ warning (mock responses) when credentials are not configured.

---

## Option A: Local Development

### Step 1 — Install dependencies

```powershell
# From the project root, activate the existing venv
.\simon\Scripts\Activate.ps1

# Install testing tool dependencies
pip install streamlit openpyxl
```

### Step 2 — Add your Databricks credentials as a local `.env` file

```powershell
# Copy the template
Copy-Item testing_tool\.env.example testing_tool\.env
notepad testing_tool\.env
```

Edit `.env` to contain your real values:
```
DATABRICKS_HOST=https://adb-2008955168844352.12.azuredatabricks.net
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

**Where to get your Personal Access Token:**
1. Open your Databricks workspace in a browser
2. Click your profile icon (top-right) → **Settings**
3. Go to **Developer** → **Access Tokens** → **Generate New Token**
4. Name it (e.g. `testing-tool-local`), set an expiry, click **Generate**
5. Copy the `dapi...` value into `.env`

> ⚠️ `.env` is git-ignored. Never commit it or share it.

### Step 3 — Run the app

```powershell
cd testing_tool
..\simon\Scripts\streamlit.exe run app.py
```

Opens at `http://localhost:8501`. The sidebar shows 🟢 if credentials loaded correctly.

---

## Option B: Deploy as a Databricks App (Shareable Team URL)

Databricks Apps runs the Streamlit app inside your workspace, injects credentials
automatically, and gives the clinical team a single shareable URL — no localhost,
no individual token setup.

### Step 1 — Verify the Databricks CLI is installed and configured

```powershell
databricks --version          # Should print a version number
databricks workspace ls /     # Should list workspace contents
```

If not installed:
```powershell
pip install databricks-cli
databricks configure --token
# Prompt 1 → Host:  https://adb-2008955168844352.12.azuredatabricks.net
# Prompt 2 → Token: your Personal Access Token (dapi...)
```

### Step 2 — Sync the project files to the workspace

The bundle is already configured in `databricks.yml` at the project root.

```powershell
# From C:\Users\achaudhary\WelldocProjects\Metablic_Readiness
databricks bundle deploy
```

This uploads all files to:
`/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/`

Verify it landed:
```powershell
databricks workspace ls "/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/testing_tool"
```

You should see `app.py`, `backend.py`, `config.py`, `app.yaml`, etc.

### Step 3 — Create the App in the Databricks UI

1. Open your workspace: `https://adb-2008955168844352.12.azuredatabricks.net`
2. In the left sidebar click **Compute**
3. At the top of the page click the **Apps** tab
4. Click **Create App**
5. Fill in:
   - **App name**: `metabolic-readiness-tester`
   - **Description**: Clinical testing tool for SIMON Health Habits insights
6. Click **Create**

### Step 4 — Point the App to the source code

Inside the newly created App settings:

1. Under **Source code**, click **Edit**
2. Select **Workspace** as the source type
3. Navigate to:
   `/Workspace/Users/achaudhary@welldocinc.com/Welldoc_4_0/Metabolic_Readiness/testing_tool`
4. Confirm `app.yaml` is detected as the app configuration file
5. Click **Save**

### Step 5 — Deploy

Click the **Deploy** button. Databricks will:
1. Pull source from the workspace path
2. Install packages from `requirements.txt`
3. Start the Streamlit server in a managed container
4. Inject `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as environment variables automatically

Deployment takes 2–4 minutes. Wait for a green **Running** badge.

### Step 6 — Share the URL

Click **View App**. The URL looks like:
```
https://adb-2008955168844352.12.azuredatabricks.net/apps/metabolic-readiness-tester
```

Copy and share this URL with the clinical team. Any workspace member can open it
directly — no additional login or token setup needed on their end.

> **If a tester is not yet a workspace member:** go to
> **Settings → Identity & Access → Users → Add User**, add their work email,
> and assign the **User** role.

### Step 7 — Redeploy after code changes

Every time you edit `app.py`, `backend.py`, `config.py`, or `prompts.yml`:

```powershell
# Re-sync files to workspace
databricks bundle deploy
```

Then in the Databricks UI: **Apps → metabolic-readiness-tester → Actions → Redeploy**

The app restarts within ~60 seconds with the new code.

---

## File Structure

```
testing_tool/
├── app.py            # Streamlit UI (main entry point)
├── backend.py        # Synthetic data generator + pipeline runner
├── config.py         # Reads DATABRICKS_HOST/TOKEN from env vars or .env
├── requirements.txt  # Python dependencies
├── app.yaml          # Databricks App deployment config
├── .env.example      # Template — copy to .env for local dev
├── .env              # Your local credentials (git-ignored, never commit)
├── test_log.xlsx     # Auto-created on first feedback submission
└── README.md         # This file
```

Parent files used by the tool (must be in the same workspace path):
```
../logic_engine.py      # Business logic
../insight_generator.py  # LLM integration
../prompts.yml           # Thresholds, prompts, scoring criteria
```

---

## Using the App

1. Set your **Tester Name** in the sidebar
2. Choose a **patient profile** (A1C group, focus area, goals, trackers)
3. Pick a specific **scenario** from any of the 11 scenario dropdowns
4. (Optional) Override model parameters: Temperature, Max Tokens, Max Positive Actions
5. Click **Generate Insight** — view the message, scoring, actions, and debug info
6. Rate the output and submit feedback — it appends to `test_log.xlsx`
7. Download the log from the sidebar at any time

## Scenario Dropdown Reference

| Dropdown | Controls |
|----------|----------|
| **Glucose Performance** | TIR %, glucose high/low percentages |
| **Daily Steps** | Step count (randomized within range) |
| **Weekly Activity** | Active minutes daily + 7-day total |
| **Sleep Quality** | Sleep hours + sleep rating |
| **Food Logging** | Meals logged, nutrient targets met |
| **Medication Adherence** | Adherence %, took-all-meds flag |
| **Weight Trend** | Weight change, logging recency |
| **Mental Wellbeing** | Meditation, journaling, action plans |
| **Journey** | Active journey + task completion |
| **Exercise Video** | Video completion status |
| **Exercise Program** | Program activity and progress |

## Pipeline Flow

```
Scenario Dropdowns → Synthetic UserContext → LogicEngine.select_content()
    → InsightGenerator.generate_insight() → Display + Feedback → Excel Log
```

## Excel Log Columns

Every test run (after submitting feedback) appends a row to `test_log.xlsx`:

- Timestamp, tester name
- All configuration choices (scenarios, model parameters)
- Generated message, rating, actions and opportunities used
- Tester feedback: quality (1–5), clinical accuracy, tone, actionability, free-text notes

## Configuration

`prompts.yml` in the parent directory controls:
- Clinical thresholds (glucose TIR targets, step targets, etc.)
- Prompt templates (system prompt, user prompt, action/opportunity templates)
- Scoring criteria (10-category health management score)
- Priority rules and message history rules

To modify thresholds or prompts, edit `../prompts.yml` — changes take effect
on the next "Generate Insight" click.
