# SIMON Health Habits — Personalized Messaging
### Stakeholder Overview

---

## What Is This Project?

**SIMON Health Habits** is an AI-powered system that sends every patient a personalized daily health message — like a friendly, knowledgeable health coach checking in each morning.

Rather than sending the same generic tip to everyone, the system looks at each individual's actual health data from the previous day and crafts a message that is specific to *them* — celebrating what they did well and suggesting one realistic thing they can do today to keep improving.

---

## Why Does It Matter?

Patient engagement drops when messages feel irrelevant or repetitive. This system solves that by making every message feel earned and personal:

- A patient who hit their step goal hears about *their* steps — not glucose tips they don't need.
- A patient with diabetes and a CGM device gets glucose insights tailored to *their* A1C target.
- A patient on a weight-loss journey sees encouragement tied to *their* actual weight progress.

The result is a message that feels like it came from someone who was watching out for them — because, in a sense, it was.

---

## How It Works (The Simple Version)

The system runs automatically every day in three stages:

```
Patient Health Data  →  Smart Rules Engine  →  AI Writing  →  Daily Message
```

### Stage 1 — Gather the Data
Every night, the system collects data logged by patients in the SIMON app: glucose readings, steps walked, meals logged, sleep hours, medications taken, weight entries, and more. All of this is organized into a clean daily summary for each patient.

### Stage 2 — Make the Decisions (Rules Engine)
A carefully designed rules engine — built on clinical guidelines — reads each patient's daily summary and decides:

- **How is this patient doing overall?** → assigns a rating from *Committed* down to *Ready*
- **What did they do well yesterday?** → picks 1–2 specific accomplishments to celebrate
- **What's one thing they should focus on today?** → picks the most relevant suggestion

Importantly, these decisions are made by deterministic code — not by AI guessing. Every choice can be traced and audited.

### Stage 3 — Write the Message (AI)
Once the rules engine has decided *what* to say, an AI language model (Meta Llama 3.3 70B, hosted securely on Databricks) turns those facts into a warm, natural-sounding paragraph — roughly 250 words, conversational in tone, clinical in accuracy.

---

## Key Features

### Personalized Health Rating
Every message opens with one of five ratings that reflects the patient's recent performance across all their active health goals:

| Rating | What It Means |
|--------|---------------|
| **Committed** | Patient is excelling — habits are consistent and producing results |
| **Strong** | Solid, dependable healthy habits |
| **Consistent** | Making a great effort to build routine |
| **Building** | Working to establish new healthy habits |
| **Ready** | Just getting started — ready to take the next step |

The rating is calculated from up to **10 health categories**: glucose, activity, steps, weight, food logging, nutrition targets, sleep duration, sleep quality, medication adherence, and mental well-being.

Crucially, **only the categories relevant to the patient's personal focus areas are included** — so a patient focused on Sleep and Weight is scored only on those areas, keeping the rating fair and meaningful.

---

### Celebrates Real Accomplishments
The system scans for over **25 types of positive actions** a patient may have taken yesterday. Examples:

- Hit their daily step goal (10,000+ steps)
- Kept blood glucose in their target range
- Slept 7+ hours
- Logged all their meals
- Completed an exercise video
- Took all their medications
- Reached a weight milestone
- Completed a Guided Journey task
- Wrote a journal entry

Only things the patient *actually did* are mentioned — the system never fabricates praise.

---

### Offers One Focused Suggestion
Rather than overwhelming patients with a list of things to improve, the system picks a single, actionable opportunity — the most relevant one based on their data. Examples:

- "You haven't logged your weight in a week — try checking in today"
- "Your glucose has been spending more time above range — let's look at what's happening after meals"
- "Your active minutes are below your weekly target — even a 10-minute walk counts"

---

### Respects Patient Preferences and Context
The system is aware of each patient's unique situation:

- **Device awareness** — Different messages for patients with a CGM vs. a standard glucometer; different advice for patients with a connected step tracker
- **Goal awareness** — Patients with a weight-loss goal see different guidance than maintenance or no-goal patients
- **A1C target awareness** — Clinical glucose thresholds are adjusted per the patient's prescribed A1C target
- **Focus area filtering** — Patients who have set a focus (e.g., "I want to focus on Sleep") only see insights relevant to that area

---

### Avoids Repetition
The system keeps a history of every message sent and uses it to keep content fresh:

- The same topic won't dominate the message more than **twice per week**
- Topics the patient hasn't seen in **6 or more days** get a higher chance of surfacing again
- If a patient is already excelling in one area for **3 days in a row**, the system rotates attention to something new — so the message always adds value rather than repeating compliments

---

### Clinical Rigor Built In
All health thresholds used in this system are grounded in clinical guidelines:

- Glucose Time-in-Range targets aligned to A1C goals (e.g., ≥70% TIR for A1C <7 patients)
- Medication adherence measured over a rolling 7-day window
- Activity benchmarks set at 150 active minutes per week (standard recommendation)
- Sleep target set at 7+ hours per night

These rules are maintained in a single configuration file, so clinical teams can review and adjust them without touching the underlying code.

---

## What the System Tracks

| Health Area | What's Measured |
|-------------|-----------------|
| **Glucose** | Time in range, high/low excursions, CGM usage |
| **Activity** | Active minutes per week, exercise type |
| **Steps** | Daily step count, trend vs. prior day, weekly average |
| **Weight** | Progress toward loss/maintenance goal, recent logging |
| **Food** | Meals logged, calories, carbs, protein, fat targets |
| **Sleep** | Hours slept, self-rated sleep quality |
| **Medications** | Daily adherence, 7-day average |
| **Mental Well-being** | Meditation sessions, journaling, action plan activity |
| **App Engagement** | Guided Journeys, exercise programs, grocery planning |

---

## How It's Deployed

The system runs on **Databricks** (Microsoft Azure), a secure, enterprise-grade cloud data platform. It is:

- **HIPAA-compatible infrastructure** — patient data stays within the existing secure environment
- **Fully automated** — runs on a daily schedule, no manual intervention needed
- **Scalable** — designed to process messages for thousands of patients in a single batch
- **API-ready** — also available as a real-time endpoint for on-demand message generation

---

## Current Status

The core system is **built and working end-to-end**. The pipeline successfully:

- Ingests data from 16+ source tables
- Calculates scores across all 10 health categories
- Selects and ranks positive actions and suggestions
- Generates natural-language messages via the LLM
- Serves messages through a Databricks API endpoint

A small number of data enhancements are in progress (e.g., connecting user focus-area preferences, completing the weight goal integration) before the system moves to full production deployment.

---

## In Summary

SIMON Health Habits Personalized Messaging turns raw health data into a daily, personal, clinically grounded conversation with each patient — at scale, automatically, and without sounding like a form letter. It's a coach in every patient's pocket, powered by the data they're already generating.

