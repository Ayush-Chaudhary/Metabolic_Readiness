# Scenario Picking Architecture & LangGraph Feasibility Analysis

**Date:** April 2, 2026  
**Author:** GitHub Copilot (Analysis)  
**Scope:** `src/logic_engine.py`, `src/insight_generator.py`, `src/main_pipeline.py`, `src/prompts.yml`

---

## 1. What is "Scenario Picking"?

The SIMON Health Habits system does **not** use pre-defined message templates or discrete scenarios. Instead, it assembles a personalized set of health facts for each user every day using a **deterministic rules engine**, then passes those facts to an LLM for natural language generation.

The "scenario" is the dynamically selected combination of:
- A **daily rating tier** (e.g., Committed, Strong, Consistent)
- Up to **2 positive actions** (things the user did well)
- **1 opportunity** (an area to improve)

---

## 2. The 5-Stage Pipeline

### Stage 1 — Calculate Daily Rating
**Function:** `LogicEngine.calculate_daily_rating()` in [logic_engine.py](../src/logic_engine.py)

- Scores the patient across **10 health categories**: glucose, weight, activity, steps, food, sleep, medications, mental wellbeing, explore, journey
- Each category contributes 0–5 points (50-point base max)
- **Focus area filtering** applies: a Weight-focused user only scores Weight + related categories
- Final score maps to a tier:

| Score % | Tier |
|---------|------|
| 81–100% | Committed |
| 61–80%  | Strong |
| 41–60%  | Consistent |
| 21–40%  | Building |
| 1–20%   | Ready |

---

### Stage 2 — Get Eligible Positive Actions
**Function:** `LogicEngine.get_eligible_positive_actions()`

Evaluates ~25 action conditions against yesterday's data. Examples:

| Action Key | Condition |
|------------|-----------|
| `glucose_tir_met` | `tir_pct >= 70%` (for DM <7% target) |
| `steps_target_met` | `daily_step_count >= 10,000` |
| `sleep_hours_met` | `sleep_duration_hours >= 7` |
| `medication_adherence` | `took_all_meds == True` |
| `weight_decreased` | `weight_change_pct < 0` (loss goal) |
| `journey_task_completed` | Journey active + task completed |

---

### Stage 3 — Get Eligible Opportunities
**Function:** `LogicEngine.get_eligible_opportunities()`

Evaluates ~20 opportunity conditions. Examples:

| Opportunity Key | Condition |
|-----------------|-----------|
| `weight_log_prompt` | `days_since_last_weight > 6` |
| `glucose_improve_tir` | `tir_pct < target` → 4-branch decision tree |
| `activity_be_active` | `weekly_active_minutes < 150` |
| `sleep_improvement` | `avg_sleep_hours < 7` (rotates 13 specific tips) |
| `food_start_logging` | No meals logged |

**Glucose Decision Tree** (for TIR < target):
```
1. No meals logged?                          → suggest logging food
2. Takes glycemic med but non-adherent?      → suggest taking medication
3. Takes med, adherent, 2+ meals logged?     → suggest contacting provider
4. Otherwise                                 → suggest paying attention to glucose
```

---

### Stage 4 — Apply Priority & Selection Rules
**Function:** `LogicEngine.select_content()`

**Priority weights:**

| Factor | Weight |
|--------|--------|
| Active journey | +100 |
| CGM with good TIR | +90 |
| Weight goal active | +80 |
| User focus area match | +60 |
| Not shown in 6 days (variety) | +50 |
| 3-day streak (over-reinforcement penalty) | −30 |
| Medication adherence | 0 (disabled per clinical feedback) |

**Selection rules:**
1. CGM users with TIR met or improved → glucose action is **always force-included**
2. Sort remaining candidates by priority (with random jitter to break ties)
3. Pick top 2 actions from **different categories** (variety enforcement)
4. Pick top 1 opportunity

**Frequency caps** (enforced via message history table):
- Weight: max 2×/week, never on consecutive days
- Any category: avoid repeat within 6-day lookback window

**Focus filter with fallback:**
- Only consider actions/opportunities matching the user's focus area
- If filtering eliminates all options, fall back to full unfiltered list (no empty messages)

---

### Stage 5 — Generate Natural Language
**Function:** `InsightGenerator.generate_insight()` in [insight_generator.py](../src/insight_generator.py)

1. Format system prompt (clinical coaching guidelines from `prompts.yml`)
2. Assemble user prompt with: greeting + positive action templates + opportunity text
3. Call **Databricks Llama 3.3 70B** (Foundation Model endpoint)
4. Optional: second LLM validation pass for grammar and reading level

**Output:** ~100-word single-paragraph message, e.g.:
> "Good morning. Your TIR was 75% yesterday, up 5% from the day before. You hit 165 active minutes this week. Try logging all three meals today to track your nutrition."

---

## 3. Architecture Diagram

```
User Yesterday's Data (Gold Table)
├─ tir_pct, step_count, active_minutes, sleep_hours
├─ meals_logged, med_adherence, weight_change, journey_tasks
└─ user_focus, a1c_target_group, has_cgm, ...

                  ↓  Logic Engine

  calculate_daily_rating()     →  Rating tier
  get_eligible_positive_actions() →  Array of action keys
  get_eligible_opportunities()    →  Array of opportunity keys
  select_content()
    ├─ Apply priority weights
    ├─ Check 6-day message history
    ├─ Enforce category variety
    └─ Return: rating + 2 actions + 1 opportunity

                  ↓  Insight Generator

  generate_insight()
    ├─ Format system + user prompt
    ├─ Call Llama 3.3 70B
    └─ [Optional] LLM validation pass

                  ↓

  Final ~100-word personalized message
```

---

## 4. Key Design Principles

| Principle | Detail |
|-----------|--------|
| **Logic-First, LLM-Second** | All routing decisions are deterministic Python; LLM only generates text |
| **Focus-Aware** | User's selected focus areas gate which categories are even evaluated |
| **Variety-First** | 6-day lookback + streak penalty prevents monotonous repetition |
| **Clinical Rigor** | 4 A1C target groups (dm_target_7, dm_target_8, dip, non_dm) with different glucose thresholds |
| **Auditable** | Every selection decision is traceable through explicit if/else conditions |
| **Fail-Safe** | Fallback defaults at every stage (e.g., `explore_browse` opportunity if nothing qualifies) |

---

## 5. LangGraph Feasibility Assessment

### What LangGraph Is Good For
LangGraph is designed for **agentic, multi-step LLM workflows** where:
- An LLM decides the next step (dynamic routing)
- There are tool-calling loops or retries
- Parallel LLM calls fan out and merge
- Multi-turn conversation state needs to persist

### Why It Doesn't Fit Here

| Dimension | Current System | LangGraph Equivalent |
|-----------|----------------|----------------------|
| Routing | Deterministic Python rules | Same rules, wrapped in graph nodes — no gain |
| LLM role in routing | **None by design** (clinical rules must be deterministic) | LangGraph requires LLM to drive routing to add value |
| State | `UserContext` dataclass + `MessageHistory` | `TypedDict` State — functionally identical |
| Branching | 1 conditional branch (CGM force-include) | Overkill for a single branch |
| Testability | Direct unit tests on each function | Must test nodes + edges + state transitions |
| New dependencies | None | `langgraph`, `langchain-core`, Databricks integration complexity |
| Clinical auditability | Readable if/else logic for clinical stakeholders | Graph node definitions harder to trace |

### Verdict: **No — not recommended**

The current architecture is a **rules engine → template assembly → single LLM call** pipeline. This is an ETL-style batch job, not an agentic workflow. Migrating to LangGraph would:
- Add complexity with no functional improvement
- Introduce new failure modes (graph state serialization on Databricks MLflow serving)
- Reduce testability and clinical auditability

---

## 6. Estimated Migration Effort (If Required)

If a migration were mandated nonetheless:

| Task | Estimated Effort |
|------|-----------------|
| Define LangGraph state schema (mirror `UserContext` + `SelectedContent`) | 1–2 days |
| Convert 5 pipeline stages into graph nodes | 2–3 days |
| Wire conditional edges (CGM branch, focus fallback, frequency cap) | 1–2 days |
| Replace Databricks SDK calls with LangChain chat model wrapper | 1–2 days |
| Rewrite unit tests for graph-based execution | 2–3 days |
| Integration testing on Databricks (MLflow serving compatibility) | 2–3 days |
| Debug state serialization in Model Serving | 1–2 days |
| **Total** | **~2–3 weeks (senior engineer)** |

The output would be functionally identical to today with more abstraction and more dependencies.

---

## 7. Recommended Improvements (Instead of LangGraph)

If the goal is to improve the system, these investments have higher ROI:

1. **Observability logging** — Log which actions were considered and why each was selected/rejected (supports debugging and clinical review)
2. **Unit test coverage** — The priority/selection logic in `select_content()` has many interacting rules; gap testing would catch edge cases
3. **Evaluation harness** — Wire the existing `evaluation_guidelines_metabolic_readiness.yaml` rubric into an automated regression suite
4. **A/B test framework** — Measure engagement lift from different priority weight configurations without redeploying code

---

## 8. File Reference

| File | Purpose |
|------|---------|
| [src/logic_engine.py](../src/logic_engine.py) | Core business logic: scoring, eligibility, priority selection |
| [src/insight_generator.py](../src/insight_generator.py) | LLM integration, prompt assembly, length validation |
| [src/main_pipeline.py](../src/main_pipeline.py) | Batch generation + MLflow Model Serving wrapper |
| [src/insight_generation_job.py](../src/insight_generation_job.py) | Daily batch job entry point |
| [src/prompts.yml](../src/prompts.yml) | Thresholds, templates, model config, system prompts |
| [docs/LOGIC_ENGINE_EXPLAINED.md](LOGIC_ENGINE_EXPLAINED.md) | Plain-English guide to all scoring rules |
| [docs/PRODUCT_TEAM_SPEC.md](PRODUCT_TEAM_SPEC.md) | Product requirements and message structure spec |
| [evaluation/evaluation_guidelines_metabolic_readiness.yaml](../evaluation/evaluation_guidelines_metabolic_readiness.yaml) | 14-criterion message quality evaluation rubric |
