# NL-to-SQL Design

This document describes the architecture and prompt design for the natural language to SQL feature — the "Ask the Pokedex" page in the Streamlit app.

## What it does

Users type a question in plain English (e.g. "Which fire-type pokemon has the highest attack?") and the system generates a SQL query against the mart tables, executes it on MotherDuck, and returns the results.

## Why this matters

The mart layer holds rich, queryable Pokemon data. But writing SQL requires knowing the schema. This feature makes the data accessible to anyone who can ask a question.

---

## Architecture decision

**Chosen approach: Option A — Direct Prompt.**

The full mart schema and a system prompt are sent with every LLM call. No RAG, no table selection step, no fine-tuning.

**Why this works here:**
- Only 11 mart tables, ~2K tokens of schema DDL total
- Token cost per question: ~$0.001-0.005
- RAG, two-stage retrieval, or agent patterns are overkill at this scale
- Simpler architecture = fewer failure modes

**Alternatives considered and rejected:**

| Option | Approach | Why not |
|--------|----------|---------|
| B | Two-stage (select tables, then generate SQL) | Unnecessary — all tables fit in context |
| C | RAG-based schema retrieval | Adds vector DB complexity for no accuracy gain |
| D | Agent/tool-use pattern | Over-engineered for a read-only SQL generation task |
| E | Fine-tuned model | Requires training data we don't have; maintenance burden |

---

## Prompt structure

The prompt follows this order (research shows this ordering maximizes accuracy):

```
1. System instruction  — role, SQL dialect, guardrails
2. Schema context      — full mart DDL with column descriptions
3. Few-shot examples   — 5-10 (question, SQL) pairs
4. User question       — the actual natural language input
```

### System instruction

Defines:
- **Role:** "You are a SQL assistant for a Pokemon database"
- **SQL dialect:** DuckDB
- **Domain scope:** Pokemon data only — refuse anything else
- **Output format:** Return only the SQL query, no explanation

### Schema context

The full mart schema DDL with column descriptions and relationships. Source of truth: [`docs/data-dictionary.md`](data-dictionary.md).

Schema is loaded at app startup and injected into every prompt. The LLM is stateless — no conversation memory between questions.

### Few-shot examples

5-10 representative (question, SQL) pairs covering:
- Simple lookups ("What is Pikachu's weight?")
- Aggregations ("Average attack by generation")
- Joins ("Which moves can Charizard learn?")
- Filters on categorical columns ("All fire-type pokemon")
- Ranking queries ("Top 10 pokemon by total stats")
- Edge cases: NULL handling (status moves have no power), multi-type pokemon

Examples live in `nl_to_sql/` alongside the prompt template.

---

## Guardrails

Three-tier question handling:

| Question type | Example | System behavior |
|--------------|---------|-----------------|
| Out of domain | "What's the weather?" | Refuse: "I can only answer questions about Pokemon data." |
| In domain, not in data | "What are Pikachu's EV yields?" | Explain: "The database tracks base stats, types, and moves but not EV yields." |
| Answerable | "Strongest water-type?" | Generate SQL and return results |

Additional safety rules:
- Never generate DML (INSERT, UPDATE, DELETE)
- Never generate DDL (CREATE, DROP, ALTER)
- Only SELECT statements against `dbt_dev_marts` schema

---

## Error handling

If the generated SQL fails on execution:
1. Send the error message back to the LLM with the original question
2. Ask it to fix the query (one retry only)
3. If it fails again, show the user a friendly error

This single-retry loop catches most self-correctable mistakes (typos in column names, wrong join keys).

---

## Stack

| Component | Role |
|-----------|------|
| Streamlit | Chat UI (`st.chat_input`, `st.chat_message`) |
| Claude API | LLM for SQL generation |
| DuckDB (MotherDuck) | Query execution |
| `nl_to_sql/` | Prompt templates, schema extractor, few-shot examples |

---

## File layout

```
nl_to_sql/
  schema_extractor.py    # Pulls mart schema from MotherDuck
  system_prompt.md       # Prompt template (planned)
  examples.yml           # Few-shot (question, SQL) pairs (planned)

streamlit/
  pages/ask.py           # Chat UI (planned)
```
