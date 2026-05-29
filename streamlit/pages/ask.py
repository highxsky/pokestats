import re
from pathlib import Path

import streamlit as st
from openai import OpenAI

import db
from config import SCHEMA_MARTS

# Open-source model served via Groq's OpenAI-compatible endpoint.
MODEL = "llama-3.3-70b-versatile"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
SYSTEM_PROMPT = (Path(__file__).parent.parent / "system_prompt.md").read_text()

# Bare mart table names the model may reference; prefixed with the live schema before execution.
MART_TABLES = [
    "mart_pokemons", "mart_stats", "mart_strongest_pokemons",
    "mart_strongest_starters", "mart_moves", "mart_pokemon_moves",
    "mart_types", "mart_past_types", "mart_pokemon_species",
    "mart_generations", "mart_version_groups", "mart_version_group_versions",
]


@st.cache_resource
def get_client():
    return OpenAI(api_key=st.secrets["groq"]["api_key"], base_url=GROQ_BASE_URL)


def generate_sql(question: str) -> str:
    resp = get_client().chat.completions.create(
        model=MODEL,
        temperature=0.0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
    )
    sql = resp.choices[0].message.content.strip()
    if sql.startswith("```"):  # strip fences if the model adds them
        sql = "\n".join(l for l in sql.splitlines() if not l.startswith("```")).strip()
    return sql


def qualify(sql: str) -> str:
    for t in MART_TABLES:
        sql = re.sub(rf"(?<!\.)\b{t}\b", f"{SCHEMA_MARTS}.{t}", sql)
    return sql


def is_safe(sql: str) -> bool:
    stripped = sql.strip().rstrip(";").lstrip("(")
    if not re.match(r"(?i)^\s*(select|with)\b", stripped):
        return False
    banned = r"(?i)\b(insert|update|delete|drop|alter|create|truncate|attach|copy|pragma|call)\b"
    return re.search(banned, sql) is None


st.title("Ask the Pokédex")
st.caption("Ask a question in plain English — it's answered with live SQL against the marts.")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Replay history
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        if m.get("sql"):
            st.code(m["sql"], language="sql")
        if m.get("df") is not None:
            st.dataframe(m["df"], use_container_width=True)
        if m.get("text"):
            st.markdown(m["text"])

if question := st.chat_input("e.g. What are the 5 strongest fire types?"):
    st.session_state.messages.append({"role": "user", "text": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        sql = generate_sql(question)

        # Model returned a refusal / non-SQL message rather than a query.
        if not re.search(r"(?i)\b(select|with)\b", sql):
            st.markdown(sql)
            st.session_state.messages.append({"role": "assistant", "text": sql})
        elif not is_safe(sql):
            msg = "I can only run read-only SELECT queries."
            st.warning(msg)
            st.session_state.messages.append({"role": "assistant", "text": msg})
        else:
            final_sql = qualify(sql)
            st.code(final_sql, language="sql")
            try:
                result = db.run_query(final_sql)
                st.dataframe(result, use_container_width=True)
                st.session_state.messages.append(
                    {"role": "assistant", "sql": final_sql, "df": result}
                )
            except Exception as e:
                err = f"Query failed: {e}"
                st.error(err)
                st.session_state.messages.append(
                    {"role": "assistant", "sql": final_sql, "text": err}
                )
