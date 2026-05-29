import os
import duckdb
from pathlib import Path
from mistralai.client import Mistral

# --- Config ---
MISTRAL_MODEL = "mistral-small-latest"  # cheap + fast for testing; switch to mistral-large-latest for accuracy
MOTHERDUCK_DB = "md:poke_db"
MART_SCHEMA = "dbt_dev_marts"
SYSTEM_PROMPT_PATH = Path(__file__).parent / "system_prompt.md"


def load_system_prompt() -> str:
    return SYSTEM_PROMPT_PATH.read_text()


def generate_sql(client: Mistral, system_prompt: str, question: str) -> str:
    response = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        temperature=0.0,
    )
    raw = response.choices[0].message.content.strip()

    # strip markdown fences if the model wraps them
    if raw.startswith("```"):
        lines = raw.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        raw = "\n".join(lines).strip()

    return raw


def execute_sql(conn: duckdb.DuckDBPyConnection, sql: str):
    # prefix bare table names with the mart schema
    qualified_sql = sql
    for keyword in [
        "mart_pokemons", "mart_stats", "mart_strongest_pokemons",
        "mart_strongest_starters", "mart_moves", "mart_pokemon_moves",
        "mart_types", "mart_past_types", "mart_generations",
        "mart_version_groups", "mart_version_group_versions",
    ]:
        qualified_sql = qualified_sql.replace(keyword, f"{MART_SCHEMA}.{keyword}")

    return conn.sql(qualified_sql).df()


def main():
    # check env
    api_key = os.environ.get("MISTRAL_API_KEY")
    if not api_key:
        print("Error: set MISTRAL_API_KEY environment variable")
        return

    # init
    client = Mistral(api_key=api_key)
    system_prompt = load_system_prompt()
    conn = duckdb.connect(MOTHERDUCK_DB)

    print("Pokemon NL-to-SQL (type 'quit' to exit)")
    print("-" * 45)

    while True:
        question = input("\nQ: ").strip()
        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            break

        # generate SQL
        try:
            sql = generate_sql(client, system_prompt, question)
        except Exception as e:
            print(f"LLM error: {e}")
            continue

        print(f"\nSQL:\n{sql}")

        # execute
        try:
            df = execute_sql(conn, sql)
            print(f"\nResults ({len(df)} rows):")
            print(df.to_string(index=False))
        except Exception as e:
            print(f"\nSQL execution error: {e}")
            print("Try rephrasing your question.")

    conn.close()
    print("Bye!")


if __name__ == "__main__":
    main()
