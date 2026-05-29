# Local Development

Full setup guide for developing on this project. For a quick start, see the [README](../README.md).

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed
- A [MotherDuck](https://motherduck.com) account with a service token
- Python 3.11+

---

## Airflow

### Start Airflow

```bash
# Set your MotherDuck token (used by the Airflow connection)
export motherduck_token=your_token_here

# Start Airflow locally via Astronomer
astro dev start
```

Airflow UI: **http://localhost:8080** (admin / admin)

### Airflow connection

The DAGs use a DuckDB connection named `motherduck_conn`. This is configured automatically via the `motherduck_token` environment variable in the Astronomer runtime.

If you need to set it manually in the Airflow UI:
- **Conn ID:** `motherduck_conn`
- **Conn Type:** DuckDB
- **Host:** `md:poke_db?motherduck_token=<your_token>`

### Useful commands

```bash
astro dev restart    # Restart after changing requirements.txt or Dockerfile
astro dev stop       # Stop all containers
astro dev logs       # View scheduler/webserver logs
```

---

## dbt

### Run dbt locally

```bash
cd include/transforms

# Install dependencies
dbt deps

# Run all models
dbt run --target dev

# Run a specific model and its downstream dependencies
dbt run --select stg_pokemons+

# Full refresh (rebuilds incremental tables from scratch)
dbt run --full-refresh --target dev
```

### Run tests

```bash
cd include/transforms

# Run all tests
dbt test --target dev

# Run tests for a specific model
dbt test --select mart_pokemons
```

### Generate docs

```bash
cd include/transforms
dbt docs generate --target dev
dbt docs serve
```

This opens the dbt docs site with interactive lineage graph, column descriptions, and test coverage. Schema descriptions are maintained in `models/*/schema.yml`.

### Profiles

dbt profiles are in `include/transforms/profiles.yml`:

| Target | Schema prefix | Threads | Use case |
|--------|--------------|---------|----------|
| `dev` | `dbt_dev` | 1 | Local development |
| `prod` | `analytics` | 4 | Production |

---

## Streamlit

### Setup secrets

Create `streamlit/.streamlit/secrets.toml`:

```toml
[motherduck]
token = "your_motherduck_token"
database = "poke_db"
```

> This file is gitignored. Never commit secrets.

### Run the app

```bash
cd streamlit
streamlit run app.py
```

App runs at **http://localhost:8501**.

---

## Project layout

```
dags/                          # Airflow DAG definitions
  ingest__*.py                 #   API → raw (PokeAPI ingestion)
  transform__*.py              #   raw → marts (dbt via Cosmos)
  setup__motherduck.py         #   one-time table creation
  check__source_freshness.py   #   dbt source freshness
include/
  transforms/                  # dbt project
    models/
      staging/                 #   views over raw JSON
      intermediate/            #   reshaped/enriched views
      marts/                   #   incremental tables
    tests/                     #   custom data quality tests
    macros/                    #   shared SQL macros
  callbacks/                   # Airflow failure callbacks
nl_to_sql/                     # NL-to-SQL prompt engine (WIP)
streamlit/                     # Streamlit app
docs/                          # You are here
Dockerfile                     # Astronomer Runtime
requirements.txt               # Python dependencies
```
