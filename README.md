# pokemon_airflow

An Airflow pipeline that ingests Pokémon data from [PokéAPI](https://pokeapi.co) into [MotherDuck](https://motherduck.com) (cloud DuckDB).

## What it does

The `pokemon_etl` DAG:
1. Fetches all Gen 1 Pokémon IDs from PokéAPI
2. Identifies which ones haven't been ingested yet
3. Picks 10 at random and fetches their details (name, height, weight, types, stats)
4. Loads and transforms the data through `raw` → `staging` → `analytics` schemas in MotherDuck

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed
- A [MotherDuck](https://motherduck.com) account with a service token and a database named `poke_db`

## Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd pokemon_airflow

# 2. Set your MotherDuck token
export motherduck_token=your_token_here

# 3. Start Airflow
astro dev start
```

Airflow UI will be available at **http://localhost:8080** (admin / admin).

Trigger the `pokemon_etl` DAG manually to run a pipeline.

## Project structure

```
dags/           # Airflow DAGs
include/
  config/       # Pipeline configuration
  sql/          # SQL queries (DuckDB)
requirements.txt
Dockerfile
```
