#%%

# ==================================================
# About
# ==================================================

# Gotta Catch Em' All!

# DAG to ingest all GEN 1 pokemons in a SQLite DB
# Once ingested, another DAG will fetch 3 random pokemons in this list, one at a time
# DAG will run once a day until all pokemons are captured!

# ==================================================
# TO DO
# ==================================================

# Get pokemons from API
# Extract pokemon IDs as list
# List pokemons not ingested (see if set / list is ok)
# Pick 3 random pokemons not ingested
# Ingest the 3 random pokemons in pokemon details
# Update the pokemon tables (pokemons AND pokemon_details then)
# Check integrity of records (match between pokemon and pokemon_details)
# If orphaned records, reupdate the table?

# Step 1 - Load all gen 1 pokemons into clean DF
# Step 2 - Check if ingestion is required
    # Does not exist in pokemons table
    # Does not exist in pokemon details table

# Rewrite
    # Check list of ingested pokemons
    # Select 3 random pokemons among all gen 1 AND NOT in list of ingested pokemons
    # Uploads their details in a specific table (e.g. pokemon_details, with weight and height and other basic attributes)
    # Then updates the pokemons table indicating "Ingested" as status for these 3 pokemons
# Wrap to DAG
# Push to GIT

# ==================================================
# Approach
# ==================================================



# ==================================================
# Imports
# ==================================================

# packages
import yaml
from dotenv import load_dotenv
from pathlib import Path
from airflow.sdk import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import date, datetime, timedelta
import logging
import requests
import json
from pathlib import Path
import os
import pandas as pd
import sqlite3

# ==================================================
# Load .env and config
# ==================================================

load_dotenv()
def load_config():
    config_path = Path(__file__).parent.parent / "include" / "poke_api" / "config" / "pipelines.yaml"
    with open(config_path) as file:
        return yaml.safe_load(file)

config = load_config()

# ==================================================
# Constants
# ==================================================

POKE_DB = config["common"]["database"]["path"]
GENERATION = config["common"]["poke_gen"]["generation"]

# ==================================================
# Functions
# ==================================================

def get_all_pokemons(generation: int) -> pd.DataFrame:
    """
    Fetch all Pokémons from the PokeAPI for a specific generation.
    Then parses it into a clean dataframe.
    Args:
        - generation: integer
    Returns:
        - dataframe of pokemons (id, pokemon) to catch
    """

    endpoint = f'https://pokeapi.co/api/v2/generation/{generation}/'
    pokemon_dict = {}

    try:
        resp = requests.get(endpoint)
        resp.raise_for_status()
        data = resp.json()
        pokemon_species = data.get("pokemon_species")
    except:
        print(f"Could not fetch Gen {generation} pokemons!")

    for pokemon in pokemon_species:
        key = int(pokemon.get("url").rstrip('/').split('/')[-1])
        # remove last /, splits by / delimiter, gets last group --> poke number
        value = pokemon.get("name")
        pokemon_dict[key] = value

    pokemon_details = pd.DataFrame(
        data=list(pokemon_dict.items()), 
        columns=['id', 'name']
    )

    pokemon_details['name'] = pokemon_details['name'].str.capitalize()

    return pokemon_details

def extract_pokemon_ids(all_gen_pokemons: pd.DataFrame) -> list[int]:
    return all_gen_pokemons["id"].unique().tolist()

def check_if_ingested(pokemon_ids: list[int]) -> list[int]:
    """
    Check if pokemons in a generation are uploaded already or not.
    Args:
        - dataframe: all pokemons of a generation
    Returns:
        - list
    """

    with sqlite3.connect(POKE_DB) as conn: 

        # parameterized query to get list of pokemons already ingested
        ph = ','.join('?' * len(pokemon_ids))
        query = f"""
            SELECT
                id
            FROM pokemon_details
            WHERE id IN ({ph})
        """
        rows = conn.execute(query, pokemon_ids)
        pokemons_ingested = set([row[0] for row in rows])
        pokemons_not_ingested = [pid for pid in pokemon_ids if pid not in pokemons_ingested]

    return pokemons_ingested, pokemons_not_ingested

all_gen_pokemons = get_all_pokemons(GENERATION)
pokemon_ids = extract_pokemon_ids(all_gen_pokemons)
print(POKE_DB)
pokemons_ingested, pokemons_not_ingested = check_if_ingested(pokemon_ids)


#%%

# ==================================================
# DAG
# ==================================================

with DAG(
    dag_id="ingest_random_pokemons",
    start_date=datetime(2026, 1, 26),
    schedule='@daily',
    catchup=False,
    tags=["pokemon", "poke_api", "poke_db"]
) as dag:
    # 1st task - Extract
    get_gen_pokemons = PythonOperator(
        task_id="extract",
        python_callable=get_gen_pokemons,
        op_kwargs={
            "target_path_str": "/opt/airflow/data/pokemon_dict"
        },
        retries=0,
        retry_delay=timedelta(minutes=1)
    )

# ==================================================
# Workflow
# ==================================================

    get_gen_pokemons

dag