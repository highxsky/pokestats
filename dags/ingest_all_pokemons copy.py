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
# Setup
# ==================================================

# Logging setup

log = logging.getLogger(__name__)

# ==================================================
# Constants
# ==================================================

# ROOT_DIR = Path("opt/airflow/data/pokemon_dict")
GENERATION = 1
POKE_DB = "/home/highxsky/highxsky/sqlite_work/poke_db/poke_db.db"

# ==================================================
# Functions
# ==================================================

def get_all_gen_pokemons(generation: int) -> list:
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

def check_uploaded_pokemons(pokemons: pd.DataFrame):
    """
    Check if pokemons in a generation are uploaded already or not.
    Args:
        - dataframe: all pokemons of a generation
    Returns:
        - list
    """

    ingested_pokemons = []

    ingested_pokemons_query = """
    SELECT 
        name
    FROM pokemons
    """

    with sqlite3.connect(poke_db) as conn:
        rows = conn.execute(ingested_pokemons_query)
    
    for row in rows:
        ingested_pokemons.append(row[0])

    return ingested_pokemons

def upload_pokemons_to_ingest(gen_pokemons: pd.DataFrame):
    """
    Upload all pokemons to ingest, unless they are already ingested.
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

    return pokemon_details

def get_already_ingested_pokemons(poke_db) -> pd.DataFrame:
    """
    Get pokemons already ingested in the DB.
    Args:
        - pokemon DB
    Returns:
        - list of ingested pokemons
    """

    ingested_pokemons = []

    ingested_pokemons_query = """
    SELECT 
        lower(name) as name
    FROM pokemons 
    WHERE status = 'Ingested'
    """

    with sqlite3.connect(poke_db) as conn:
        rows = conn.execute(ingested_pokemons_query)
    
    for row in rows:
        ingested_pokemons.append(row[0])

    return ingested_pokemons

def get_pokemons_to_ingest(pokemon_details, ingested_pokemons) -> pd.DataFrame:
    """
    Excludes ingested pokemons from the full pokemon dataframe
    Args:
        - pokemons (dataframe)
        - ingested pokemons (list)
    Returns:
        - pokemons to ingest (df)
    """
    pokemons_to_ingest = pokemon_details[~pokemon_details["name"].isin(ingested_pokemons)]["name"].to_list()

    return pokemons_to_ingest

def select_random_pokemons(pokemons_to_ingest) -> list:
    """
    Get pokemons already ingested in the DB.
    Args:
        - list of pokemons to ingest
    Returns:
        - list of 3 randomly selected pokemons
    """
    import random

    selected_pokemons_to_ingest = random.sample(pokemons_to_ingest, 3)

    return selected_pokemons_to_ingest

def ingest_selected_pokemons(poke_db, pokemons_to_ingest, pokemon_details) -> pd.DataFrame:
    """
    Filters the pokemon details dataframe on the 3 pokemons to ingest, then builds the tuples, then ingests.
    Args:
        - list of pokemons to ingest
        - dataframe with pokemon details
    Returns:
        - None
    """

    filtered_pokemon_details = pokemon_details[pokemon_details["name"].isin(pokemons_to_ingest)]
    pokemon_tuples = list(filtered_pokemon_details.itertuples(index=False, name=None))

    ingest_pokemons_query = """
    INSERT INTO pokemons (id, name, status)
    VALUES (?, ?, 'Ingested')
    """

    with sqlite3.connect(poke_db) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            ingest_pokemons_query,
            pokemon_tuples
        )

    return None

all_gen_pokemons = get_all_gen_pokemons(GENERATION)

#%%

already_ingested_pokemons = get_already_ingested_pokemons(POKE_DB)
pokemons_not_ingested_yet = get_pokemons_to_ingest(all_pokemon_details, already_ingested_pokemons)
random_pokemons_to_ingest = select_random_pokemons(pokemons_not_ingested_yet)
ingest_selected_pokemons(POKE_DB, random_pokemons_to_ingest, all_pokemon_details)

#%%

# ==================================================
# DAG
# ==================================================

with DAG(
    dag_id="get_pokemon_data",
    start_date=datetime(2025, 11, 14),
    schedule='@daily',
    catchup=False,
    tags=["pokemon", "poke_api", "poke_db"]
) as dag:
    # 1st task - Extract
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=get_pokemon_details,
        op_kwargs={
            "target_path_str": "/opt/airflow/data/pokemon_dict"
        },
        retries=0,
        retry_delay=timedelta(minutes=1)
    )
    # 2nd task - Transform
    dl_sprites = PythonOperator(
        task_id="dl_sprites",
        python_callable=download_sprites,
        op_kwargs={ 
            "input_file": "/opt/airflow/data/pokemon_dict/{{ds}}_pokemon_dict.json",
            "sprite_folder": "/opt/airflow/data/sprites"
        },
        retries=0,
        retry_delay=timedelta(minutes=1)
    )

# ==================================================
# Workflow
# ==================================================

    extract_task >> dl_sprites

dag