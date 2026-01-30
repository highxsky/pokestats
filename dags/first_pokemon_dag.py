# packages
import yaml

from dotenv import load_dotenv
from pathlib import Path
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from datetime import date, datetime, timedelta
import requests

import pandas as pd
import sqlite3

import json
import os

# ==================================================
# Load .env and config
# ==================================================

# Load config from root folder --> include --> poke_api --> config --> pipelines.yaml

# load_dotenv()
# def load_config():
#     config_path = Path(__file__).parent.parent / "include" / "poke_api" / "config" / "pipelines.yaml"
#     with open(config_path) as file:
#         return yaml.safe_load(file)

# config = load_config()

# # ==================================================
# # Constants
# # ==================================================

# POKE_DB = config["common"]["database"]["path"]
# GENERATION = config["common"]["poke_gen"]["generation"]


@dag(
    dag_id='pokemon_etl',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['pokemon', 'etl']
)

def pokemon_etl():

    @task
    def extract_pokemon_ids(generation):

        """
        Extract all pokemons of a generation
        Args:
            - generation: int
        Returns:
            - pokemons: list
        """

        endpoint = f'https://pokeapi.co/api/v2/generation/{generation}/'
        try:
            resp = requests.get(endpoint)
            resp.raise_for_status()
            data = resp.json()
            pokemon_species = data.get("pokemon_species")
        except:
            print(f"Could not fetch Gen {generation} pokemons!")

        pokemons = [pokemon.get('url').rstrip('/').split('/')[-1] for pokemon in pokemon_species]

        return pokemons    

    @task
    def identify_new_pokemon(pokemons):

        poke_db = '/usr/local/airflow/include/poke_api/db/poke_db.db'

        with sqlite3.connect(poke_db) as conn: 
            # parameterized query to get list of pokemons already ingested
            placeholders = ','.join('?' * len(pokemons))
            query = f"""
                SELECT
                    id
                FROM pokemon_details
                WHERE id IN ({placeholders})
            """
            rows = conn.execute(query, pokemons)
            pokemons_ingested = set([row[0] for row in rows])
            pokemons_to_ingest = [pid for pid in pokemons if pid not in pokemons_ingested]

        return pokemons_to_ingest

    pokemons = extract_pokemon_ids(1)
    identify_new_pokemon(pokemons)

pokemon_etl()

# @task
# def extract_pokemon_ids(generation):

#     endpoint = f'https://pokeapi.co/api/v2/generation/{generation}/'

#     try:
#         resp = requests.get(endpoint)
#         resp.raise_for_status()
#         data = resp.json()
#         pokemon_species = data.get("pokemon_species")
#     except:
#         print(f"Could not fetch Gen {generation} pokemons!")

#     pokemons = [pokemon.get('url').rstrip('/').split('/')[-1] for pokemon in pokemon_species]

#     return pokemons

# def extract_pokemon_ids(all_gen_pokemons: pd.DataFrame) -> list[int]:
#     return all_gen_pokemons["id"].unique().tolist()

# def check_if_ingested(pokemon_ids: list[int]) -> list[int]:
#     """
#     Check if pokemons in a generation are uploaded already or not.
#     Args:
#         - dataframe: all pokemons of a generation
#     Returns:
#         - list
#     """

#     with sqlite3.connect(POKE_DB) as conn: 

#         # parameterized query to get list of pokemons already ingested
#         ph = ','.join('?' * len(pokemon_ids))
#         query = f"""
#             SELECT
#                 id
#             FROM pokemon_details
#             WHERE id IN ({ph})
#         """
#         rows = conn.execute(query, pokemon_ids)
#         pokemons_ingested = set([row[0] for row in rows])
#         pokemons_not_ingested = [pid for pid in pokemon_ids if pid not in pokemons_ingested]

#     return pokemons_ingested, pokemons_not_ingested

# all_gen_pokemons = get_all_pokemons(GENERATION)
# pokemon_ids = extract_pokemon_ids(all_gen_pokemons)
# print(POKE_DB)
# pokemons_ingested, pokemons_not_ingested = check_if_ingested(pokemon_ids)


# #%%

# # ==================================================
# # DAG
# # ==================================================

# with DAG(
#     dag_id="ingest_random_pokemons",
#     start_date=datetime(2026, 1, 26),
#     schedule='@daily',
#     catchup=False,
#     tags=["pokemon", "poke_api", "poke_db"]
# ) as dag:
#     # 1st task - Extract
#     get_gen_pokemons = PythonOperator(
#         task_id="extract",
#         python_callable=get_gen_pokemons,
#         op_kwargs={
#             "target_path_str": "/opt/airflow/data/pokemon_dict"
#         },
#         retries=0,
#         retry_delay=timedelta(minutes=1)
#     )

# # ==================================================
# # Workflow
# # ==================================================

#     get_gen_pokemons

# dag