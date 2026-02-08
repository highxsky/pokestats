# TO DO
    # clean up create tables query
    # create a truncate table query
    # pass DAG ID to tables steps to avoid duplicates
    # Fetch evolution chains for pokemons
        # Step 1 --> Pass ID to https://pokeapi.co/api/v2/pokemon-species/{id}/
        # Step 2 --> Fetch evolution_chain https://pokeapi.co/api/v2/evolution-chain/32/
            # chain --> species --> url
        # https://pokeapi.co/api/v2/evolution-chain/32
    # Fetch generation
        # Pass ID to https://pokeapi.co/api/v2/pokemon-species/{id}/

# --------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------

import logging
from pathlib import Path
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import duckdb
from airflow_provider_duckdb.hooks.duckdb import DuckDBHook

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

ROOT_PATH = Path(__file__).parent.parent
QUERIES_PATH = ROOT_PATH / "include" / "sql"
LOG = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

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
            LOG.info(f"Could not fetch Gen {generation} pokemons!")

        pokemons = [pokemon.get('url').rstrip('/').split('/')[-1] for pokemon in pokemon_species]

        return pokemons    

    @task
    def identify_pokemons_not_ingested(pokemon_ids):
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            query = """SELECT id FROM raw.pokemons"""
            df = con.execute(query).df()

            # No pokemons imported in raw
            if df.empty:
                return pokemon_ids
            
            pokemons_ingested = df["id"].to_list()
            pokemon_ids_not_ingested = [pid for pid in pokemon_ids if pid not in pokemons_ingested]
            return pokemon_ids_not_ingested
        except duckdb.CatalogException:
            # Table not in catalog
            LOG.info("Table raw.pokemons doesn't exist.")
            return pokemon_ids

    @task
    def select_three_random_pokemons(pokemon_ids_not_ingested):
        import random

        pokemons_ids_to_ingest = random.sample(pokemon_ids_not_ingested, k=10)

        return pokemons_ids_to_ingest

    @task
    def create_raw_pokemons(pokemons_ids_to_ingest):

        context = get_current_context()
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        query = (QUERIES_PATH / "duckdb_create_raw_pokemons.sql").read_text()

        pokemon_list = []
        attributes = ["name", "height", "weight", "types", "stats"]

        for pokemon_id in pokemons_ids_to_ingest:
            endpoint = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}/'

            try:
                resp = requests.get(endpoint)
                resp.raise_for_status()
                data = resp.json()
                pokemon_data = {attr: data.get(attr) for attr in attributes}
                pokemon_data["id"] = pokemon_id
                pokemon_list.append(pokemon_data)
            except requests.RequestException as e:
                LOG.info(f"An error occurred fetching the PokeAPI: {e}")

        df = pd.DataFrame(pokemon_list)
        df['fetch_date'] = datetime.now()
        df["batch_id"] = context["dag_run"].run_id

        con.execute(query)
        
        return None  

    @task
    def clean_raw_to_staging():

        context = get_current_context()
        run_id = context["dag_run"].run_id
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        query = (QUERIES_PATH / "duckdb_clean_to_staging.sql").read_text()
        con.execute(query, [run_id])

    @task
    def create_pokemons():

        context = get_current_context()
        run_id = context["dag_run"].run_id
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        query = (QUERIES_PATH / "duckdb_create_update_pokemons.sql").read_text()
        con.execute(query, [run_id])

    @task
    def create_pokemon_stats():

        context = get_current_context()
        run_id = context["dag_run"].run_id
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        query = (QUERIES_PATH / "duckdb_create_update_stats.sql").read_text()
        con.execute(query, [run_id])

    @task
    def create_pokemon_types():
        
        context = get_current_context()
        run_id = context["dag_run"].run_id
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        query = (QUERIES_PATH / "duckdb_create_update_types.sql").read_text()
        con.execute(query, [run_id])

    pokemon_ids = extract_pokemon_ids(1)
    pokemons_not_ingested = identify_pokemons_not_ingested(pokemon_ids)
    pokemons_to_ingest = select_three_random_pokemons(pokemons_not_ingested)

# --------------------------------------------------------------------------------
# Flow
# --------------------------------------------------------------------------------

    (
        create_raw_pokemons(pokemons_to_ingest)
        >> clean_raw_to_staging() 
        >> create_pokemons() 
        >> create_pokemon_stats() 
        >> create_pokemon_types()
    )

pokemon_etl()