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
from airflow.models.param import Param
from datetime import datetime
import requests
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

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
    dag_id="poke_api_to_raw",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pokemon", "etl"],
    params={
        "generation": Param(
            default=1,
            type="integer",
            minimum=1,
            maximum=9,
            description="Pokemon generation picked for ingestion"
        )
    }
)

def pokemon_etl():

    @task
    def fetch_pokemon_ids_in_gen():
        """
        Extract all pokemons of a generation
        Args:
            - generation: int
        Returns:
            - pokemons: list
        """
        context = get_current_context()
        generation = context["params"]["generation"]

        endpoint = f'https://pokeapi.co/api/v2/generation/{generation}/'
        try:
            resp = requests.get(endpoint)
            resp.raise_for_status()
            data = resp.json()
            pokemon_species = data.get("pokemon_species")
        except:
            LOG.info(f"Could not fetch Gen {generation} pokemons!")

        pokemons = [int(pokemon.get('url').rstrip('/').split('/')[-1]) for pokemon in pokemon_species]

        return pokemons    

    @task
    def identify_pokemons_not_ingested(pokemon_ids):
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()

        # check that table exists
        table_exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'raw' AND table_name = 'pokemon_data'
        """).fetchone()[0]

        if not table_exists:
            LOG.info("Table raw.pokemon_data doesn't exist.")
            return pokemon_ids

        df = con.execute("SELECT id FROM raw.pokemon_data").df()

        if df.empty:
            return pokemon_ids

        pokemons_ingested = [int(pid) for pid in df["id"].to_list()]
        return [pid for pid in pokemon_ids if pid not in pokemons_ingested]

    @task
    def select_random_pokemons(pokemon_ids_not_ingested):
        import random

        # error handling when there are less than 10 pokemons to ingest
        left_to_ingest = len(pokemon_ids_not_ingested)
        if left_to_ingest >= 10:
            pokemons_ids_to_ingest = random.sample(pokemon_ids_not_ingested, k=10)
        else: 
            pokemons_ids_to_ingest = random.sample(pokemon_ids_not_ingested, k=left_to_ingest)
        
        return pokemons_ids_to_ingest

    @task
    def fetch_and_import_pokemon_data(pokemons_ids_to_ingest):
        import json
        import pyarrow as pa

        context = get_current_context()
        generation = context["params"]["generation"]
        fetch_date = context["data_interval_end"].isoformat()
        run_id = context["dag_run"].run_id

        pokemon_list = []

        for pokemon_id in pokemons_ids_to_ingest:
            endpoint = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}/'

            try:
                resp = requests.get(endpoint)
                resp.raise_for_status()
                data = resp.json()

                pokemon_list.append({
                    "id": int(pokemon_id),
                    "fetch_date": fetch_date,
                    "batch_id": run_id,
                    "generation": generation,
                    "raw": json.dumps(data)
                })

            except requests.RequestException as e:
                LOG.warning(f"Failed to fetch pokemon {pokemon_id}: {e}")

        if pokemon_list:
            schema = pa.schema([
                pa.field("id", pa.int32()),
                pa.field("fetch_date", pa.string()),
                pa.field("batch_id", pa.string()),
                pa.field("generation", pa.int16()),
                pa.field("raw", pa.large_string()),
            ])

            arrow_table = pa.Table.from_pylist(pokemon_list, schema=schema)

            con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
            
            con.execute("""CREATE SCHEMA IF NOT EXISTS raw""")

            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.pokemon_data (
                    id INTEGER,
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    generation SMALLINT,
                    raw JSON
                )
            """)
            con.execute("""
                INSERT INTO raw.pokemon_data 
                SELECT 
                    id, 
                    fetch_date, 
                    batch_id, 
                    generation, 
                    raw::JSON 
                FROM arrow_table
            """)

        return None

    pokemon_ids = fetch_pokemon_ids_in_gen()
    pokemons_not_ingested = identify_pokemons_not_ingested(pokemon_ids)
    pokemons_to_ingest = select_random_pokemons(pokemons_not_ingested)
    fetch_and_import_pokemon_data(pokemons_to_ingest)

pokemon_etl()