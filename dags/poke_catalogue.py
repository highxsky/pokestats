# --------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------

import logging
import requests
from datetime import datetime

from airflow.sdk import dag, task, get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.callbacks import notify_on_failure

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

LOG = logging.getLogger(__name__)
GENERATIONS = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# --------------------------------------------------------------------------------
# Asset
# --------------------------------------------------------------------------------
# 
# pokemon_catalogue_raw_asset = Asset("motherduck://raw/pokemon_data")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="poke_catalogue",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["pokemon", "etl"],
    on_failure_callback=notify_on_failure
)

def pokemon_catalogue_etl():

    @task.sensor(poke_interval=60, timeout=300)
    def api_check():
        """Pokes the API and returns a 200 response"""
        endpoint = 'https://pokeapi.co/api/v2/'

        try:
            resp = requests.get(endpoint)
            if resp.status_code == 200:
                return PokeReturnValue(is_done=True)
            else:
                return PokeReturnValue(is_done=False)
        except requests.RequestException as e:
            return PokeReturnValue(is_done=False)

    @task
    def fetch_and_import_gen_pokemons(generation):
        """
        Extract all pokemons of a generation
        Args:
            - generation: int
        Returns:
            - pokemons: list
        """
        import pyarrow as pa
        import json

        context = get_current_context()
        fetch_date = context["data_interval_end"].isoformat()
        run_id = context["dag_run"].run_id

        endpoint = f'https://pokeapi.co/api/v2/generation/{generation}/'

        try:
            resp = requests.get(endpoint)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            LOG.info(f"Could not fetch Gen {generation} pokemons!")
            raise
        
        # only fetching whats relevant for this dag
        data_subset = {
            "fetch_date": fetch_date,
            "batch_id": run_id,
            "raw": json.dumps({
                "id": data["id"],
                "pokemon_species": data["pokemon_species"]
            })
        }

        schema = pa.schema([
            pa.field("fetch_date", pa.string()),
            pa.field("batch_id", pa.string()),
            pa.field("raw", pa.large_string()),
        ])

        arrow_table = pa.Table.from_pydict(
            {k: [v] for k, v in data_subset.items()},
            schema=schema
        )

        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        con.register("arrow_table", arrow_table)
        
        # Schema
        con.execute("""CREATE SCHEMA IF NOT EXISTS raw""")

        # Table
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.pokemon_catalogue (
                fetch_date TIMESTAMP,
                batch_id VARCHAR,
                raw JSON
            )
        """)

        # Records
        con.execute("""
            INSERT INTO raw.pokemon_catalogue 
            SELECT 
                fetch_date, 
                batch_id,
                raw::JSON 
            FROM arrow_table
        """)

        return None

    check = api_check()
    fetch_tasks = fetch_and_import_gen_pokemons.expand(generation=GENERATIONS)

    check >> fetch_tasks

pokemon_catalogue_etl()
