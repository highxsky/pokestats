# --------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------

import logging
import requests
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context, Asset
from airflow.sdk.bases.sensor import PokeReturnValue

from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.callbacks import notify_on_failure

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

LOG = logging.getLogger(__name__)


# --------------------------------------------------------------------------------
# Asset
# --------------------------------------------------------------------------------

pokemon_catalogue_raw_asset = Asset("motherduck://raw/pokemon_catalogue")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="ingest__pokemon_catalogue",
    start_date=datetime(2026, 2, 15),
    schedule="@weekly",
    catchup=False,
    tags=["layer:ingest", "entity:pokemon_catalogue", "tool:pokeapi"],
    doc_md="""
## Step 1 — ingest__pokemon_catalogue

Fetches the list of all pokemon species per generation from PokeAPI
and loads them into `raw.pokemon_catalogue`.

**Trigger:** @weekly
**Triggers next:** `transform__pokemon_catalogue` (via asset `raw/pokemon_catalogue`)
""",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def pokemon_catalogue_etl():

    @task.sensor(poke_interval=60, timeout=300)
    def api_check():
        """Pokes the API and returns a 200 response"""
        endpoint = 'https://pokeapi.co/api/v2/'

        try:
            resp = requests.get(endpoint, timeout=10)
            if resp.status_code == 200:
                return PokeReturnValue(is_done=True)
            else:
                return PokeReturnValue(is_done=False)
        except requests.RequestException as e:
            return PokeReturnValue(is_done=False)

    @task
    def cleanup_previous_batch():
        """Delete any rows from a previous attempt of this DAG run"""
        context = get_current_context()
        run_id = context["dag_run"].run_id
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute(
                "DELETE FROM raw.pokemon_catalogue WHERE batch_id = ?",
                [run_id],
            )
        finally:
            con.close()

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
            resp = requests.get(endpoint, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            LOG.info(f"Could not fetch Gen {generation} pokemons!")
            raise
        
        # only fetching whats relevant for this dag
        data_subset = {
            "fetch_date": fetch_date,
            "batch_id": run_id,
            "payload": json.dumps({
                "id": data["id"],
                "pokemon_species": data["pokemon_species"]
            })
        }

        schema = pa.schema([
            pa.field("fetch_date", pa.string()),
            pa.field("batch_id", pa.string()),
            pa.field("payload", pa.large_string()),
        ])

        arrow_table = pa.Table.from_pydict(
            {k: [v] for k, v in data_subset.items()},
            schema=schema
        )

        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.register("arrow_table", arrow_table)
            con.execute("""
                INSERT INTO raw.pokemon_catalogue
                SELECT fetch_date, batch_id, payload::JSON
                FROM arrow_table
            """)
        finally:
            con.close()

    @task
    def get_generation_ids():
        """Fetch the generation list endpoint and return a list of IDs"""
        resp = requests.get('https://pokeapi.co/api/v2/generation/', timeout=15)
        resp.raise_for_status()
        count = resp.json()["count"]
        return list(range(1, count + 1))

    @task(outlets=[pokemon_catalogue_raw_asset])
    def mark_catalogue_complete():
        LOG.info("All generations ingested successfully.")

    check = api_check()
    cleanup = cleanup_previous_batch()
    gen_ids = get_generation_ids()
    fetch_tasks = fetch_and_import_gen_pokemons.expand(generation=gen_ids)

    check >> cleanup >> gen_ids >> fetch_tasks >> mark_catalogue_complete()

pokemon_catalogue_etl()