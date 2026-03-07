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
BATCH_SIZE = 50

# --------------------------------------------------------------------------------
# Asset
# --------------------------------------------------------------------------------

pokemon_catalogue_stg_asset = Asset("motherduck://staging/stg_pokemon_catalogue")
pokemon_data_raw_asset = Asset("motherduck://raw/pokemon_data")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="poke_api_to_raw",
    start_date=datetime(2026, 2, 15),
    schedule=pokemon_catalogue_stg_asset,
    catchup=False,
    tags=["pokemon_data", "elt", "ingest"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def pokemon_etl():

    @task.sensor(poke_interval=60, timeout=300)
    def api_check():
        endpoint = 'https://pokeapi.co/api/v2/'

        try:
            resp = requests.get(endpoint, timeout=10)
            if resp.status_code == 200:
                return PokeReturnValue(is_done=True)
            else:
                return PokeReturnValue(is_done=False)
        except requests.RequestException as e:
            return PokeReturnValue(is_done=False)

    @task.short_circuit
    def catalogue_check():
        """Checks that the catalogue table exists and returns data"""
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            count = con.execute("""
                SELECT COUNT(*) FROM staging.stg_pokemon_catalogue
            """).fetchone()[0]
            return count > 0
        except Exception:
            return False
        finally:
            con.close()

    @task
    def identify_pokemons_not_ingested():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            to_ingest_df = con.execute("""
                SELECT pc.poke_id FROM staging.stg_pokemon_catalogue pc
                ANTI JOIN raw.pokemon_data pd
                    ON pc.poke_id = pd.id
            """).df()

            pokemons_to_ingest = [int(pid) for pid in to_ingest_df["poke_id"].to_list()]
            LOG.info(f"{len(pokemons_to_ingest)} pokemon(s) to ingest.")
            return pokemons_to_ingest
        finally:
            con.close()

    @task.short_circuit
    def check_if_ingestion_required(pokemons_not_ingested):
        return bool(pokemons_not_ingested)

    @task
    def select_pokemons_to_import(pokemon_ids_to_ingest):
        return pokemon_ids_to_ingest[:BATCH_SIZE]

    @task(outlets=[pokemon_data_raw_asset])
    def fetch_and_import_pokemon_data(pokemons_ids_to_ingest):
        import json
        import pyarrow as pa

        context = get_current_context()
        fetch_date = context["data_interval_end"].isoformat()
        run_id = context["dag_run"].run_id

        pokemon_list = []
        failed_ids = []

        for pokemon_id in pokemons_ids_to_ingest:
            endpoint = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}/'

            try:
                resp = requests.get(endpoint, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                pokemon_list.append({
                    "id": int(pokemon_id),
                    "fetch_date": fetch_date,
                    "batch_id": run_id,
                    "raw": json.dumps(data)
                })

            except requests.RequestException as e:
                LOG.warning(f"Failed to fetch pokemon {pokemon_id}: {e}")
                failed_ids.append(pokemon_id)

        if pokemon_list:
            schema = pa.schema([
                pa.field("id", pa.int32()),
                pa.field("fetch_date", pa.string()),
                pa.field("batch_id", pa.string()),
                pa.field("raw", pa.large_string()),
            ])

            arrow_table = pa.Table.from_pylist(pokemon_list, schema=schema)

            con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
            try:
                con.register("arrow_table", arrow_table)
                con.execute("""
                    INSERT INTO raw.pokemon_data
                    SELECT id, fetch_date, batch_id, raw::JSON
                    FROM arrow_table
                    WHERE id NOT IN (SELECT id FROM raw.pokemon_data)
                """)
            finally:
                con.close()

        if failed_ids:
            raise RuntimeError(f"Failed to fetch {len(failed_ids)} pokemon(s): {failed_ids}")

    # API test call, then checks for catalogue
    check_api = api_check()
    check_cat = catalogue_check()
    check_api >> check_cat

    # Identify pokemons not yet ingested
    not_ingested = identify_pokemons_not_ingested()
    check_cat >> not_ingested

    # Short-circuit if nothing to ingest, otherwise batch and fetch
    gate = check_if_ingestion_required(not_ingested)
    batch = select_pokemons_to_import(not_ingested)
    gate >> batch

    fetch_and_import_pokemon_data(batch)

pokemon_etl()
