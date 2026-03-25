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
BATCH_SIZE = 100

# --------------------------------------------------------------------------------
# Asset
# --------------------------------------------------------------------------------

pokemon_catalogue_stg_asset = Asset("motherduck://staging/stg_pokemon_catalogue")
pokemon_species_raw_asset = Asset("motherduck://raw/pokemon_species")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="ingest__pokemon_species",
    start_date=datetime(2026, 2, 15),
    schedule=pokemon_catalogue_stg_asset,
    catchup=False,
    tags=["layer:ingest", "entity:pokemon_species", "tool:pokeapi"],
    doc_md="""
## ingest__pokemon_species

Reads `staging.stg_pokemon_catalogue` to identify pokemon species not yet ingested,
then fetches their species data from PokeAPI in batches of 50 into `raw.pokemon_species`.
Re-runs automatically until all species are ingested.

**Trigger:** asset `staging/stg_pokemon_catalogue` (set by `transform__pokemon_catalogue`)
**Triggers next:** `transform__pokemon_species` (via asset `raw/pokemon_species`)
""",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def pokemon_species_etl():

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
    def identify_species_not_ingested():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            to_ingest_df = con.execute("""
                SELECT pc.poke_id FROM staging.stg_pokemon_catalogue pc
                ANTI JOIN raw.pokemon_species ps
                    ON pc.poke_id = ps.id
            """).df()

            species_to_ingest = [int(pid) for pid in to_ingest_df["poke_id"].to_list()]
            LOG.info(f"{len(species_to_ingest)} species to ingest.")
            return species_to_ingest
        finally:
            con.close()

    @task.short_circuit
    def check_if_ingestion_required(species_not_ingested):
        return bool(species_not_ingested)

    @task
    def select_species_to_import(species_ids_to_ingest):
        return species_ids_to_ingest[:BATCH_SIZE]

    @task
    def fetch_and_import_species(species_ids_to_ingest):
        import json
        import pyarrow as pa

        context = get_current_context()
        fetch_date = context["data_interval_end"].isoformat()
        run_id = context["dag_run"].run_id

        species_list = []
        failed_ids = []

        for species_id in species_ids_to_ingest:
            endpoint = f'https://pokeapi.co/api/v2/pokemon-species/{species_id}/'

            try:
                resp = requests.get(endpoint, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                species_list.append({
                    "id": int(species_id),
                    "fetch_date": fetch_date,
                    "batch_id": run_id,
                    "payload": json.dumps(data)
                })

            except requests.RequestException as e:
                LOG.warning(f"Failed to fetch species {species_id}: {e}")
                failed_ids.append(species_id)

        if species_list:
            schema = pa.schema([
                pa.field("id", pa.int32()),
                pa.field("fetch_date", pa.string()),
                pa.field("batch_id", pa.string()),
                pa.field("payload", pa.large_string()),
            ])

            arrow_table = pa.Table.from_pylist(species_list, schema=schema)

            con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
            try:
                con.register("arrow_table", arrow_table)
                con.execute("""
                    INSERT INTO raw.pokemon_species
                    SELECT id, fetch_date, batch_id, payload::JSON
                    FROM arrow_table
                    WHERE id NOT IN (SELECT id FROM raw.pokemon_species)
                """)
            finally:
                con.close()

        if failed_ids:
            raise RuntimeError(f"Failed to fetch {len(failed_ids)} species: {failed_ids}")

    @task(outlets=[pokemon_species_raw_asset], trigger_rule="all_done")
    def mark_species_complete():
        LOG.info("Pokemon species ingestion DAG completed.")

    # API test call, then checks for catalogue
    check_api = api_check()
    check_cat = catalogue_check()
    check_api >> check_cat

    # Identify species not yet ingested
    not_ingested = identify_species_not_ingested()
    check_cat >> not_ingested

    # Short-circuit if nothing to ingest, otherwise batch and fetch
    gate = check_if_ingestion_required(not_ingested)
    batch = select_species_to_import(not_ingested)
    gate >> batch

    fetch_and_import_species(batch) >> mark_species_complete()

pokemon_species_etl()
