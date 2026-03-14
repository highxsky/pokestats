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

generations_raw_asset = Asset("motherduck://raw/generations")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="ingest__generations",
    start_date=datetime(2026, 2, 15),
    schedule=None,
    catchup=False,
    tags=["layer:ingest", "entity:generation", "tool:pokeapi"],
    doc_md="""
## Step 5 — ingest__generations

Fetches all generations from PokeAPI and loads them into `raw.generations`.
Generations are static data (new one roughly every 2 years) — trigger manually
when a new game generation is released.

**Trigger:** manual
**Triggers next:** `transform__generations` (via asset `raw/generations`)
""",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def ingest_generations():

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
        except requests.RequestException:
            return PokeReturnValue(is_done=False)

    @task
    def get_missing_generation_ids():
        """Return only generation IDs not yet in raw.generations"""
        resp = requests.get('https://pokeapi.co/api/v2/generation/', timeout=15)
        resp.raise_for_status()
        api_count = resp.json()["count"]

        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            db_count = con.execute("SELECT COUNT(*) FROM raw.generations").fetchone()[0]
        finally:
            con.close()

        if db_count >= api_count:
            LOG.info(f"raw.generations has {db_count}/{api_count} — nothing to ingest.")
            return []

        missing = list(range(db_count + 1, api_count + 1))
        LOG.info(f"raw.generations has {db_count}/{api_count} — ingesting IDs: {missing}")
        return missing

    @task.short_circuit
    def check_if_ingestion_required(generation_ids):
        """Short-circuit if there are no missing generations"""
        return bool(generation_ids)

    @task
    def fetch_and_import_generation(generation):
        """Fetch a single generation's data and insert into raw"""
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
        except requests.RequestException:
            LOG.info(f"Could not fetch generation {generation}!")
            raise

        data_subset = {
            "fetch_date": fetch_date,
            "batch_id": run_id,
            "payload": json.dumps({
                "id": data["id"],
                "name": data["name"],
                "names": data["names"],
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
                INSERT INTO raw.generations
                SELECT fetch_date, batch_id, payload::JSON
                FROM arrow_table
            """)
        finally:
            con.close()

    @task(outlets=[generations_raw_asset])
    def mark_generation_complete():
        LOG.info("All generations ingested successfully.")

    check = api_check()
    gen_ids = get_missing_generation_ids()
    gate = check_if_ingestion_required(gen_ids)
    fetch_tasks = fetch_and_import_generation.expand(generation=gen_ids)

    check >> gen_ids >> gate >> fetch_tasks >> mark_generation_complete()

ingest_generations()
