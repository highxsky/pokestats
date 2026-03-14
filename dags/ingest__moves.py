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

generations_raw_asset = Asset("motherduck://raw/generations")
moves_raw_asset = Asset("motherduck://raw/moves")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="ingest__moves",
    start_date=datetime(2026, 2, 15),
    schedule=generations_raw_asset,
    catchup=False,
    tags=["layer:ingest", "entity:moves", "tool:pokeapi"],
    doc_md="""
## ingest__moves

Fetches move detail data (power, accuracy, type, PP, etc.) from PokeAPI
into `raw.moves`. Only fetches moves not already ingested.
Move IDs have gaps so IDs are extracted from the list endpoint.

**Trigger:** asset `raw/generations` (new generation = new moves)
**Triggers next:** `transform__moves` (via asset `raw/moves`)
""",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def ingest_moves():

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
    def get_missing_move_ids():
        """Return move IDs present in the API but not yet in raw.moves"""
        resp = requests.get(
            'https://pokeapi.co/api/v2/move/?limit=2000',
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        api_ids = {
            int(r["url"].rstrip("/").split("/")[-1])
            for r in results
        }

        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            db_ids = {
                row[0]
                for row in con.execute("SELECT id FROM raw.moves").fetchall()
            }
        finally:
            con.close()

        missing = sorted(api_ids - db_ids)
        LOG.info(f"{len(missing)} move(s) to ingest out of {len(api_ids)} total.")
        return missing

    @task.short_circuit
    def check_if_ingestion_required(move_ids):
        """Short-circuit if there are no missing moves"""
        return bool(move_ids)

    @task
    def select_moves_to_import(move_ids):
        return move_ids[:BATCH_SIZE]

    @task(outlets=[moves_raw_asset])
    def fetch_and_import_moves(move_ids_to_ingest):
        """Fetch move detail data and insert into raw.moves"""
        import json
        import pyarrow as pa

        context = get_current_context()
        fetch_date = context["data_interval_end"].isoformat()
        run_id = context["dag_run"].run_id

        move_list = []
        failed_ids = []

        for move_id in move_ids_to_ingest:
            endpoint = f'https://pokeapi.co/api/v2/move/{move_id}/'

            try:
                resp = requests.get(endpoint, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                move_list.append({
                    "id": int(move_id),
                    "fetch_date": fetch_date,
                    "batch_id": run_id,
                    "payload": json.dumps(data),
                })

            except requests.RequestException as e:
                LOG.warning(f"Failed to fetch move {move_id}: {e}")
                failed_ids.append(move_id)

        if move_list:
            schema = pa.schema([
                pa.field("id", pa.int32()),
                pa.field("fetch_date", pa.string()),
                pa.field("batch_id", pa.string()),
                pa.field("payload", pa.large_string()),
            ])

            arrow_table = pa.Table.from_pylist(move_list, schema=schema)

            con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
            try:
                con.register("arrow_table", arrow_table)
                con.execute("""
                    INSERT INTO raw.moves
                    SELECT id, fetch_date, batch_id, payload::JSON
                    FROM arrow_table
                """)
            finally:
                con.close()

        if failed_ids:
            raise RuntimeError(f"Failed to fetch {len(failed_ids)} move(s): {failed_ids}")

    check = api_check()
    missing = get_missing_move_ids()
    gate = check_if_ingestion_required(missing)
    batch = select_moves_to_import(missing)
    gate >> batch

    check >> missing
    fetch_and_import_moves(batch)

ingest_moves()
