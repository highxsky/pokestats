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

version_groups_raw_asset = Asset("motherduck://raw/version_groups")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="ingest__version_groups",
    start_date=datetime(2026, 2, 15),
    schedule=None,
    catchup=False,
    tags=["layer:ingest", "entity:version_group", "tool:pokeapi"],
    doc_md="""
## Step 7 — ingest__version_groups

Fetches all version groups from PokeAPI and loads them into `raw.version_groups`.
Version groups are tied to generations — trigger manually alongside `ingest__generations`
when a new generation is released.

**Trigger:** manual
**Triggers next:** `transform__version_groups` (via asset `raw/version_groups`)
""",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def ingest_version_groups():

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
    def get_missing_version_group_ids():
        """Return only version group IDs not yet in raw.version_groups"""
        resp = requests.get(
            'https://pokeapi.co/api/v2/version-group/?limit=100',
            timeout=15,
        )
        resp.raise_for_status()
        api_count = resp.json()["count"]

        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            db_count = con.execute("SELECT COUNT(*) FROM raw.version_groups").fetchone()[0]
        finally:
            con.close()

        if db_count >= api_count:
            LOG.info(f"raw.version_groups has {db_count}/{api_count} — nothing to ingest.")
            return []

        missing = list(range(db_count + 1, api_count + 1))
        LOG.info(f"raw.version_groups has {db_count}/{api_count} — ingesting IDs: {missing}")
        return missing

    @task.short_circuit
    def check_if_ingestion_required(version_group_ids):
        """Short-circuit if there are no missing version groups"""
        return bool(version_group_ids)

    @task
    def fetch_and_import_version_group(version_group_id):
        """Fetch a single version group's data and insert into raw"""
        import pyarrow as pa
        import json

        context = get_current_context()
        fetch_date = context["logical_date"].isoformat()
        run_id = context["dag_run"].run_id

        endpoint = f'https://pokeapi.co/api/v2/version-group/{version_group_id}/'

        try:
            resp = requests.get(endpoint, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            LOG.info(f"Could not fetch version group {version_group_id}!")
            raise

        data_subset = {
            "fetch_date": fetch_date,
            "batch_id": run_id,
            "payload": json.dumps({
                "id": data["id"],
                "name": data["name"],
                "generation": data["generation"],
                "versions": data["versions"],
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
                INSERT INTO raw.version_groups
                SELECT fetch_date, batch_id, payload::JSON
                FROM arrow_table
            """)
        finally:
            con.close()

    @task(outlets=[version_groups_raw_asset], trigger_rule="all_done")
    def mark_version_group_complete():
        LOG.info("All version groups ingested successfully.")

    check = api_check()
    vg_ids = get_missing_version_group_ids()
    gate = check_if_ingestion_required(vg_ids)
    fetch_tasks = fetch_and_import_version_group.expand(version_group_id=vg_ids)

    done = mark_version_group_complete()
    check >> vg_ids >> gate >> fetch_tasks >> done
    gate >> done

ingest_version_groups()
