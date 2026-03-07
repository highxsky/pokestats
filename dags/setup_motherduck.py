# --------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------

import logging
from datetime import datetime

from airflow.sdk import dag, task

from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.callbacks import notify_on_failure

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

LOG = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="setup_motherduck",
    start_date=datetime(2026, 2, 15),
    schedule=None,
    catchup=False,
    tags=["setup", "motherduck"],
    on_failure_callback=notify_on_failure,
)
def setup_motherduck():

    @task
    def create_schemas():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS raw")
            LOG.info("Schema 'raw' ready.")
        finally:
            con.close()

    @task
    def create_raw_pokemon_catalogue():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.pokemon_catalogue (
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    raw JSON
                )
            """)
            LOG.info("Table 'raw.pokemon_catalogue' ready.")
        finally:
            con.close()

    @task
    def create_raw_pokemon_data():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.pokemon_data (
                    id INTEGER,
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    raw JSON
                )
            """)
            LOG.info("Table 'raw.pokemon_data' ready.")
        finally:
            con.close()

    schemas = create_schemas()
    catalogue_table = create_raw_pokemon_catalogue()
    data_table = create_raw_pokemon_data()

    schemas >> [catalogue_table, data_table]

setup_motherduck()