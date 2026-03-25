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
    dag_id="setup__motherduck",
    start_date=datetime(2026, 2, 15),
    schedule=None,
    catchup=False,
    tags=["layer:setup", "tool:duckdb"],
    doc_md="""
## Step 0 — setup__motherduck

One-time manual setup. Creates all raw schemas and tables in MotherDuck.

**Trigger:** manual
**Run this before:** anything else, on first deploy only
**Triggers next:** nothing (run `ingest__pokemon_catalogue` manually after)
""",
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
                    payload JSON
                )
            """)
            LOG.info("Table 'raw.pokemon_catalogue' ready.")
        finally:
            con.close()

    @task
    def create_raw_pokemons():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.pokemons (
                    id INTEGER,
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    payload JSON
                )
            """)
            LOG.info("Table 'raw.pokemons' ready.")
        finally:
            con.close()

    @task
    def create_raw_generations():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.generations (
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    payload JSON
                )
            """)
            LOG.info("Table 'raw.generations' ready.")
        finally:
            con.close()

    @task
    def create_raw_version_groups():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.version_groups (
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    payload JSON
                )
            """)
            LOG.info("Table 'raw.version_groups' ready.")
        finally:
            con.close()

    @task
    def create_raw_moves():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.moves (
                    id INTEGER,
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    payload JSON
                )
            """)
            LOG.info("Table 'raw.moves' ready.")
        finally:
            con.close()

    @task
    def create_raw_pokemon_species():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.pokemon_species (
                    id INTEGER,
                    fetch_date TIMESTAMP,
                    batch_id VARCHAR,
                    payload JSON
                )
            """)
            LOG.info("Table 'raw.pokemon_species' ready.")
        finally:
            con.close()

    @task
    def create_raw_pokemon_ids():
        con = DuckDBHook(duckdb_conn_id='motherduck_conn').get_conn()
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS raw.pokemon_ids (
                    poke_id INTEGER,
                    poke_gen INTEGER
                )
            """)
            LOG.info("Table 'raw.pokemon_ids' ready.")
        finally:
            con.close()

    schemas = create_schemas()
    catalogue_table = create_raw_pokemon_catalogue()
    ids_table = create_raw_pokemon_ids()
    data_table = create_raw_pokemons()
    generation_table = create_raw_generations()
    version_group_table = create_raw_version_groups()
    moves_table = create_raw_moves()
    species_table = create_raw_pokemon_species()

    schemas >> [catalogue_table, ids_table, data_table, generation_table, version_group_table, moves_table, species_table]

setup_motherduck()