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

from pathlib import Path
from datetime import datetime

from airflow.sdk import dag, Asset
from airflow.providers.standard.operators.bash import BashOperator

from include.callbacks import notify_on_failure

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

ROOT_PATH = Path(__file__).parent.parent
DBT_PROJECT_PATH = ROOT_PATH / "include" / "transforms"

# --------------------------------------------------------------------------------
# Asset
# --------------------------------------------------------------------------------

pokemon_raw_asset = Asset("motherduck://raw/pokemon_data")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="dbt_transforms",
    start_date=datetime(2024, 1, 1),
    schedule=pokemon_raw_asset,
    catchup=False,
    tags=["dbt", "transforms"],
    on_failure_callback=notify_on_failure,
)

def dbt_transforms():

    BashOperator(
        task_id="dbt_build",
        on_failure_callback=notify_on_failure,
        bash_command=(
    f"dbt deps --project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH} && "
    f"dbt build --project-dir {DBT_PROJECT_PATH} --profiles-dir {DBT_PROJECT_PATH}"
),
    )

dbt_transforms()
