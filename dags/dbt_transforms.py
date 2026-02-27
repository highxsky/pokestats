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

import logging
from pathlib import Path
from airflow.sdk import dag, Asset
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

LOG = logging.getLogger(__name__)


def notify_on_failure(context):
    from airflow.models import Variable
    from airflow.utils.email import send_email
    alert_email = Variable.get("alert_email", default_var=None)
    if not alert_email:
        LOG.warning("Alert email variable not set, skipping failure email.")
        return None
    
    dag_id  = context["dag"].dag_id
    run_id  = context["dag_run"].run_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exc     = context.get("exception")

    send_email(
        to=[alert_email],
        subject=f"[Airflow] DAG `{dag_id}` failed",
        html_content=f"""
            <h3>Pipeline Failure Alert</h3>
            <b>DAG:</b> {dag_id}<br>
            <b>Run ID:</b> {run_id}<br>
            <b>Failed Task:</b> {task_id}<br>
            <b>Error:</b> {exc}<br>
            <b>Logs:</b> <a href="{log_url}">View logs</a>
        """,
    )

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
