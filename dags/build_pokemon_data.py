# --------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------

from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import Asset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig

from include.callbacks import notify_on_failure

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

DBT_PROJECT_PATH = Path(__file__).parent.parent / "include" / "transforms"

# --------------------------------------------------------------------------------
# Asset
# --------------------------------------------------------------------------------

pokemon_data_raw_asset = Asset("motherduck://raw/pokemon_data")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

dbt_pokemon_data = DbtDag(
    dag_id="dbt_pokemon_data",
    start_date=datetime(2026, 2, 15),
    schedule=pokemon_data_raw_asset,
    catchup=False,
    tags=["pokemon_data", "elt", "dbt", "build"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name="transforms",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
    ),
    render_config=RenderConfig(
        select=["source:raw.pokemon_data+"],
    ),
    operator_args={
        "on_failure_callback": notify_on_failure,
    },
)
