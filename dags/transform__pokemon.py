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

pokemons_raw_asset = Asset("motherduck://raw/pokemons")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

dbt_pokemons = DbtDag(
    dag_id="transform__pokemons",
    start_date=datetime(2026, 2, 15),
    schedule=pokemons_raw_asset,
    catchup=False,
    tags=["layer:transform", "entity:pokemons", "tool:dbt"],
    doc_md="""
## Step 4 — transform__pokemons

Runs all dbt models downstream of `raw.pokemons` (staging → intermediate → marts).

**Trigger:** asset `raw/pokemons` (set by `ingest__pokemons`)
**Triggers next:** nothing (end of the pokemon pipeline)
""",
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
        select=["source:raw.pokemons+"],
    ),
    operator_args={
        "on_failure_callback": notify_on_failure,
    },
)
