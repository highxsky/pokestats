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

generation_data_raw_asset = Asset("motherduck://raw/generations")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

dbt_generation_data = DbtDag(
    dag_id="transform__generation_data",
    start_date=datetime(2026, 2, 15),
    schedule=generation_data_raw_asset,
    catchup=False,
    tags=["layer:transform", "entity:generation", "tool:dbt"],
    doc_md="""
## Step 6 — transform__generation_data

Runs all dbt models downstream of `raw.generations`.

**Trigger:** asset `raw/generations` (set by `ingest__generation_data`)
**Triggers next:** nothing (end of the generation pipeline)
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
        select=["source:raw.generations+"],
    ),
    operator_args={
        "on_failure_callback": notify_on_failure,
    },
)
