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

version_group_data_raw_asset = Asset("motherduck://raw/version_groups")

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

dbt_version_group_data = DbtDag(
    dag_id="transform__version_group_data",
    start_date=datetime(2026, 2, 15),
    schedule=version_group_data_raw_asset,
    catchup=False,
    tags=["layer:transform", "entity:version_group", "tool:dbt"],
    doc_md="""
## Step 8 — transform__version_group_data

Runs all dbt models downstream of `raw.version_groups`.

**Trigger:** asset `raw/version_groups` (set by `ingest__version_group_data`)
**Triggers next:** nothing (end of the version group pipeline)
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
        select=["source:raw.version_groups+"],
    ),
    operator_args={
        "on_failure_callback": notify_on_failure,
    },
)
