# --------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------

from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import dag
from cosmos.operators.local import DbtSourceFreshnessLocalOperator

from include.callbacks import notify_on_failure

# --------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------

DBT_PROJECT_PATH = Path(__file__).parent.parent / "include" / "transforms"

# --------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------

@dag(
    dag_id="source_freshness",
    start_date=datetime(2026, 2, 15),
    schedule="@weekly",
    catchup=False,
    tags=["data_quality", "dbt", "freshness"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    on_failure_callback=notify_on_failure,
)
def source_freshness():

    DbtSourceFreshnessLocalOperator(
        task_id="dbt_source_freshness",
        project_dir=DBT_PROJECT_PATH,
        profile_name="transforms",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
        on_failure_callback=notify_on_failure,
    )

source_freshness()
