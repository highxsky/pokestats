import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

if ENVIRONMENT == "prod":
    SCHEMA_BASE = "analytics"
    SCHEMA_STAGING = "staging"
    SCHEMA_INTERMEDIATE = "intermediate"
    SCHEMA_MARTS = "marts"
else:
    SCHEMA_BASE = "dbt_dev"
    SCHEMA_STAGING = "dbt_dev_staging"
    SCHEMA_INTERMEDIATE = "dbt_dev_intermediate"
    SCHEMA_MARTS = "dbt_dev_marts"
