{{ mart_incremental_load_config('version_id', 'stg_version_groups') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_version_groups') }}
  {{ incremental_where() }}
)

SELECT
    version_id,
    version_name,
    version_group_id
FROM source
