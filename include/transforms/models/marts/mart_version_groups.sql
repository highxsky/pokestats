{{ mart_incremental_load_config('version_group_id', 'stg_version_groups') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_version_groups') }}
  {{ incremental_where() }}
)

SELECT DISTINCT
    version_group_id,
    version_group_name,
    poke_gen
FROM source
