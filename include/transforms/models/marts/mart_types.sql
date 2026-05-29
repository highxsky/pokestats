{{ mart_incremental_load_config('poke_id', 'int_types') }}

WITH source AS (
  SELECT * FROM {{ ref('int_types') }}
  {{ incremental_where() }}
)

SELECT
  {{ dbt_utils.star(ref('int_types'), except=['fetch_date']) }}
FROM source