{{ mart_incremental_load_config('poke_id', 'int_types') }}

WITH source AS (
  SELECT * FROM {{ ref('int_types') }}
  {{ incremental_where() }}
)

SELECT
  type_id,
  poke_id,
  type,
  slot,
  CASE 
    WHEN slot == 1
    THEN True
    ELSE False
  END AS is_primary_slot
FROM source