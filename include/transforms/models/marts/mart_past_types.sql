{{ mart_incremental_load_config('type_id', 'int_type_history') }}

WITH source AS (
  SELECT * FROM {{ ref('int_type_history') }}
  {{ incremental_where() }}
)

SELECT
  type_id,
  poke_id,
  slot,
  type,
  valid_from_gen,
  valid_to_gen
FROM source
