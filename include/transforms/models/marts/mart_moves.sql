{{ mart_incremental_load_config('move_id', 'stg_move_details') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_move_details') }}
  {{ incremental_where() }}
)

SELECT
    move_id,
    move_name,
    power,
    accuracy,
    pp,
    priority,
    type,
    damage_class,
    poke_gen
FROM source
