{{ mart_incremental_load_config('pokemon_move_id', 'int_moves') }}

WITH source AS (
  SELECT * FROM {{ ref('int_moves') }}
  {{ incremental_where() }}
)

SELECT
    pokemon_move_id,
    poke_id,
    move_id,
    version_group_id,
    learn_method,
    level_learned_at
FROM source
