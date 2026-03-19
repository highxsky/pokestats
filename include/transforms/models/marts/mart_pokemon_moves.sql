{{ mart_incremental_load_config('pokemon_move_id', 'int_pokemon_moves') }}

WITH source AS (
  SELECT * FROM {{ ref('int_pokemon_moves') }}
  {{ incremental_where() }}
)

SELECT
    pokemon_move_id,
    poke_id,
    move_id
FROM source
