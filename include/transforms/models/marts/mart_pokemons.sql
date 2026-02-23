{{ mart_incremental_load_config('poke_id', 'stg_pokemons') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_pokemons') }}
  {{ incremental_where() }}
)

SELECT
    poke_gen,
    poke_id,
    poke_name,
    height,
    weight
FROM source