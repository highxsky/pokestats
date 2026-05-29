{{ mart_incremental_load_config('poke_id', 'stg_pokemon_species') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_pokemon_species') }}
  {{ incremental_where() }}
)

SELECT
    poke_id,
    description,
    genus,
    is_legendary,
    is_mythical,
    is_baby,
    color,
    habitat,
    evolves_from_name,
    evolves_from_id
FROM source
