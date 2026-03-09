{{ mart_incremental_load_config('poke_gen', 'stg_generations') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_generations') }}
  {{ incremental_where() }}
)

SELECT
    poke_gen,
    gen_name
FROM source
