{{ mart_incremental_load_config('poke_id', 'stg_pokemons') }}

WITH source AS (
  SELECT
    p.*
  FROM
    {{ ref('stg_pokemons') }} p
  INNER JOIN {{ ref('starter_pokemons') }} sp
    ON sp.poke_id = p.poke_id
  {{ incremental_where() }}
)

SELECT
    s.poke_gen,
    s.poke_id,
    s.poke_name,
    {{ dbt_utils.star(from=ref('mart_stats'), except=['poke_id']) }},
    ROW_NUMBER() OVER (ORDER BY ms.total_stat_points desc) as rank
FROM source s
INNER JOIN {{ ref('mart_stats') }} ms
    on ms.poke_id = s.poke_id