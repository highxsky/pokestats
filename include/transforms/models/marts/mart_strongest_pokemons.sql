{{ mart_incremental_load_config('poke_id', 'stg_pokemons') }}

WITH source AS (
  SELECT * FROM {{ ref('stg_pokemons') }}
  {{ incremental_where() }}
),

ranked AS (
  SELECT
    s.poke_gen,
    s.poke_id,
    s.poke_name,
    {{ dbt_utils.star(from=ref('mart_stats'), except=['poke_id']) }},
    ROW_NUMBER() OVER (ORDER BY ms.total_stat_points DESC) AS rank,
    PERCENT_RANK() OVER (ORDER BY ms.total_stat_points) AS pct_rank
  FROM source s
  INNER JOIN {{ ref('mart_stats') }} ms
    ON ms.poke_id = s.poke_id
)

SELECT
  * EXCLUDE (pct_rank),
  CASE
    WHEN pct_rank >= 0.95 THEN 'S'
    WHEN pct_rank >= 0.80 THEN 'A'
    WHEN pct_rank >= 0.55 THEN 'B'
    WHEN pct_rank >= 0.30 THEN 'C'
    WHEN pct_rank >= 0.10 THEN 'D'
    ELSE                       'F'
  END AS tier
FROM ranked