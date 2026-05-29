{{ config(materialized='view') }}

WITH ranked AS (
  SELECT
    mp.poke_gen,
    mp.poke_id,
    mp.poke_name,
    {{ dbt_utils.star(from=ref('mart_stats'), except=['poke_id']) }},
    ROW_NUMBER() OVER (ORDER BY ms.total_stat_points DESC) AS rank,
    PERCENT_RANK() OVER (ORDER BY ms.total_stat_points) AS pct_rank
  FROM {{ ref('mart_pokemons') }} mp
  INNER JOIN {{ ref('mart_stats') }} ms
    ON ms.poke_id = mp.poke_id
)

SELECT
  * EXCLUDE (pct_rank),
  CASE
    WHEN pct_rank >= 0.95 THEN 'S'
    WHEN pct_rank >= 0.80 THEN 'A'
    WHEN pct_rank >= 0.55 THEN 'B'
    WHEN pct_rank >= 0.30 THEN 'C'
    WHEN pct_rank >= 0.10 THEN 'D'
    ELSE 'F'
  END AS tier
FROM ranked