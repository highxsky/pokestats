{% set stat_names = stat_values() %}

-- every pokemon must have all 6 current stats (one row per stat)
-- this test returns pokemons that don't have exactly 6 stat rows
SELECT
    poke_id,
    COUNT(DISTINCT stat_name) AS stat_count
FROM {{ ref('int_stats') }}
GROUP BY poke_id
HAVING COUNT(DISTINCT stat_name) <> {{ stat_names | length }}
