{{ config(materialized="view") }}

WITH parsed_stats as (
  SELECT
    fetch_date,
    poke_id,
    CAST(je.value->>'$.base_stat' AS INT) AS value,
    je.value->>'$.stat.name' AS stat
  FROM {{ ref('stg_stats') }} st,
    json_each(st.stats) AS je
),

pivoted_stats AS (
  PIVOT (SELECT * FROM parsed_stats)
  ON stat IN ('hp', 'attack', 'special-attack', 'defense', 'special-defense', 'speed')
  USING FIRST(value)
  GROUP BY poke_id, fetch_date
)

SELECT
  fetch_date,
  poke_id,
  hp,
  attack,
  defense,
  "special-attack" as special_attack,
  "special-defense" as special_defense,
  speed
FROM pivoted_stats