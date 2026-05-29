-- Query to fetch from raw to staging and materialize as view
{{ config(materialized="view") }}

WITH raw_input AS (
  SELECT
    fetch_date,
    cast(payload->'$.id' AS INT) AS poke_gen,
    payload->'$.pokemon_species' AS pkm_species
  FROM {{ source('raw', 'pokemon_catalogue') }}
),

parsed AS (
  SELECT
    ri.fetch_date,
    ri.poke_gen,
    cast(split_part(je.value->>'$.url', '/', -2) AS INT) AS poke_id,
    je.value->>'$.name' AS poke_name
  FROM raw_input ri,
  json_each(ri.pkm_species) AS je
)

SELECT
  fetch_date,
  poke_gen,
  poke_id,
  poke_name
FROM parsed
QUALIFY ROW_NUMBER() OVER (PARTITION BY poke_id ORDER BY fetch_date DESC) = 1