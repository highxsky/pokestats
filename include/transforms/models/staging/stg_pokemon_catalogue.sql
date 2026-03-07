-- Query to fetch from raw to staging and materialize as view
{{ config(materialized="view") }}

WITH raw_input AS (
  SELECT
    fetch_date,
    cast(raw->'$.id' AS INT) AS poke_gen,
    raw->'$.pokemon_species' AS pkm_species
  FROM {{ source('raw', 'pokemon_catalogue') }}
)

SELECT
  ri.fetch_date,
  ri.poke_gen,
  cast(split_part(je.value->>'$.url', '/', -2) AS INT) AS poke_id,
  je.value->>'$.name' AS poke_name
FROM raw_input ri,
json_each(ri.pkm_species) AS je