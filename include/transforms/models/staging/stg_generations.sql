{{ config(materialized="view") }}

WITH raw_input AS (
  SELECT
    fetch_date,
    cast(payload->>'$.id' AS INT) AS poke_gen,
    payload->>'$.name' AS gen_api_name,
    payload->'$.names' AS names
  FROM {{ source('raw', 'generation_data') }}
)

SELECT
  ri.fetch_date,
  ri.poke_gen,
  ri.gen_api_name,
  je.value->>'$.name' AS gen_name
FROM raw_input ri,
json_each(ri.names) AS je
WHERE je.value->'$.language'->>'$.name' = 'en'
