{{ config(materialized="view") }}

WITH raw_input AS (
  SELECT
    fetch_date,
    cast(payload->>'$.id' AS INT) AS version_group_id,
    payload->>'$.name' AS version_group_name,
    cast(split_part(payload->'$.generation'->>'$.url', '/', -2) AS INT) AS poke_gen,
    payload->'$.versions' AS versions
  FROM {{ source('raw', 'version_group_data') }}
)

SELECT
  ri.fetch_date,
  ri.version_group_id,
  ri.version_group_name,
  ri.poke_gen,
  cast(split_part(v.value->>'$.url', '/', -2) AS INT) AS version_id,
  v.value->>'$.name' AS version_name
FROM raw_input ri,
json_each(ri.versions) AS v
