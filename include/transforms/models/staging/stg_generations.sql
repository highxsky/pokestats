{{ config(materialized="view") }}

WITH parsed AS (
  SELECT
    fetch_date,
    from_json(
      payload,
      '{
        "id": "INT",
        "name": "VARCHAR",
        "names": [{"name": "VARCHAR", "language": {"name": "VARCHAR"}}],
      }'
    ) as p
  FROM {{ source('raw', 'generations') }}
)

SELECT
  fetch_date,
  p.id as poke_gen,
  p.name as gen_api_name,
  list_filter(p.names, lambda n: n.language.name = 'en')[-1].name as gen_name
FROM parsed