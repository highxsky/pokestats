{{ config(materialized="view") }}

WITH raw_input AS (
  SELECT
    fetch_date,
    id AS poke_id,
    payload
  FROM {{ source('raw', 'pokemon_species') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY fetch_date DESC) = 1
),

en_flavor_text AS (
  SELECT
    ri.poke_id,
    ft->>'$.flavor_text' AS flavor_text
  FROM raw_input ri,
  UNNEST(from_json(ri.payload->'$.flavor_text_entries', '["json"]')) WITH ORDINALITY AS t(ft, pos)
  WHERE ft->>'$.language.name' = 'en'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ri.poke_id ORDER BY pos DESC) = 1
),

en_genus AS (
  SELECT
    ri.poke_id,
    g->>'$.genus' AS genus
  FROM raw_input ri,
  UNNEST(from_json(ri.payload->'$.genera', '["json"]')) AS g
  WHERE g->>'$.language.name' = 'en'
)

SELECT
  ri.fetch_date,
  ri.poke_id,
  REPLACE(REPLACE(ft.flavor_text, chr(12), ' '), chr(10), ' ') AS description,
  eg.genus,
  CAST(ri.payload->>'$.is_legendary' AS BOOLEAN) AS is_legendary,
  CAST(ri.payload->>'$.is_mythical' AS BOOLEAN) AS is_mythical,
  CAST(ri.payload->>'$.is_baby' AS BOOLEAN) AS is_baby,
  ri.payload->>'$.color.name' AS color,
  ri.payload->>'$.habitat.name' AS habitat,
  ri.payload->>'$.evolves_from_species.name' AS evolves_from_name,
  CAST(
    split_part(ri.payload->>'$.evolves_from_species.url', '/', -2) AS INT
  ) AS evolves_from_id
FROM raw_input ri
LEFT JOIN en_flavor_text ft ON ri.poke_id = ft.poke_id
LEFT JOIN en_genus eg ON ri.poke_id = eg.poke_id
