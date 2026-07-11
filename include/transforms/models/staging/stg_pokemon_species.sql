{{ config(materialized="view") }}

-- Step 1 - raw input
-- Step 2 - parsed input (picking what's needed / relevant)
-- Step 3 - processing + casting types

WITH raw_input AS (
  SELECT
    fetch_date,
    id AS poke_id,
    payload
  FROM {{ source('raw', 'pokemon_species') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY fetch_date DESC) = 1
),

parsed AS (
  SELECT
    fetch_date,
    poke_id,
    from_json(
      payload,
      '{
        "flavor_text_entries": [{"flavor_text": "VARCHAR", "language": {"name": "VARCHAR"}}],
        "genera": [{"genus": "VARCHAR", "language": {"name": "VARCHAR"}}],
        "is_legendary": "BOOLEAN", 
        "is_mythical": "BOOLEAN", 
        "is_baby": "BOOLEAN",
        "color": {"name": "VARCHAR"},
        "habitat": {"name": "VARCHAR"},
        "evolves_from_species": {"name": "VARCHAR", "url": "VARCHAR"},        
      }'
    ) as p
  FROM raw_input
)

SELECT
  fetch_date,
  poke_id,
  REPLACE(
    REPLACE(
      list_filter(p.flavor_text_entries, lambda e: e.language.name = 'en')[-1].flavor_text,
      chr(12), ' '
    ),
    chr(10), ' '
  ) AS "description",
  list_filter(p.genera, lambda g: g.language.name = 'en')[-1].genus AS genus,
  p.is_legendary,
  p.is_mythical,
  p.is_baby,
  p.color.name AS color,
  p.habitat.name AS habitat,
  p.evolves_from_species.name AS evolves_from_name,
  split_part(p.evolves_from_species.url, '/', -2)::INT as evolves_from_id
FROM parsed