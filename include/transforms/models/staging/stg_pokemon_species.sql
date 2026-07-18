{{ config(materialized="view") }}

-- Step 1 - raw input
-- Step 2 - parsed input (picking what's needed / relevant)
-- Step 3 - processing + casting types

with raw_input as (
  select
    fetch_date,
    id as poke_id,
    payload
  from {{ source('raw', 'pokemon_species') }}
  qualify row_number() over (partition by id order by fetch_date desc) = 1
),

parsed as (
  select
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
  from raw_input
)

select
  fetch_date,
  poke_id,
  p.is_legendary,
  p.is_mythical,
  p.is_baby,
  p.color.name as color,
  p.habitat.name as habitat,
  p.evolves_from_species.name as evolves_from_name,
  replace(
    replace(
      list_filter(p.flavor_text_entries, lambda e: e.language.name = 'en')[-1].flavor_text,
      chr(12), ' '
    ),
    chr(10), ' '
  ) as poke_description,
  list_filter(p.genera, lambda g: g.language.name = 'en')[-1].genus as genus,
  split_part(p.evolves_from_species.url, '/', -2)::INT as evolves_from_id
from parsed
