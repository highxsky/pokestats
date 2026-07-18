{{ config(materialized="table") }}

-- Extracting required fields from raw pokemons source
with parsed as (
  select
    fetch_date,
    id as poke_id,
    from_json(
      payload,
      '{
                "name": "VARCHAR",
                "height": "INT",
                "weight": "INT",
                "moves": [{"move": {"url": "VARCHAR"}}],
                "stats": [{"base_stat": "INT", "stat": {"name": "VARCHAR"}}],
                "types": [{"slot": "INT", "type": {"name": "VARCHAR"}}],
                "past_stats": [{
                    "generation": {"url": "VARCHAR"},
                    "stats": [{
                        "base_stat": "INT", 
                        "stat": {"name": "VARCHAR"}
                    }]
                }],
                "past_types": [{
                    "generation": {"url": "VARCHAR"},
                    "types": [{
                        "slot": "INT",
                        "type": {"name": "VARCHAR"}
                    }]
                }]
            }'
    ) as p
  from {{ source('raw', 'pokemons') }}
)

-- Pre-processing and casting

select
  fetch_date,
  poke_id,
  p.name as poke_name,
  p.moves,
  p.stats,
  p.types,
  p.past_stats,
  p.past_types,
  round(p.height / 10, 2) as height,
  round(p.weight / 10, 2) as weight
from parsed
