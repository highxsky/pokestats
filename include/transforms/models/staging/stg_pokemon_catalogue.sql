-- One row per Pokémon (latest fetch), mapping each Pokémon to the generation it was introduced in.

with raw_input as (
  select
    fetch_date,
    cast(payload -> '$.id' as INT) as gen_id,
    payload -> '$.pokemon_species' as pkm_species
  from {{ source('raw', 'pokemon_catalogue') }}
),

-- Explode the pokemon_species JSON array: one row per Pokémon in the generation
parsed as (
  select
    ri.fetch_date,
    ri.gen_id,
    cast(split_part(je.value ->> '$.url', '/', -2) as INT) as poke_id,
    je.value ->> '$.name' as poke_name
  from raw_input as ri,
    json_each(ri.pkm_species) as je
)

select
  fetch_date,
  gen_id,
  poke_id,
  poke_name
from parsed
-- Keep only the latest fetch per Pokémon
qualify row_number() over (partition by poke_id order by fetch_date desc) = 1
