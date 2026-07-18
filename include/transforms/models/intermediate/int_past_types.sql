-- One row per (Pokémon, generation, slot) recorded in the past-types changelog.
-- Explodes the nested past_types JSON (generation -> types).

with source as (
  -- Keep only Pokémon that have a past-types changelog
  select
    fetch_date,
    poke_id,
    past_types
  from {{ ref('stg_pokemons') }}
  where len(past_types) <> 0
),

-- Explode the outer array: one row per changelog generation
by_gen as (
  select
    fetch_date,
    poke_id,
    unnest(past_types) as gen
  from source
),

-- Explode the inner array: one row per type slot within each generation
by_slot as (
  select
    fetch_date,
    poke_id,
    split_part(gen.generation.url, '/', -2)::INT as gen_id,
    unnest(gen.types) as typ
  from by_gen
)

select
  {{ dbt_utils.generate_surrogate_key(['poke_id', 'gen_id', 'typ.slot']) }} as past_type_id,
  fetch_date,
  poke_id,
  gen_id,
  typ.slot,
  typ.type.name as type_name
from by_slot
