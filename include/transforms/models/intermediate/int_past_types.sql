with source as (
  select
    fetch_date,
    poke_id,
    past_types
  from {{ ref('stg_pokemons') }}
  where len(past_types) <> 0
),

by_gen as (
  select
    fetch_date,
    poke_id,
    unnest(past_types) as gen
  from source
),

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
