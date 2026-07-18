with source as (
  select
    fetch_date,
    poke_id,
    types
  from {{ ref('stg_pokemons') }}
),

by_slot as (
  select
    fetch_date,
    poke_id,
    unnest(types) as slot
  from source
)

select
  fetch_date,
  {{ dbt_utils.generate_surrogate_key(["poke_id", "slot"]) }} as type_id,
  poke_id,
  slot.slot,
  slot.type.name as type_name
from by_slot
