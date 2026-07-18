-- moves per pokemon (pokemon id, move id)

with source as (
  select
    fetch_date,
    poke_id,
    moves
  from {{ ref('stg_pokemons') }}
),

-- Step 1: parse JSON array, fetch only required column
by_move as (
  select
    fetch_date,
    poke_id,
    unnest(moves) as move_entry
  from source
),

parsed as (
  select
    fetch_date,
    poke_id,
    split_part(move_entry.move.url, '/', -2)::INT as move_id
  from by_move
)

select
  fetch_date,
  {{ dbt_utils.generate_surrogate_key(['poke_id', 'move_id']) }} as pokemon_move_id,
  poke_id,
  move_id
from parsed
