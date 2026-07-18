-- One row per (Pokémon, move) bridge.
-- Explodes the moves JSON array from stg_pokemons and derives the move id from its URL.

with source as (
  select
    fetch_date,
    poke_id,
    moves
  from {{ ref('stg_pokemons') }}
),

-- Explode the moves array: one row per move entry
by_move as (
  select
    fetch_date,
    poke_id,
    unnest(moves) as move_entry
  from source
),

-- Derive the integer move id from the entry URL
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
