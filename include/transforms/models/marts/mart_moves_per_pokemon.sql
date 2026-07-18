{{ mart_incremental_load_config('pokemon_move_id', 'int_moves_per_pokemon') }}

with source as (
  select * from {{ ref('int_moves_per_pokemon') }}
  {{ incremental_where() }}
)

select
  pokemon_move_id,
  poke_id,
  move_id
from source
