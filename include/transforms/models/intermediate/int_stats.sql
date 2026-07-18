-- One row per (Pokémon, stat) for current stats.
-- Explodes the stats JSON array from stg_pokemons into long format.

with source as (
  select
    fetch_date,
    poke_id,
    stats
  from {{ ref('stg_pokemons') }}
),

-- Explode the stats array: one row per stat
by_stat as (
  select
    fetch_date,
    poke_id,
    unnest(stats) as stat
  from source
)

select
  fetch_date,
  {{ dbt_utils.generate_surrogate_key(["poke_id", "stat.stat.name"]) }} as stat_id,
  poke_id,
  stat.stat.name as stat_name,
  stat.base_stat as stat_value
from by_stat
