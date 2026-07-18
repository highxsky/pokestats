-- One row per (Pokémon, generation, stat) recorded in the past-stats changelog.
-- Explodes the nested past_stats JSON (generation -> stats).

with source as (
  -- Keep only Pokémon that have a past-stats changelog
  select
    fetch_date,
    poke_id,
    past_stats
  from {{ ref('stg_pokemons') }}
  where len(past_stats) <> 0
),

-- Explode the outer array: one row per changelog generation
by_gen as (
  select
    fetch_date,
    poke_id,
    unnest(past_stats) as gen
  from source
),

-- Explode the inner array: one row per stat within each generation
by_stat as (
  select
    fetch_date,
    poke_id,
    split_part(gen.generation.url, '/', -2)::INT as gen_id,
    unnest(gen.stats) as stat
  from by_gen
)

select
  {{ dbt_utils.generate_surrogate_key(['poke_id', 'gen_id', 'stat.stat.name']) }} as past_stat_id,
  fetch_date,
  poke_id,
  gen_id,
  stat.stat.name as stat_name,
  stat.base_stat as stat_value
from by_stat
