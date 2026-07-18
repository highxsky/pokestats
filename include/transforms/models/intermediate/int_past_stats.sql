with source as (
  select
    fetch_date,
    poke_id,
    past_stats
  from {{ ref('stg_pokemons') }}
  where len(past_stats) <> 0
),

by_gen as (
  select
    fetch_date,
    poke_id,
    unnest(past_stats) as gen
  from source
),

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
