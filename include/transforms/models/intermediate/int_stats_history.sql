-- One row per (Pokémon, stat, validity interval). Combines current stats with the
-- past-stats changelog into generation-ranged validity windows (SCD type-2 style).

-- Fetching stats from past stats table (i.e. from changelog)
with past as (
  select
    ps.fetch_date,
    ps.poke_id,
    ps.stat_name,
    ps.stat_value,
    -- In case of multiple changes, fetches the previous one + 1 as valid from
    -- Otherwise it's valid from the gen the pokemon was introduced
    ps.gen_id as valid_to_gen,
    -- The changelog gen is the last gen the past value applied
    coalesce(
      lag(ps.gen_id) over (
        partition by ps.poke_id, ps.stat_name
        order by ps.gen_id
      ) + 1,
      pc.gen_id
    ) as valid_from_gen
  from {{ ref('int_past_stats') }} as ps
  left join {{ ref('stg_pokemon_catalogue') }} as pc
    on ps.poke_id = pc.poke_id
),

-- Last gen a stat changed, to open the current value's validity right after
latest_past as (
  select
    poke_id,
    stat_name,
    max(valid_to_gen) as last_changed_gen
  from past
  group by poke_id, stat_name
),

-- Fetching stats from current stats table
current_stats as (
  select
    c.fetch_date,
    c.poke_id,
    c.stat_name,
    c.stat_value,
    cast(null as INT) as valid_to_gen,
    coalesce(lp.last_changed_gen + 1, pc.gen_id) as valid_from_gen
  from {{ ref('int_stats') }} as c
  left join latest_past as lp
    on
      c.poke_id = lp.poke_id
      and c.stat_name = lp.stat_name
  left join {{ ref('stg_pokemon_catalogue') }} as pc
    on c.poke_id = pc.poke_id
),

combined as (
  select
    fetch_date,
    poke_id,
    stat_name,
    stat_value,
    valid_from_gen,
    valid_to_gen
  from past
  union all
  select
    fetch_date,
    poke_id,
    stat_name,
    stat_value,
    valid_from_gen,
    valid_to_gen
  from current_stats
)

select
  {{ dbt_utils.generate_surrogate_key(['poke_id', 'stat_name', 'valid_from_gen']) }} as stat_id,
  fetch_date,
  poke_id,
  stat_name,
  stat_value,
  valid_from_gen,
  valid_to_gen
from combined
