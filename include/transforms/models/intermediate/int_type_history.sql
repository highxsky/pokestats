-- One row per (Pokémon, slot, validity interval). Combines current types with the
-- past-types changelog into generation-ranged validity windows (SCD type-2 style).

-- Fetching types from past types table (i.e. from changelog)
with past as (
  select
    pt.fetch_date,
    pt.poke_id,
    pt.slot,
    pt.type_name,
    -- In case of multiple changes, fetches the previous one + 1 as valid from
    -- Otherwise it's valid from the gen the pokemon was introduced
    pt.gen_id as valid_to_gen,
    -- The changelog gen is the last gen the past value applied
    coalesce(
      lag(pt.gen_id) over (
        partition by pt.poke_id, pt.slot
        order by pt.gen_id
      ) + 1,
      pc.gen_id
    ) as valid_from_gen
  from {{ ref('int_past_types') }} as pt
  left join {{ ref('stg_pokemon_catalogue') }} as pc
    on pt.poke_id = pc.poke_id
),

-- Last gen a type changed, to open the current value's validity right after
latest_past as (
  select
    poke_id,
    slot,
    max(valid_to_gen) as last_changed_gen
  from past
  group by poke_id, slot
),

-- Fetching types from current types table
current_types as (
  select
    c.fetch_date,
    c.poke_id,
    c.slot,
    c.type_name,
    cast(null as INT) as valid_to_gen,
    coalesce(lp.last_changed_gen + 1, pc.gen_id) as valid_from_gen
  from {{ ref('int_types') }} as c
  left join latest_past as lp
    on
      c.poke_id = lp.poke_id
      and c.slot = lp.slot
  left join {{ ref('stg_pokemon_catalogue') }} as pc
    on c.poke_id = pc.poke_id
),

combined as (
  select
    fetch_date,
    poke_id,
    slot,
    type_name,
    valid_from_gen,
    valid_to_gen
  from past
  union all
  select
    fetch_date,
    poke_id,
    slot,
    type_name,
    valid_from_gen,
    valid_to_gen
  from current_types
)

select
  {{ dbt_utils.generate_surrogate_key(['poke_id', 'slot', 'valid_from_gen']) }} as type_id,
  fetch_date,
  poke_id,
  slot,
  type_name,
  valid_from_gen,
  valid_to_gen
from combined
