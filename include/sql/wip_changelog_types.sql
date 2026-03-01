-- past values
  -- valid from for current value is
    -- Case 1 - The previous gen for a pokemon's slot, from past_types
    -- Case 2 - the gen the pokemon was introduced on, from pokemons
with past_values as (
  select 
    pt.poke_id,
    pt.slot,
    pt.type,
    COALESCE(
      LAG(pt.poke_gen, 1) over (
        partition by pt.poke_id, pt.slot
        order by pt.poke_gen
      ),
      p.poke_gen
    ) as valid_from,
    pt.poke_gen as valid_to
  from pma_tests.past_types pt
  left join pma_tests.pokemons p
    on pt.poke_id = p.poke_id
  where pt.poke_id IN (298, 35)
),

-- current values
  -- valid from for current value is
    -- Case 1 - The latest gen + 1 for a pokemon's slot, from past_types
    -- Case 2 - the gen the pokemon was introduced on, from pokemons
current_values as (
  select 
    t.poke_id,
    t.slot,
    t.type,
    COALESCE(max_poke_gen, p.poke_gen + 1) as valid_from,
    NULL as valid_to
  from pma_tests.types t
  left join pma_tests.pokemons p
    on p.poke_id = t.poke_id
  left join (
    select
      poke_id,
      slot,
      max(poke_gen) as max_poke_gen
    from pma_tests.past_types
    group by poke_id, slot
  ) pt
    on pt.poke_id = t.poke_id
    and pt.slot = t.slot
  where t.poke_id IN (298, 35)
),

all_values as (
  select * from current_values
  UNION ALL
  select * from past_values
)

select 
  *
from all_values
order by poke_id, slot