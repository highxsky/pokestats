{{ mart_incremental_load_config('poke_id', 'int_stats') }}

{% set stat_names = stat_values() %}

with source as (
  select * from {{ ref('int_stats') }}
  {{ incremental_where() }}
),

-- Pivoting long stats to one column per stat
{#- source stats are kebab-case ('special-attack'); snake_case them for clean identifiers #}
pivoted as (
  select
    poke_id,
  {%- for s in stat_names %}
    max(stat_value) filter (where stat_name = '{{ s }}') as {{ s | replace('-', '_') }}{{ ',' if not loop.last }} -- noqa: LT02,LT05
  {%- endfor %}
  from source
  group by poke_id
)

select
  poke_id,
  {%- for s in stat_names %}
    {{ s | replace('-', '_') }},
  {%- endfor %}
  (hp + attack + special_attack + defense + special_defense + speed) as total_stat_points
from pivoted
