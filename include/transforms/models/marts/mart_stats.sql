{{ mart_incremental_load_config('poke_id', 'int_stats') }}

WITH source AS (
  SELECT * FROM {{ ref('int_stats') }}
  {{ incremental_where() }}
)

SELECT
  {{ dbt_utils.star(ref('int_stats'), except=['fetch_date']) }},
  (hp + attack + special_attack + defense + special_defense + speed) as total_stat_points
FROM source