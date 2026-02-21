{{ 
    config(
      materialized='incremental', 
      unique_key='poke_id' ,
      incremental_strategy='delete+insert',
      pre_hook="
        SET variable max_fetch_date = (SELECT MAX(fetch_date) FROM {{ ref('int_stats') }}); 
      "
    )
}}

WITH source as (
  SELECT
    *
  FROM
    {{ ref('int_stats') }}
  {% if is_incremental() %}
  WHERE fetch_date >= getvariable('max_fetch_date')
  {% endif %}
)

SELECT
  {{ dbt_utils.star(ref('int_stats'), except=['fetch_date']) }},
  (hp + attack + special_attack + defense + special_defense + speed) as total_stat_points
FROM source