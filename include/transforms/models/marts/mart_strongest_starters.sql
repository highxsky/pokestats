{{
    config(
      materialized='incremental',
      unique_key='poke_id' ,
      incremental_strategy='delete+insert',
      pre_hook="
        SET variable max_fetch_date = (SELECT MAX(fetch_date) FROM {{ ref('stg_pokemons') }});
      ",
    )
}}

WITH source as (
  SELECT
    p.*
  FROM
    {{ ref('stg_pokemons') }} p
  INNER JOIN {{ ref('starter_pokemons') }} sp
    ON sp.poke_id = p.poke_id
  {% if is_incremental() %}
  WHERE fetch_date >= getvariable('max_fetch_date')
  {% endif %}
)

SELECT
    s.poke_gen,
    s.poke_id,
    s.poke_name,
    {{ dbt_utils.star(from=ref('mart_stats'), except=['poke_id']) }}
FROM source s
INNER JOIN {{ ref('mart_stats') }} ms
    on ms.poke_id = s.poke_id