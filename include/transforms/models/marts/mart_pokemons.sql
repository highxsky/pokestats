{{ 
    config(
      materialized='incremental', 
      unique_key='poke_id' ,
      incremental_strategy='delete+insert',
      pre_hook="
        SET variable max_fetch_date = (SELECT MAX(fetch_date) FROM {{ ref('stg_pokemons') }}); 
      "
    )
}}

WITH source as (
  SELECT
    *
  FROM
    {{ ref('stg_pokemons') }}
  {% if is_incremental() %}
  WHERE fetch_date >= getvariable('max_fetch_date')
  {% endif %}
)

SELECT
    poke_gen,
    poke_id,
    poke_name,
    height,
    weight
FROM source