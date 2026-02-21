{{ 
    config(
      materialized='incremental', 
      unique_key='type_id' ,
      incremental_strategy='delete+insert',
      pre_hook="
        SET variable max_fetch_date = (SELECT MAX(fetch_date) FROM {{ ref('int_types') }}); 
      "
    )
}}

WITH source as (
  SELECT
    *
  FROM
    {{ ref('int_types') }}
  {% if is_incremental() %}
  WHERE fetch_date >= getvariable('max_fetch_date')
  {% endif %}
)

SELECT
  type_id,
  poke_id,
  type,
  slot,
  CASE 
    WHEN slot == 1
    THEN True
    ELSE False
  END AS is_primary_slot
FROM source