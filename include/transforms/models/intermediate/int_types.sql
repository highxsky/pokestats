{{ config(materialized='view') }}

WITH parsed as (
  SELECT
    fetch_date,
    poke_id,
    CAST(je.value->>'$.slot' AS INT) AS slot,
    je.value->>'$.type.name' AS type
  FROM {{ ref('stg_types') }} t,
    json_each(t.types) AS je
)

SELECT
  {{ dbt_utils.generate_surrogate_key(["poke_id", "slot"]) }} AS "type_id",
  p.*
FROM parsed p