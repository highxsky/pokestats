{{ config(materialized='table') }}

SELECT
    type_id,
    poke_id,
    slot,
    type,
    CASE
        WHEN slot = 1 THEN True
        ELSE False
    END AS is_primary_slot,
    valid_from_gen,
    valid_to_gen
FROM {{ ref('int_type_history') }}