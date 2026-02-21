{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        id as poke_id,
        raw->'$.past_types' AS past_types
    FROM {{ source('raw', 'pokemon_data') }}
)

SELECT * FROM source