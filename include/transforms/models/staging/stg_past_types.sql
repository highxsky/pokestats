{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        id as poke_id,
        payload->'$.past_types' AS past_types
    FROM {{ source('raw', 'pokemon_data') }}
)

SELECT * FROM source