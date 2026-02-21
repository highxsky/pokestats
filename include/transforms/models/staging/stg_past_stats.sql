{{ config(materialized="view") }}

SELECT
    fetch_date,
    id as poke_id,
    raw->'$.past_stats' AS past_stats
FROM {{ source('raw', 'pokemon_data') }}