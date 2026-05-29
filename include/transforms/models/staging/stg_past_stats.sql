{{ config(materialized="view") }}

SELECT
    fetch_date,
    id as poke_id,
    payload->'$.past_stats' AS past_stats
FROM {{ source('raw', 'pokemons') }}