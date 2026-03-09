-- Query to fetch from raw to staging and materialize as view
{{ config(materialized="view") }}

SELECT
    fetch_date,
    id as poke_id,
    payload->'$.stats' AS stats
FROM {{ source('raw', 'pokemon_data') }}