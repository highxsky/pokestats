-- Query to fetch from raw to staging and materialize as view
{{ config(materialized="view") }}

SELECT
    fetch_date,
    generation as poke_gen,
    id as poke_id,
    raw->>'$.name' AS poke_name,
    ROUND(CAST(raw->>'$.height' AS INT) / 10, 2) AS height,
    ROUND(CAST(raw->>'$.weight' AS INT) / 10, 2) AS weight
FROM {{ source('raw', 'pokemon_data') }}