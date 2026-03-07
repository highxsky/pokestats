-- Query to fetch from raw to staging and materialize as view
{{ config(materialized="view") }}

SELECT
    pd.fetch_date,
    pc.poke_gen,
    pd.id as poke_id,
    pd.raw->>'$.name' AS poke_name,
    ROUND(CAST(pd.raw->>'$.height' AS INT) / 10, 2) AS height,
    ROUND(CAST(pd.raw->>'$.weight' AS INT) / 10, 2) AS weight
FROM {{ source('raw', 'pokemon_data') }} pd
LEFT JOIN {{ ref('stg_pokemon_catalogue') }} pc
    ON pd.id = pc.poke_id