{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        poke_id,
        moves
    FROM {{ ref('stg_pokemon_moves') }}
),

-- Step 1: parse JSON array, fetch only required column
unnested AS (
    SELECT
        fetch_date,
        poke_id,
        UNNEST(FROM_JSON(moves, '[{"move": {"url": "VARCHAR"}}]')) AS m
    FROM source
),

-- Step 2: extract the numeric move_id from each move url
parsed AS (
    SELECT DISTINCT
        fetch_date,
        poke_id,
        CAST(STRING_SPLIT(RTRIM(m.move.url, '/'), '/')[-1] AS INT) AS move_id
    FROM unnested
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'move_id']) }} AS pokemon_move_id,
    p.*
FROM parsed p
