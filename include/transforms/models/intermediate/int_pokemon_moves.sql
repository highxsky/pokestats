{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        poke_id,
        moves
    FROM {{ ref('stg_pokemon_moves') }}
),

parsed AS (
    SELECT DISTINCT
        fetch_date,
        poke_id,
        CAST(STRING_SPLIT(RTRIM(move->>'$.move.url', '/'), '/')[-1] AS INT) AS move_id
    FROM source,
        UNNEST(from_json(source.moves, '["json"]')) AS t1(move)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'move_id']) }} AS pokemon_move_id,
    p.*
FROM parsed p
