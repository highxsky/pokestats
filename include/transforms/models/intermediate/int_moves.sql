{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        poke_id,
        moves
    FROM {{ ref('stg_moves') }}
),

parsed AS (
    SELECT
        fetch_date,
        poke_id,
        CAST(STRING_SPLIT(RTRIM(move->>'$.move.url', '/'), '/')[-1] AS INT) AS move_id,
        CAST(STRING_SPLIT(RTRIM(vgd->>'$.version_group.url', '/'), '/')[-1] AS INT) AS version_group_id,
        vgd->>'$.move_learn_method.name' AS learn_method,
        CAST(vgd->>'$.level_learned_at' AS INT) AS level_learned_at
    FROM source,
        UNNEST(from_json(source.moves, '["json"]')) AS t1(move),
        UNNEST(from_json(move->'$.version_group_details', '["json"]')) AS t2(vgd)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'move_id', 'version_group_id', 'learn_method']) }} AS pokemon_move_id,
    p.*
FROM parsed p
