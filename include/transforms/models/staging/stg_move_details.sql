{{ config(materialized="view") }}

SELECT
    fetch_date,
    CAST(payload->>'id' AS INT) AS move_id,
    payload->>'name' AS move_name,
    CAST(payload->>'power' AS INT) AS power,
    CAST(payload->>'accuracy' AS INT) AS accuracy,
    CAST(payload->>'pp' AS INT) AS pp,
    CAST(payload->>'priority' AS INT) AS priority,
    payload->'type'->>'name' AS type,
    payload->'damage_class'->>'name' AS damage_class,
    CAST(STRING_SPLIT(RTRIM(payload->'generation'->>'url', '/'), '/')[-1] AS INT) AS poke_gen
FROM {{ source('raw', 'moves') }}
