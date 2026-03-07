{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        poke_id,
        past_types
    FROM {{ ref('stg_past_types') }}
),

parsed AS (
    SELECT
        fetch_date,
        poke_id,
        CAST(STRING_SPLIT(RTRIM(gen->>'$.generation.url', '/'), '/')[-1] AS INT) AS poke_gen,
        CAST(types->>'$.slot' AS INT) AS slot,
        types->>'$.type.name' AS type
    FROM source,
        UNNEST(from_json(source.past_types, '["json"]')) AS t1(gen),
        UNNEST(from_json(gen->'types', '["json"]')) AS t2(types)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'poke_gen', 'slot']) }} AS past_type_id,
    p.*
FROM parsed p