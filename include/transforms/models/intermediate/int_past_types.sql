{{ config(materialized="view") }}

WITH source AS (
    SELECT
        fetch_date,
        poke_id,
        past_types
    FROM {{ ref('stg_past_types') }}
),

source_pre_parsed AS (
    SELECT
        pt.fetch_date,
        pt.poke_id,
        CAST(je.value->>'$.generation.url' AS VARCHAR) AS generation_url,
        CAST(je.value->>'$.types' AS JSON) AS past_types
    FROM source pt,
        JSON_EACH(pt.past_types) AS je
),

source_parsed AS (
    SELECT
        spp.fetch_date,
        spp.poke_id,
        CAST(RIGHT(RTRIM(spp.generation_url, '/'), 1) AS INT) AS poke_gen,
        CAST(je.value->>'$.slot' AS INT) AS slot,
        CAST(je.value->>'$.type.name' AS VARCHAR) AS type
    FROM source_pre_parsed spp,
        JSON_EACH(spp.past_types) AS je
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'slot']) }} as type_id,
    sp.* 
FROM source_parsed sp