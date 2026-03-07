{{ config(materialized="view") }}

WITH source as (
    SELECT
        fetch_date,
        poke_id,
        past_stats
    FROM {{ ref('stg_past_stats') }}
),

parsed AS (
    SELECT
        fetch_date,
        poke_id,
        CAST(STRING_SPLIT(RTRIM(gen->>'$.generation.url', '/'), '/')[-1] AS INT) AS poke_gen,
        CAST(stat->>'$.base_stat' AS INT) AS stat_value,
        stat->>'$.stat.name' AS stat_name
    FROM source,
        UNNEST(from_json(source.past_stats, '["json"]')) AS t1(gen),
        UNNEST(from_json(gen->'stats', '["json"]')) AS t2(stat)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'poke_gen', 'stat_name']) }} AS past_stat_id,
    p.*
FROM parsed p
