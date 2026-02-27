{{ config(materialized='view') }}

WITH past AS (
    SELECT
        fetch_date,
        poke_id,
        slot,
        type,
        poke_gen AS valid_to_gen,
        COALESCE(
            LAG(poke_gen) OVER (PARTITION BY poke_id, slot ORDER BY poke_gen) + 1,
            1
        ) AS valid_from_gen
    FROM {{ ref('int_past_types') }}
),

current AS (
    SELECT
        c.fetch_date,
        c.poke_id,
        c.slot,
        c.type,
        COALESCE(MAX(p.valid_to_gen) + 1, 1) AS valid_from_gen,
        CAST(NULL AS INT) AS valid_to_gen
    FROM {{ ref('int_types') }} c
    LEFT JOIN past p ON c.poke_id = p.poke_id
    GROUP BY c.fetch_date, c.poke_id, c.slot, c.type
),

combined AS (
    SELECT * FROM past
    UNION ALL
    SELECT * FROM current
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['poke_id', 'slot', 'valid_from_gen']) }} AS type_id,
    fetch_date,
    poke_id,
    slot,
    type,
    valid_from_gen,
    valid_to_gen
FROM combined
