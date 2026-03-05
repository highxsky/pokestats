{{ config(materialized='view') }}

-- Fetching types from past types table (i.e. from changelog)
WITH past AS (
    SELECT
        pk.fetch_date,
        pt.poke_id,
        pt.slot,
        pt.type,
        -- In case of multiple changes, fetches the previous one + 1 as valid from
        -- Otherwise it's valid from the gen the pokemon was introduced
        COALESCE(
            LAG(pk.poke_gen) OVER (
            PARTITION BY pt.poke_id, pt.slot
            ORDER BY pt.poke_gen
            ) + 1,
            pk.poke_gen
        ) AS valid_from_gen,
        pk.poke_gen AS valid_to_gen,
    FROM {{ ref('int_past_types') }} pt
    LEFT JOIN {{ ref('stg_pokemons') }} pk
        ON pt.poke_id = pk.poke_id
),

-- Fetching types from current types table
current AS (
    SELECT
        c.fetch_date,
        c.poke_id,
        c.slot,
        c.type,
        -- Valid to 
        COALESCE(
            MAX(p.valid_to_gen) + 1,
            pk.poke_gen
        ) AS valid_from_gen,
        CAST(NULL AS INT) AS valid_to_gen
    FROM {{ ref('int_types') }} c
    LEFT JOIN past p
        ON c.poke_id = p.poke_id
        AND c.slot = p.slot
    LEFT JOIN {{ ref('stg_pokemons') }} pk
        ON c.poke_id = pk.poke_id
    GROUP BY
        c.fetch_date,
        c.poke_id,
        c.slot,
        c.type,
        pk.poke_gen
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
