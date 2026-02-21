SELECT
    id,
    name,
    height
FROM {{ ref('stg_pokemons') }}
ORDER BY height
LIMIT 10