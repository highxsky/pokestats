SELECT
    id,
    name,
    height,
    weight,
    ROUND(height / weight, 2) as height_weight_ratio
FROM {{ ref('stg_pokemons') }}
WHERE height > 0 and weight > 0
ORDER BY NAME