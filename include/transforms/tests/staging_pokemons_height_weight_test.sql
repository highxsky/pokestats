-- test to ensure no negative height or weight is input
SELECT
    poke_id
FROM {{ ref('stg_pokemons') }}
WHERE (height < 0 or weight < 0)