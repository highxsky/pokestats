-- test to ensure the top-ranked pokemon (rank = 1) is always assigned S tier
SELECT
    poke_id
FROM {{ ref('mart_strongest_pokemons') }}
WHERE rank = 1
  AND tier != 'S'
