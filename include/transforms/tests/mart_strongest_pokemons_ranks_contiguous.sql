-- ranks should be contiguous from 1 to N with no gaps
-- this test fails if the max rank doesn't equal the row count
SELECT
    max(rank) AS max_rank,
    count(*) AS row_count
FROM {{ ref('mart_strongest_pokemons') }}
HAVING max(rank) != count(*)
