-- every pokemon should have at least one primary type (slot = 1)
-- returns pokemon ids that are missing a primary type in their current type assignment
SELECT
    poke_id
FROM {{ ref('mart_types') }}
WHERE valid_to_gen IS NULL
GROUP BY poke_id
HAVING sum(CASE WHEN slot = 1 THEN 1 ELSE 0 END) = 0
