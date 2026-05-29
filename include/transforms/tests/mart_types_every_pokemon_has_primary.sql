-- every pokemon should have at least one primary type (slot = 1)
-- returns pokemon ids that are missing a primary type
SELECT
    poke_id
FROM {{ ref('mart_types') }}
GROUP BY poke_id
HAVING sum(CASE WHEN slot = 1 THEN 1 ELSE 0 END) = 0
