-- every pokemon must have all 6 stat columns populated (non-null)
-- this test returns rows where any stat is missing
SELECT
    poke_id
FROM {{ ref('int_stats') }}
WHERE
    hp IS NULL
    OR attack IS NULL
    OR defense IS NULL
    OR special_attack IS NULL
    OR special_defense IS NULL
    OR speed IS NULL
