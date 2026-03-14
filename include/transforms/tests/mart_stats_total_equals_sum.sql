-- total_stat_points must equal the sum of all 6 individual stats
SELECT
    poke_id,
    total_stat_points,
    (hp + attack + defense + special_attack + special_defense + speed) AS computed_total
FROM {{ ref('mart_stats') }}
WHERE total_stat_points != (hp + attack + defense + special_attack + special_defense + speed)
