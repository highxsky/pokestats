-- Query to create the table if it doesn't exist
-- Followed by a query to unnest + parse + pivot raw json for stats (hp, speed, ...)
-- And then insert into types table

CREATE TABLE IF NOT EXISTS analytics.stats (
  "poke_id" INTEGER PRIMARY KEY,
  "hp" INTEGER,
  "attack" INTEGER,
  "special_attack" INTEGER,
  "defense" INTEGER,
  "special_defense" INTEGER,
  "speed" INTEGER
);

INSERT INTO analytics.stats (poke_id, hp, attack, special_attack, defense, special_defense, speed)

-- Step 1 - Unnest
WITH unnested_json AS (
  SELECT
    id as "poke_id",
    UNNEST(stats) AS "json_unnested"
  FROM staging.pokemons
  WHERE batch_id = ?
),

-- Step 2 - Parse
parsed_stats AS (
  SELECT
    poke_id,
    CAST(json_unnested -> 'base_stat' AS INTEGER) AS "value",
    CAST(json_unnested -> 'stat'  ->> 'name' AS VARCHAR) AS "stat"
  FROM unnested_json
),

-- Step 3 - Pivot
pivoted_stats AS (
  PIVOT (SELECT * FROM parsed_stats)
  ON stat IN ('hp', 'attack', 'special-attack', 'defense', 'special-defense', 'speed')
  USING FIRST(value)
  GROUP BY poke_id
)

-- Step 4 - Insert
SELECT
  poke_id,
  hp,
  attack,
  "special-attack" as special_attack,
  defense,
  "special-defense" as special_defense,
  speed
FROM pivoted_stats