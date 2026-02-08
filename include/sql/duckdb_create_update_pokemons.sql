-- Create table if it doesn't exist
CREATE TABLE IF NOT EXISTS analytics.pokemons (
  "poke_id" INTEGER PRIMARY KEY,
  "name" VARCHAR,
  "height" DECIMAL(10, 2),
  "weight" DECIMAL(10, 2),
  "generation" INTEGER
);

-- Insert records
INSERT INTO analytics.pokemons (poke_id, name, height, weight)
SELECT
  id as "poke_id",
  name,
  height,
  weight
  -- pkm.generation,
FROM staging.pokemons
WHERE batch_id = ?