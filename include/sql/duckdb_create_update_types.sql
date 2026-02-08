-- Query to create the table if it doesn't exist
-- Followed by a query to unnest + parse raw json for types (fire, water, ...)
-- And then insert into types table

CREATE TABLE IF NOT EXISTS analytics.types (
  "poke_id" INTEGER,
  "type" VARCHAR,
  "slot" INTEGER ,
  "is_primary_slot" BOOLEAN,
  PRIMARY KEY ("poke_id", "slot")
);

INSERT INTO analytics.types (poke_id, type, slot, is_primary_slot)

-- Step 1 - Unnest
WITH unnested_json AS (
  SELECT
    id as "poke_id",
    UNNEST(types) AS "json_unnested"
  FROM staging.pokemons
  WHERE batch_id = ?
),

-- Step 2 - Parse
parsed_json AS (
  SELECT
    poke_id,
    CAST(json_unnested -> 'type' ->> 'name' AS VARCHAR) AS "type",
    CAST(json_unnested -> 'slot' AS INTEGER) AS "slot",
    CASE 
      WHEN (json_unnested -> 'slot') == 1
      THEN True
      ELSE False
    END AS is_primary_slot
  FROM unnested_json
)

-- Step 3 - Insert
SELECT 
  poke_id, 
  type, 
  slot, 
  is_primary_slot 
FROM parsed_json;
