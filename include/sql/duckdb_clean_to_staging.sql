-- Creates the table if it doesn't exist (without any data, just the target structure)
-- Otherwise inserts the records

CREATE TABLE IF NOT EXISTS staging.pokemons(
    batch_id VARCHAR,
    fetch_date TIMESTAMP,
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    height DECIMAL(10, 2),
    weight DECIMAL(10, 2),
    types STRUCT(slot INTEGER, type STRUCT(name VARCHAR, url VARCHAR))[],
    stats STRUCT(base_stat INTEGER, effort INTEGER, stat STRUCT(name VARCHAR, url VARCHAR))[]
);

INSERT INTO staging.pokemons
SELECT
    batch_id,
    fetch_date,
    CAST(id AS INTEGER) as id,
    "name",
    ROUND((height / 10), 2) AS height,
    ROUND((weight / 10), 2) AS weight,
    "types",
    "stats"
FROM
    raw.pokemons
-- only current batch from raw.pokemons
WHERE batch_id = ?