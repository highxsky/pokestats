-- Queries to truncate tables (keep table, remove content)

-- Raw
TRUNCATE TABLE raw.pokemons;

-- Staging
TRUNCATE TABLE staging.pokemons;

-- Analytics
TRUNCATE TABLE analytics.pokemons;
TRUNCATE TABLE analytics.types;
TRUNCATE TABLE analytics.stats;