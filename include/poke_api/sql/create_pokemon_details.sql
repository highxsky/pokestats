-- Create table with pokemon attributes
CREATE TABLE IF NOT EXISTS pokemon_details (
	id INTEGER PRIMARY KEY,
    generation INTEGER,
    height INTEGER,
    weight INTEGER
    -- front_sprite BLOB,
    -- back_sprite BLOB
);