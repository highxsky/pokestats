-- Create raw table from dataframe (without any row)
CREATE TABLE IF NOT EXISTS raw.pokemons AS 
SELECT * 
FROM df 
WHERE False;

-- Insert records from dataframe
INSERT INTO raw.pokemons SELECT * from df