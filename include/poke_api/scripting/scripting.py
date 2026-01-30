#%%

import sqlite3
from pathlib import Path 

poke_dir = Path(__file__).parent.parent
db_path = poke_dir / 'db' / 'poke_db.db'

queries_dir = poke_dir / 'sql'
select_pokemon_details = queries_dir / 'select_pokemon_details.sql'
drop_pokemon_details = queries_dir / 'drop_pokemon_details.sql'
create_pokemon_details = queries_dir / 'create_pokemon_details.sql'
connection = sqlite3.connect(db_path)
cursor = connection.cursor()

sql_query = create_pokemon_details.read_text()
cursor.execute(sql_query)
connection.commit()

cursor.execute("""SELECT * FROM pokemon_details""")
column_names = [description[0] for description in cursor.description]
print(column_names)

#%%

connection.close()

#%%

with sqlite3.connect(db_path) as conn:
    rows = conn.execute(kwery)
    col_names = [description[0] for description in rows.description]
    print(col_names)

#%%