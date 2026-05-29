# imports
import duckdb

# variables
mart_schema_query = """
    SELECT 
        table_name as table, 
        column_name as column, 
        data_type as type 
    FROM information_schema.columns WHERE table_schema = 'dbt_dev_marts'
"""

# script

with duckdb.connect('md:poke_db') as conn:
    try:
        # get the schema (tables, columns, dtype)
        mart_columns_df = conn.sql(mart_schema_query).df()

        # group columns by table
        mart_columns_by_table = (
            mart_columns_df.groupby("table")
            .apply(lambda g: g[["column", "type"]].to_dict("records"))
            .to_dict()
        )

        # build schema with columns + sample rows per table
        tables = mart_columns_df["table"].unique().tolist()
        schema = {}
        for table in tables:
            samples = conn.sql(f"SELECT * FROM dbt_dev_marts.{table} LIMIT 5").df().to_dict("records")
            schema[table] = {
                "columns": mart_columns_by_table[table],
                "sample_rows": samples,
            }

        print(schema)
    except Exception as e:
        print(e)