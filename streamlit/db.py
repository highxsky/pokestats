# ----------
# Imports
# ----------

import streamlit as st
import duckdb

# ----------
# Functions
# ----------

@st.cache_resource
def connect_to_db():
    token = st.secrets["motherduck"]["token"]
    database = st.secrets["motherduck"]["database"]
    return duckdb.connect(f"md:{database}?motherduck_token={token}")

conn = connect_to_db()

@st.cache_data
def run_query(sql:str):
    return conn.sql(sql).df()