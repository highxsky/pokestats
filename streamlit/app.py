import streamlit as st

st.set_page_config(
    page_title="Pokemon Streamlit App",
    page_icon="snorlax.png",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "Pokemon streamlit app with a pokedex and NL to SQL feature"
    }
)

pokedex = st.Page("pages/pokedex.py", title="Pokédex Explorer")
ask = st.Page("pages/ask.py", title="Ask the Pokédex")

nav = st.navigation([pokedex, ask])
nav.run()