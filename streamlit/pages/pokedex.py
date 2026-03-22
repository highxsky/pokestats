# ----------
# Imports
# ----------

import streamlit as st
import plotly.graph_objects as go
import db
import queries

# ----------
# Constants
# ----------

HUMAN_HEIGHT_M = 1.7
HUMAN_WEIGHT_KG = 70.0

STAT_LABELS = ["HP", "Attack", "Defense", "Sp. Atk", "Sp. Def", "Speed"]
STAT_COLUMNS = ["hp", "attack", "defense", "special_attack", "special_defense", "speed"]
STAT_MAX = 255

TYPE_COLORS = {
    "normal":   {"solid": "rgb(168, 168, 120)", "fill": "rgba(168, 168, 120, 0.35)"},
    "fire":     {"solid": "rgb(240, 128, 48)",  "fill": "rgba(240, 128, 48, 0.35)"},
    "water":    {"solid": "rgb(104, 144, 240)", "fill": "rgba(104, 144, 240, 0.35)"},
    "electric": {"solid": "rgb(248, 208, 48)",  "fill": "rgba(248, 208, 48, 0.35)"},
    "grass":    {"solid": "rgb(120, 200, 80)",  "fill": "rgba(120, 200, 80, 0.35)"},
    "ice":      {"solid": "rgb(152, 216, 216)", "fill": "rgba(152, 216, 216, 0.35)"},
    "fighting": {"solid": "rgb(192, 48, 40)",   "fill": "rgba(192, 48, 40, 0.35)"},
    "poison":   {"solid": "rgb(160, 64, 160)",  "fill": "rgba(160, 64, 160, 0.35)"},
    "ground":   {"solid": "rgb(224, 192, 104)", "fill": "rgba(224, 192, 104, 0.35)"},
    "flying":   {"solid": "rgb(168, 144, 240)", "fill": "rgba(168, 144, 240, 0.35)"},
    "psychic":  {"solid": "rgb(248, 88, 136)",  "fill": "rgba(248, 88, 136, 0.35)"},
    "bug":      {"solid": "rgb(168, 184, 32)",  "fill": "rgba(168, 184, 32, 0.35)"},
    "rock":     {"solid": "rgb(184, 160, 56)",  "fill": "rgba(184, 160, 56, 0.35)"},
    "ghost":    {"solid": "rgb(112, 88, 152)",  "fill": "rgba(112, 88, 152, 0.35)"},
    "dragon":   {"solid": "rgb(112, 56, 248)",  "fill": "rgba(112, 56, 248, 0.35)"},
    "dark":     {"solid": "rgb(112, 88, 72)",   "fill": "rgba(112, 88, 72, 0.35)"},
    "steel":    {"solid": "rgb(184, 184, 208)", "fill": "rgba(184, 184, 208, 0.35)"},
    "fairy":    {"solid": "rgb(238, 153, 172)", "fill": "rgba(238, 153, 172, 0.35)"},
}

# ----------
# Data
# ----------

df_pokemons = db.run_query(queries.POKEMON_LIST)
df_types = db.run_query(queries.POKEMON_TYPES)
df_generations = db.run_query(queries.POKEMON_GENERATIONS)

# ----------
# Sidebar filters
# ----------

st.sidebar.header("Filters")

# Generation dropdown
gen_options = df_generations.sort_values("poke_gen")
selected_gen = st.sidebar.selectbox(
    "Generation",
    options=gen_options["poke_gen"].tolist(),
    format_func=lambda x: gen_options.loc[gen_options["poke_gen"] == x, "gen_name"].iloc[0],
)

# Pokemon dropdown (filtered by generation)
gen_pokemons = df_pokemons[df_pokemons["poke_gen"] == selected_gen].sort_values("poke_id")
selected_pokemon = st.sidebar.selectbox(
    "Pokemon",
    options=gen_pokemons["poke_id"].tolist(),
    format_func=lambda x: f"#{x} — {gen_pokemons.loc[gen_pokemons['poke_id'] == x, 'poke_name'].iloc[0].capitalize()}",
)

# ----------
# Selected Pokemon data
# ----------

pokemon = df_pokemons[df_pokemons["poke_id"] == selected_pokemon].iloc[0]
pokemon_types = df_types[df_types["poke_id"] == selected_pokemon].sort_values("slot")

sprite_url = f"https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/{selected_pokemon}.png"

# ----------
# Type color
# ----------

primary_type = pokemon_types.iloc[0]["type"]
type_color = TYPE_COLORS.get(primary_type, TYPE_COLORS["normal"])
HUMAN_COLOR = "rgba(180, 180, 180, 0.6)"

# ----------
# Pokemon card
# ----------

col_info, col_chart = st.columns([1, 2])

# -- Left column: identity + physical stats --
with col_info:
    st.image(sprite_url, width=150)
    st.markdown(f"### #{int(pokemon['poke_id'])} — {pokemon['poke_name'].capitalize()}")

    # Types
    types_list = pokemon_types["type"].str.capitalize().tolist()
    st.markdown(f"**Type:** {' / '.join(types_list)}")

    # Tier & rank
    tier = pokemon["tier"] if pokemon["tier"] else "—"
    rank = int(pokemon["rank"]) if pokemon["rank"] else "—"
    st.markdown(f"**Tier:** {tier} | **Power Rank:** #{rank}")

    # Height & Weight comparison
    col_height, col_weight = st.columns([1, 1], gap="large")

    height = pokemon["height"]
    weight = pokemon["weight"]
    poke_name = pokemon["poke_name"].capitalize()

    # Height: two vertical bars side by side
    with col_height:
        fig_h = go.Figure()
        fig_h.add_trace(go.Bar(
            x=["Avg. Human"], y=[HUMAN_HEIGHT_M], width=[0.3],
            marker_color=HUMAN_COLOR,
            text=[f"{HUMAN_HEIGHT_M}m"], textposition="outside",
        ))
        fig_h.add_trace(go.Bar(
            x=[poke_name], y=[height], width=[0.3],
            marker_color=type_color["fill"],
            text=[f"{height}m"], textposition="outside",
        ))
        y_max = max(height, HUMAN_HEIGHT_M) * 1.3
        fig_h.update_layout(
            title=dict(text="Height", x=0.5),
            xaxis=dict(showticklabels=True, showgrid=False),
            yaxis=dict(range=[0, y_max], visible=False),
            showlegend=False, bargap=0.5,
            margin=dict(l=10, r=10, t=40, b=20), height=250,
        )
        st.plotly_chart(fig_h, use_container_width=True)

    # Vertical separator + Weight
    with col_weight:
        max_r = max(weight, HUMAN_WEIGHT_KG)
        human_size = (HUMAN_WEIGHT_KG / max_r) * 80
        poke_size = (weight / max_r) * 80

        fig_w = go.Figure()
        # Larger circle in back, smaller in front
        if weight >= HUMAN_WEIGHT_KG:
            fig_w.add_trace(go.Scatter(
                x=[0], y=[0], mode="markers",
                marker=dict(size=poke_size, color=type_color["fill"]),
                name=f"{poke_name} ({weight}kg)",
            ))
            fig_w.add_trace(go.Scatter(
                x=[0], y=[0], mode="markers",
                marker=dict(size=human_size, color=HUMAN_COLOR),
                name=f"Avg. Human ({HUMAN_WEIGHT_KG}kg)",
            ))
        else:
            fig_w.add_trace(go.Scatter(
                x=[0], y=[0], mode="markers",
                marker=dict(size=human_size, color=HUMAN_COLOR),
                name=f"Avg. Human ({HUMAN_WEIGHT_KG}kg)",
            ))
            fig_w.add_trace(go.Scatter(
                x=[0], y=[0], mode="markers",
                marker=dict(size=poke_size, color=type_color["fill"]),
                name=f"{poke_name} ({weight}kg)",
            ))
        fig_w.update_layout(
            title=dict(text="Weight", x=0.5),
            xaxis=dict(visible=False, range=[-1, 1]),
            yaxis=dict(visible=False, range=[-1, 1]),
            showlegend=True,
            legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
            margin=dict(l=10, r=10, t=40, b=20), height=250,
        )
        st.plotly_chart(fig_w, use_container_width=True)

# -- Right column: radar chart --
with col_chart:
    stat_values = [int(pokemon[col]) for col in STAT_COLUMNS]

    st.markdown(f"#### Power Rank #{rank} — Total: {int(pokemon['total_stat_points'])}")

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=stat_values + [stat_values[0]],
        theta=STAT_LABELS + [STAT_LABELS[0]],
        fill="toself",
        fillcolor=type_color["fill"],
        line=dict(color=type_color["solid"], width=2),
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, STAT_MAX]),
        ),
        showlegend=False,
        margin=dict(l=40, r=40, t=20, b=20),
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)
