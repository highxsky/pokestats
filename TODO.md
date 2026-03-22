# Streamlit Pokedex — TODO

## 0. Housekeeping

- [x] Fix syntax error in `queries.py` (missing comma line 9)
- [ ] Remove or use `VERSIONS` query in `queries.py`
- [x] Add `plotly` and `anthropic` to project dependencies

## 1. Foundation

### Database layer (`db.py`, `queries.py`)

- [x] Add `POKEMON_LIST` query (pokemons + stats + rank/tier)
- [x] Add `POKEMON_TYPES` query (poke_id, slot, type)
- [x] Add `POKEMON_MOVES` query (poke_id + move details)

### App config (`app.py`)

- [x] Set `st.set_page_config` (title, icon, wide layout)

## 2. Pokedex Explorer (`pages/pokedex.py`)

### Filters

- [x] Generation dropdown (from `mart_generations`)
- [x] Pokemon dropdown filtered by generation (with poke_id)
- [ ] Type multi-select (18 types)
- [ ] Stat range sliders (total stat points)

### Pokemon card — layout rework needed

- [x] Sprite image (PokeAPI GitHub sprites via `poke_id`)
- [x] Name, types, tier + power rank
- [x] Height comparison bars (vs avg human)
- [x] Weight comparison circles (vs avg human)
- [x] Radar chart (hp/atk/def/spa/spd/spe)
- [x] Type-based color theming (all charts use primary type color)
- [ ] Reorder: pokemon id + name first, then sprite below
- [ ] Key info on the left, height/weight below with clear separation line
- [ ] Fix weight section alignment (title, circles center, legend)
- [ ] Add separation line between identity section and height/weight section
- [ ] Radar chart: remove background fill, white grid lines, type-colored polygon only
- [ ] Center radar chart properly

### Pokemon detail view

- [ ] Moves table (name, type, power, accuracy, damage class)

## 3. Ask the Pokedex (`pages/ask.py`)

### Setup

- [ ] Add Anthropic API key to `secrets.toml`
- [ ] Set up Anthropic SDK client

### Chat UI

- [ ] Implement `st.chat_input` + `st.chat_message` interface
- [ ] Store conversation history in `st.session_state`

### NL-to-SQL pipeline

- [ ] Send user question + `system_prompt.md` to Claude
- [ ] Execute returned SQL via `db.run_query()`
- [ ] Display results as `st.dataframe()`
- [ ] Client-side SQL safety check (reject non-SELECT statements)

## 4. Styling

- [x] Type color map (18 types to canonical Pokemon colors)
- [ ] Type badge styling via `st.markdown` + HTML
- [x] Stat bar/chart coloring (type-based)

## 5. Nice-to-have

- [ ] Compare mode — 2 Pokemon side-by-side with overlaid stat charts
- [ ] Search autocomplete via `st.selectbox` with full name list
- [ ] Generation timeline chart (Pokemon count by gen)
