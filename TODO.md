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
- [x] Add `POKEMON_STARTERS` query (starter poke_ids)
- [x] Add `POKEMON_LEGENDARIES` query (from seed, temporary until species pipeline runs)
- [x] Add `POKEMON_SPECIES` query (description, genus, legendary, mythical, baby, color, habitat, evolution)

### App config (`app.py`)

- [x] Set `st.set_page_config` (title, icon, wide layout)

## 2. Pokedex Explorer (`pages/pokedex.py`)

### Filters

- [x] Generation dropdown (from `mart_generations`)
- [x] Pokemon dropdown filtered by generation (with poke_id)
- [ ] Type multi-select (18 types)
- [ ] Stat range sliders (total stat points)

### Pokemon card

- [x] Reorder: pokemon id + name first, then sprite below
- [x] Sprite image (PokeAPI GitHub sprites via `poke_id`)
- [x] Type sprites (PokeAPI Sword/Shield type badges)
- [x] Generation display
- [x] Tier + power rank
- [x] Conditional badges (Starter, Legendary)
- [x] Divider between identity section and height/weight
- [x] Height comparison bars with type-colored borders
- [x] Weight circles: human outline only, pokemon filled, with visible edges
- [x] Radar chart: transparent background, white grid lines, angular shape, type-colored polygon
- [x] Radar chart title: "Total Points: N"
- [x] Remove Plotly modebar on hover (all charts)
- [x] Type-based color theming (all charts use primary type color)
- [ ] Fine-tune weight section alignment (title, circles center, legend)
- [ ] Left/right column visual separator
- [ ] Add species info to card: description, genus, habitat, evolves_from (after pipeline runs)

### Pokemon detail view

- [ ] Moves table (name, type, power, accuracy, damage class)

## 3. Pokemon Species Pipeline (new)

### Ingestion (`ingest__pokemon_species.py`)

- [x] DAG: fetch from `/pokemon-species/{id}`, batches of 50, skip-if-exists
- [x] Trigger: asset `staging/stg_pokemon_catalogue`
- [x] Output: asset `raw/pokemon_species`
- [x] Setup: add `raw.pokemon_species` table to `setup__motherduck.py`

### Transform (`transform__pokemon_species.py`)

- [x] DAG: Cosmos DbtDag with selector `source:raw.pokemon_species+`
- [x] Source definition in `schema.yml`

### dbt models

- [x] `stg_pokemon_species.sql` — parse flavor text (en, latest version), genus, booleans, evolution link
- [x] `mart_pokemon_species.sql` — incremental mart
- [x] Schema definitions for both layers

### Post-pipeline

- [ ] Run `setup__motherduck` to create `raw.pokemon_species` table
- [ ] Run `ingest__pokemon_species` to populate data
- [ ] Verify `mart_pokemon_species` data
- [ ] Switch `POKEMON_LEGENDARIES` query from seed to mart
- [ ] Wire species data into Streamlit card

## 4. Ask the Pokedex (`pages/ask.py`)

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

## 5. Styling

- [x] Type color map (18 types to canonical Pokemon colors)
- [ ] Type badge styling via `st.markdown` + HTML
- [x] Stat bar/chart coloring (type-based)

## 6. Nice-to-have

- [ ] Compare mode — 2 Pokemon side-by-side with overlaid stat charts
- [ ] Search autocomplete via `st.selectbox` with full name list
- [ ] Generation timeline chart (Pokemon count by gen)
- [ ] Full evolution chain visualization (requires fetching evolution-chain endpoint)
