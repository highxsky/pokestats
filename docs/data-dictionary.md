# Data Dictionary

The mart layer is the consumption-ready interface to all Pokemon data. These tables power the Streamlit app, the NL-to-SQL query engine, and any ad-hoc analysis.

All mart tables live in the `dbt_dev_marts` schema (dev) or `analytics_marts` (prod) in MotherDuck.

> Column-level descriptions and tests are maintained in [`include/transforms/models/marts/schema.yml`](../include/transforms/models/marts/schema.yml). This document adds business context, relationships, and query guidance that `schema.yml` does not cover.

---

## Table overview

| Table | Grain | Rows (approx) | Description |
|-------|-------|---------------|-------------|
| `mart_pokemons` | 1 row per pokemon | ~1,025 | Core pokemon attributes |
| `mart_stats` | 1 row per pokemon | ~1,025 | Base stats (HP through Speed) + total |
| `mart_types` | 1 row per pokemon per type slot | ~1,800 | Current type assignments |
| `mart_past_types` | 1 row per pokemon per slot per validity window | varies | Historical type changes (SCD Type 2) |
| `mart_moves` | 1 row per move | ~937 | Move attributes (power, accuracy, type) |
| `mart_pokemon_moves` | 1 row per pokemon x move | ~50,000+ | Which pokemon can learn which moves |
| `mart_generations` | 1 row per generation | 9 | Generation names |
| `mart_version_groups` | 1 row per version group | 27 | Version groups mapped to generations |
| `mart_version_group_versions` | 1 row per game version | ~45 | Individual game versions |
| `mart_strongest_pokemons` | 1 row per pokemon | ~1,025 | All pokemon ranked by total stats with tier labels |
| `mart_strongest_starters` | 1 row per starter pokemon | ~27 | Starter pokemon ranked by total stats |

---

## Table details

### mart_pokemons

Core identity table for all pokemon.

| Column | Type | Description |
|--------|------|-------------|
| `poke_id` | INTEGER | Unique pokemon identifier (PK). Matches PokeAPI ID. |
| `poke_name` | VARCHAR | English name (lowercase, e.g. `pikachu`) |
| `poke_gen` | SMALLINT | Generation the pokemon was introduced in (1-9) |
| `height` | DOUBLE | Height in meters |
| `weight` | DOUBLE | Weight in kilograms |

**Joins to:** `mart_stats` on `poke_id`, `mart_types` on `poke_id`, `mart_pokemon_moves` on `poke_id`

---

### mart_stats

One row per pokemon with all six base stats pivoted into columns.

| Column | Type | Description |
|--------|------|-------------|
| `poke_id` | INTEGER | Pokemon identifier (PK) |
| `hp` | INTEGER | Hit points (0-255) |
| `attack` | INTEGER | Physical attack (0-255) |
| `defense` | INTEGER | Physical defense (0-255) |
| `special_attack` | INTEGER | Special attack (0-255) |
| `special_defense` | INTEGER | Special defense (0-255) |
| `speed` | INTEGER | Speed (0-255) |
| `total_stat_points` | INTEGER | Sum of all six stats |

**Joins to:** `mart_pokemons` on `poke_id`

---

### mart_types

Current type assignment per pokemon. Most pokemon have 1-2 types.

| Column | Type | Description |
|--------|------|-------------|
| `type_id` | VARCHAR | Surrogate key: hash of (poke_id, slot) (PK) |
| `poke_id` | INTEGER | Pokemon identifier |
| `slot` | INTEGER | Type slot: 1 = primary, 2 = secondary |
| `type` | VARCHAR | Type name (e.g. `fire`, `water`, `grass`) |

**Joins to:** `mart_pokemons` on `poke_id`

**Valid type values:** normal, fire, water, grass, electric, ice, fighting, poison, ground, flying, psychic, bug, rock, ghost, dark, dragon, steel, fairy

---

### mart_past_types

Full type history with generation-based validity windows (SCD Type 2 pattern). Only pokemon whose types have changed across generations appear here.

| Column | Type | Description |
|--------|------|-------------|
| `type_id` | VARCHAR | Surrogate key: hash of (poke_id, slot, valid_from_gen) (PK) |
| `poke_id` | INTEGER | Pokemon identifier |
| `slot` | INTEGER | Type slot (1 = primary, 2 = secondary) |
| `type` | VARCHAR | Type name |
| `valid_from_gen` | INTEGER | Generation this type assignment became active |
| `valid_to_gen` | INTEGER | Last generation this type was active (NULL = still current) |

**Joins to:** `mart_pokemons` on `poke_id`, `mart_generations` on `valid_from_gen = poke_gen`

---

### mart_moves

One row per move with its core battle attributes.

| Column | Type | Description |
|--------|------|-------------|
| `move_id` | INTEGER | Move identifier (PK). Matches PokeAPI ID. |
| `move_name` | VARCHAR | Move name (lowercase, e.g. `thunderbolt`) |
| `power` | INTEGER | Base power. NULL for status moves (e.g. `growl`). |
| `accuracy` | INTEGER | Accuracy percentage. NULL for moves that never miss. |
| `pp` | INTEGER | Power points (number of uses per battle) |
| `priority` | INTEGER | Priority bracket (-8 to +8, 0 = normal speed) |
| `type` | VARCHAR | Elemental type (e.g. `electric`) |
| `damage_class` | VARCHAR | `physical`, `special`, or `status` |
| `poke_gen` | INTEGER | Generation this move was introduced in |

**Joins to:** `mart_pokemon_moves` on `move_id`, `mart_generations` on `poke_gen`

---

### mart_pokemon_moves

Bridge table linking pokemon to the moves they can learn.

| Column | Type | Description |
|--------|------|-------------|
| `pokemon_move_id` | VARCHAR | Surrogate key: (poke_id, move_id) (PK) |
| `poke_id` | INTEGER | Pokemon identifier |
| `move_id` | INTEGER | Move identifier |

**Joins to:** `mart_pokemons` on `poke_id`, `mart_moves` on `move_id`

**Example query — all moves a pokemon can learn:**
```sql
SELECT m.move_name, m.type, m.power, m.damage_class
FROM mart_pokemon_moves pm
JOIN mart_moves m ON m.move_id = pm.move_id
JOIN mart_pokemons p ON p.poke_id = pm.poke_id
WHERE p.poke_name = 'pikachu'
ORDER BY m.power DESC NULLS LAST
```

---

### mart_generations

| Column | Type | Description |
|--------|------|-------------|
| `poke_gen` | INTEGER | Generation identifier (PK), 1-9 |
| `gen_name` | VARCHAR | English display name (e.g. `Generation I`) |

**Joins to:** `mart_pokemons` on `poke_gen`, `mart_version_groups` on `poke_gen`, `mart_moves` on `poke_gen`

---

### mart_version_groups

A version group is a set of game versions released together (e.g. "red-blue").

| Column | Type | Description |
|--------|------|-------------|
| `version_group_id` | INTEGER | Version group identifier (PK) |
| `version_group_name` | VARCHAR | Name (e.g. `red-blue`, `gold-silver`) |
| `poke_gen` | INTEGER | Generation this version group belongs to |

**Joins to:** `mart_version_group_versions` on `version_group_id`, `mart_generations` on `poke_gen`

---

### mart_version_group_versions

Maps individual game versions to their version groups.

| Column | Type | Description |
|--------|------|-------------|
| `version_id` | INTEGER | Version identifier (PK) |
| `version_name` | VARCHAR | Game name (e.g. `red`, `blue`, `gold`) |
| `version_group_id` | INTEGER | FK to `mart_version_groups` |

---

### mart_strongest_pokemons

All pokemon ranked by total base stats, with a tier label. Materialized as a view (no incremental load).

| Column | Type | Description |
|--------|------|-------------|
| `poke_gen` | SMALLINT | Generation |
| `poke_id` | INTEGER | Pokemon identifier (PK) |
| `poke_name` | VARCHAR | Pokemon name |
| `hp`, `attack`, `defense`, `special_attack`, `special_defense`, `speed` | INTEGER | Individual base stats |
| `total_stat_points` | INTEGER | Sum of all six stats |
| `rank` | INTEGER | Overall rank (1 = strongest) |
| `tier` | VARCHAR | Strength tier based on percentile |

**Tier breakdown:**

| Tier | Percentile | Meaning |
|------|-----------|---------|
| S | Top 5% | Legendary / pseudo-legendary |
| A | Top 5-20% | Very strong |
| B | Top 20-45% | Above average |
| C | 45-70% | Average |
| D | 70-90% | Below average |
| F | Bottom 10% | Weakest |

---

### mart_strongest_starters

Same structure as `mart_strongest_pokemons` but filtered to starter pokemon only. Includes `rank` (among starters).

---

## Relationships diagram

```
mart_generations (poke_gen)
  |
  +--< mart_pokemons (poke_gen)
  |      |
  |      +--< mart_stats (poke_id)
  |      +--< mart_types (poke_id)
  |      +--< mart_past_types (poke_id)
  |      +--< mart_pokemon_moves (poke_id) >-- mart_moves (move_id)
  |      +--< mart_strongest_pokemons (poke_id)
  |      +--< mart_strongest_starters (poke_id)
  |
  +--< mart_version_groups (poke_gen)
         |
         +--< mart_version_group_versions (version_group_id)
```
