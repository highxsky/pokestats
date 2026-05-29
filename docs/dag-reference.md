# DAG Reference

Operational reference for every DAG in the project. Use this when running, debugging, or extending pipelines.

For the *why* behind these patterns, see [architecture.md](architecture.md). For table schemas, see [data-dictionary.md](data-dictionary.md).

---

## Quick reference

| DAG | Schedule | Trigger | Produces asset | Idempotent |
|-----|----------|---------|---------------|------------|
| `setup__motherduck` | manual | - | - | Yes (IF NOT EXISTS) |
| `ingest__pokemon_catalogue` | `@weekly` | - | `raw/pokemon_catalogue` | Yes (dedup on gen ID) |
| `transform__pokemon_catalogue` | asset | `raw/pokemon_catalogue` | `staging/stg_pokemon_catalogue` | Yes (view) |
| `ingest__pokemons` | asset | `staging/stg_pokemon_catalogue` | `raw/pokemons` | Yes (ANTI JOIN) |
| `transform__pokemons` | asset | `raw/pokemons` | - | Yes (incremental) |
| `ingest__generations` | manual | - | `raw/generations` | Yes (count check) |
| `transform__generations` | asset | `raw/generations` | - | Yes (incremental) |
| `ingest__version_groups` | manual | - | `raw/version_groups` | Yes (count check) |
| `transform__version_groups` | asset | `raw/version_groups` | - | Yes (incremental) |
| `ingest__moves` | asset | `raw/generations` | `raw/moves` | Yes (set difference) |
| `transform__moves` | asset | `raw/moves` | - | Yes (incremental) |
| `check__source_freshness` | `@weekly` | - | - | Yes (read-only) |

---

## DAG details

### setup__motherduck

**Purpose:** One-time setup that creates the `raw` schema and all raw tables in MotherDuck.

**When to run:** Once, on first deploy. Before any ingest DAG.

**Tasks:** `create_schemas` → `create_raw_pokemon_catalogue`, `create_raw_pokemons`, `create_raw_generations`, `create_raw_version_groups`, `create_raw_moves` (in parallel)

**Re-run behavior:** Safe to re-run — all statements use `CREATE IF NOT EXISTS`.

---

### ingest__pokemon_catalogue

**Purpose:** Fetches the list of all pokemon species per generation from PokeAPI into `raw.pokemon_catalogue`.

**Schedule:** `@weekly` — catches new pokemon releases.

**Task flow:**
1. `api_check` — sensor, polls PokeAPI every 60s (timeout 300s)
2. `cleanup_previous_batch` — deletes rows from any previous failed attempt of the same `run_id`
3. `get_generation_ids` — fetches generation count from API, returns `[1, 2, ..., N]`
4. `fetch_and_import_gen_pokemons` — dynamic task mapping: one task per generation. Deduplicates on generation ID during INSERT.
5. `mark_catalogue_complete` — fires asset `raw/pokemon_catalogue`

**Failure remediation:**
- If a single generation fetch fails, the task retries (2 retries, 3 min delay)
- On full re-run, `cleanup_previous_batch` removes partial data from the failed run
- `mark_catalogue_complete` uses `trigger_rule="all_done"` so the asset always fires

---

### ingest__pokemons

**Purpose:** Reads `staging.stg_pokemon_catalogue` to identify pokemon not yet ingested, fetches their full data from PokeAPI in batches of 50.

**Schedule:** Asset-triggered by `staging/stg_pokemon_catalogue`.

**Task flow:**
1. `api_check` — sensor
2. `catalogue_check` — short-circuits if catalogue table is empty
3. `identify_pokemons_not_ingested` — ANTI JOIN to find missing pokemon IDs
4. `check_if_ingestion_required` — short-circuits if nothing to ingest
5. `select_pokemons_to_import` — takes first 50 from the missing list
6. `fetch_and_import_pokemons` — fetches batch, inserts with `WHERE id NOT IN` guard
7. `mark_pokemons_complete` — fires asset `raw/pokemons`

**Failure remediation:**
- Partial batches are safe: the `NOT IN` guard prevents duplicates on re-run
- If API calls fail, failed IDs are logged and the task raises `RuntimeError`
- Re-triggering the asset will restart ingestion for remaining pokemon

**Note:** Processes 50 pokemon per run. The asset trigger re-fires the DAG until all ~1,025 pokemon are ingested.

---

### ingest__generations

**Purpose:** Fetches all generations from PokeAPI into `raw.generations`.

**Schedule:** Manual — generations are static (new one every ~2 years with a new game release).

**Task flow:**
1. `api_check` — sensor
2. `get_missing_generation_ids` — compares DB count vs API count, returns missing IDs
3. `check_if_ingestion_required` — short-circuits if nothing to ingest
4. `fetch_and_import_generation` — dynamic task mapping: one task per generation
5. `mark_generation_complete` — fires asset `raw/generations`

**Re-run behavior:** Skips entirely if DB count matches API count.

---

### ingest__version_groups

**Purpose:** Fetches all version groups from PokeAPI into `raw.version_groups`.

**Schedule:** Manual — trigger alongside `ingest__generations` when a new generation is released.

**Task flow:** Same pattern as `ingest__generations` (count comparison, dynamic mapping, short-circuit gate).

**Re-run behavior:** Skips entirely if DB count matches API count.

---

### ingest__moves

**Purpose:** Fetches move details (power, accuracy, type, PP, damage class) from PokeAPI into `raw.moves`. Processes 50 moves per batch.

**Schedule:** Asset-triggered by `raw/generations` — new generation means new moves.

**Task flow:**
1. `api_check` — sensor
2. `get_missing_move_ids` — set difference between API move IDs and DB move IDs (handles gaps in move ID sequence)
3. `check_if_ingestion_required` — short-circuits if nothing to ingest
4. `select_moves_to_import` — takes first 50
5. `fetch_and_import_moves` — fetches batch, inserts into raw
6. `mark_moves_complete` — fires asset `raw/moves`

**Note:** Move IDs have gaps (not sequential), so the DAG fetches the full move list endpoint to get valid IDs.

---

### transform__* DAGs

All transform DAGs follow the same pattern:

- **Engine:** Cosmos `DbtDag` — runs dbt models as individual Airflow tasks
- **Model selection:** `source:raw.<entity>+` — runs all models downstream of the raw source
- **Retries:** 2 retries, 3 min delay (per task)
- **Failure callback:** `notify_on_failure` at both DAG and task level

| Transform DAG | Triggered by | Models run |
|--------------|-------------|------------|
| `transform__pokemon_catalogue` | `raw/pokemon_catalogue` | `stg_pokemon_catalogue` |
| `transform__pokemons` | `raw/pokemons` | All pokemon models (stats, types, moves, rankings) |
| `transform__generations` | `raw/generations` | `stg_generations` → `mart_generations` |
| `transform__version_groups` | `raw/version_groups` | Version group models (excludes `mart_pokemon_moves`) |
| `transform__moves` | `raw/moves` | `stg_move_details` → `mart_moves` |

**Failure remediation:** dbt models are idempotent. Views rebuild fully; incremental tables only process new rows. Safe to re-run any transform DAG at any time.

---

### check__source_freshness

**Purpose:** Runs `dbt source freshness` across all raw sources to verify data is recent.

**Schedule:** `@weekly` — independent of ingestion/transform pipelines.

**Freshness thresholds (from schema.yml):**

| Source | Warn after | Error after |
|--------|-----------|-------------|
| `raw.pokemons` | 15 days | 30 days |
| `raw.pokemon_catalogue` | 180 days | 360 days |
| `raw.generations` | 360 days | 720 days |
| `raw.version_groups` | 360 days | 720 days |
| `raw.moves` | 360 days | 720 days |

---

## Common operations

**Bootstrap a fresh environment:**
1. Trigger `setup__motherduck`
2. Trigger `ingest__pokemon_catalogue` → cascades to `transform__pokemon_catalogue` → `ingest__pokemons` → `transform__pokemons`
3. Trigger `ingest__generations` → cascades to `transform__generations` and `ingest__moves` → `transform__moves`
4. Trigger `ingest__version_groups` → cascades to `transform__version_groups`

**Add data for a new generation:**
1. Trigger `ingest__generations` — picks up the new generation
2. Trigger `ingest__version_groups` — picks up new version groups
3. `ingest__pokemon_catalogue` runs weekly and will pick up new pokemon automatically

**Force a full dbt rebuild:**
```bash
cd include/transforms
dbt run --full-refresh --target dev
```

**Check what data is missing:**
```sql
-- Pokemon not yet ingested
SELECT pc.poke_id FROM staging.stg_pokemon_catalogue pc
ANTI JOIN raw.pokemons pd ON pc.poke_id = pd.id;

-- Moves not yet ingested
SELECT COUNT(*) FROM raw.moves;
```
