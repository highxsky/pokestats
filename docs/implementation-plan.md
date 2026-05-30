# Implementation Plan — Local DuckDB + Raw Landing Zone + Marts Serving

> **Goal of this document:** move the pipeline off MotherDuck-for-everything onto a
> cheaper, faster, free-tier-proof design, while teaching you the patterns behind it.
>
> **Working style:** this is built for short sessions (a baby nap, a 1–2h evening).
> Every chunk is self-contained, time-boxed, and has a testable *Done when*.
> **Code is intentionally left as hints, not solutions** — you write it. The hints
> point you at the right function/concept so you don't get stuck, but the learning
> is in typing it yourself.

---

## How to use this plan

**Each session:**
1. `git switch -c feat/<chunk-name>` (or keep one long-lived branch `feat/local-duckdb` — your call).
2. Pick the next unchecked chunk. Do **only** that chunk.
3. Hit the *Done when* check before stopping. If you can't, leave a `NOTE:` comment in the code where you stopped.
4. Commit with a message naming the chunk. Tick the box here.

**Legend:** ⏱ = rough time. 🔑 = key concept to learn. ⚠️ = gotcha that will bite you.

**Chunk template** (what each one gives you):
- **Goal** — the one outcome.
- **Read first** — the concept + a doc link, so you understand *why*.
- **Do** — the concrete steps.
- **Hints** — APIs/functions to reach for (not the full answer).
- **Done when** — how you prove it works.

---

## Target architecture (end state)

```
Object storage / local archive (Parquet)  →  local DuckDB        →  MotherDuck (marts only)
  = durable raw landing zone                  = ephemeral build       = serving layer
    (PokeAPI called ONCE, source of truth)      engine (fast, free)     (tiny → free tier)
                                                                              │
                                                                              ▼
                                                                       Streamlit (read-only)
```

**Three principles you're implementing:**
1. **Extract once, load many** — the API is hit once; rebuilds read from files.
2. **Compute/serving separation** — build locally (free, fast); publish only the gold layer.
3. **Idempotency** — every step can re-run safely without dupes or drift.

---

## Phase 0 — Safety net & backup ⏱ ~1 evening total

Do this first. It also doubles as the seed for your raw archive (Phase 2).

### ☐ Chunk 0.1 — Branch + snapshot current state ⏱ ~20 min
- **Goal:** a safe place to work and a way back.
- **Do:** create a working branch. Confirm `streamlit/.streamlit/secrets.toml` is gitignored (it is — `.gitignore:231`) and `.env` too.
- **Done when:** `git status` is clean on a new branch; no secrets show in `git status`.

### ☐ Chunk 0.2 — Export current MotherDuck raw tables to Parquet ⏱ ~45–90 min
- **Goal:** pull everything you've already fetched out of MotherDuck **before** you stop paying it attention. This is your backup *and* the first version of your raw archive.
- **🔑 Read first:** DuckDB `COPY ... TO` and Parquet — https://duckdb.org/docs/sql/statements/copy
- **Do:** connect to MotherDuck once (you have a token), and `COPY` each `raw.*` table to a local `.parquet` file under a new `include/archive/raw/` folder.
- **Hints:** `COPY (SELECT * FROM raw.pokemons) TO 'include/archive/raw/pokemons.parquet' (FORMAT PARQUET);` — repeat per table. A tiny throwaway Python script with `duckdb.connect("md:poke_db?motherduck_token=...")` is fine; this is one-time, not a DAG.
- **⚠️** Do this while MotherDuck still has your data. Don't skip it.
- **Done when:** `include/archive/raw/` holds one Parquet per raw table, and `duckdb.read_parquet()` on each returns the expected row counts.

### ☐ Chunk 0.3 — Gitignore the archive + local DB ⏱ ~10 min
- **Goal:** don't commit data files / the DB into git.
- **Do:** add `include/archive/` and `*.duckdb` to `.gitignore` (you'll move the archive to durable storage later; for now it's local).
- **Done when:** `git status` ignores the Parquet files and any `.duckdb` file.

---

## Phase 1 — Local DuckDB backend ⏱ ~2 evenings total

Switch the *build* engine from MotherDuck to a local file. This alone should fix your slow/failing dbt steps.

### ☐ Chunk 1.1 — Point dbt at a local DuckDB file ⏱ ~30–45 min
- **Goal:** dbt builds into a local file, not `md:poke_db`. **Both** `dev` and `prod` targets go to local DuckDB — MotherDuck does not re-enter until **Phase 3** (publishing the mart layer only).
- **🔑 Read first:** dbt-duckdb profile config — https://github.com/duckdb/dbt-duckdb
- **Do:** in `include/transforms/profiles.yml`, change **both** the `dev` and `prod` `path` from `md:poke_db` to the same local path: `/usr/local/airflow/include/transforms/poke_db.duckdb`. Bump `dev` `threads` from `1` to `4` (`prod` is already `4`). Leave the `schema` values as-is (`dbt_dev` / `analytics`).
- **⚠️** Keep the DB file under `include/`, **not** under `/tmp`. The `.duckdb` file holds your actual data and must persist; `/tmp` is wiped on container restart (it's only used for dbt's regenerable `target/` and `log` artifacts per `dbt_project.yml`). The container path `/usr/local/airflow/include/...` and host path `include/...` are the **same physical file** (bind mount).
- **Done when:** inside the container (`astro dev bash` → `cd include/transforms && dbt debug`) the connection succeeds against the local file with no token. A full `dbt build` will still be empty until Chunks 1.2 (Airflow conn → same file) and 1.3 (seed from Parquet) are done.

### ☐ Chunk 1.2 — Point the Airflow DuckDB connection at the local file ⏱ ~30 min
- **Goal:** ingest DAGs write to the same local file dbt reads.
- **🔑 Read first:** how `DuckDBHook` resolves its connection (`airflow-provider-duckdb`).
- **Do:** in `airflow_settings.yaml`, change `motherduck_conn` to point `conn_host` at the local `.duckdb` path and drop the `motherduck_token` extra. Re-import settings (`astro dev object import`, or restart).
- **Done when:** `setup__motherduck` (rename later) creates schemas/tables in the **local** file; you can query them with the DuckDB CLI on the host.

### ☐ Chunk 1.3 — Seed the local DB from your Parquet backup ⏱ ~30–45 min
- **Goal:** load the data you exported in 0.2 into the fresh local DB so you don't re-fetch from the API.
- **Do:** after `setup__motherduck` has created the raw tables, `INSERT INTO raw.X SELECT * FROM read_parquet('include/archive/raw/X.parquet')` for each table. A one-off task or manual CLI run is fine for now (you'll automate reload in Phase 2).
- **Done when:** local `raw.*` tables have the same row counts as your MotherDuck backup.

### ☐ Chunk 1.4 — Handle the single-writer lock ⏱ ~45–60 min
- **Goal:** stop concurrent DAG tasks from crashing on DuckDB file locks.
- **🔑 Read first:** DuckDB concurrency (one read-write process at a time) — https://duckdb.org/docs/connect/concurrency ; Airflow pools — https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html
- **⚠️** This *will* bite you: two asset-triggered DAGs writing the same file at once → `Conflicting lock` error.
- **Do:** create an Airflow **pool** with **1 slot** (e.g. `duckdb_write`) and assign every task that *writes* to the DB to that pool. Reads can stay outside it. Alternatively/additionally set `max_active_tasks` appropriately.
- **Done when:** triggering two transform/ingest DAGs at once no longer throws lock errors; writes serialize.

---

## Phase 2 — Raw landing zone (extract once) ⏱ ~3–4 evenings total

Make the API archive the source of truth so rebuilds never re-hit PokeAPI. Do this for **one** entity first, prove it, then replicate.

### ☐ Chunk 2.1 — Design the archive layout (no code) ⏱ ~30 min
- **Goal:** decide the folder/partition scheme before writing anything.
- **🔑 Read first:** Hive-style partitioning + `read_parquet` globs — https://duckdb.org/docs/data/partitioning/hive_partitioning
- **Do:** write the layout into this doc, e.g. `archive/raw/<entity>/batch_id=<run_id>/data.parquet`. Decide: one file per batch, append-only, never mutated.
- **Done when:** the scheme is written down and you can explain why it's append-only.

### ☐ Chunk 2.2 — Restructure ONE ingest DAG: Extract → archive → Load ⏱ ~1–2 evenings
- **Goal:** `ingest__pokemons` writes raw payloads to Parquet **first**, then loads the DB *from that file* — one extraction path.
- **🔑 Read first:** your current `dags/ingest__pokemons.py` (it already builds an Arrow table) — you're inserting a write-to-Parquet step before the DB insert.
- **Do:**
  1. After fetching, write the batch to `archive/raw/pokemons/batch_id=<run_id>/data.parquet` (DuckDB `COPY` from the Arrow table, or `pyarrow.parquet.write_table`).
  2. Change the DB insert to read **from the Parquet file** (`read_parquet(...)`) instead of the in-memory Arrow table.
  3. Keep the existing `WHERE id NOT IN (...)` idempotency guard.
- **Hints:** you can register the Arrow table and `COPY (SELECT * FROM arrow_table) TO '<path>' (FORMAT PARQUET)`. Then `INSERT INTO raw.pokemons SELECT ... FROM read_parquet('<path>') WHERE id NOT IN (...)`.
- **⚠️** Fix the Airflow-3 `logical_date` bug while you're in here: asset-triggered runs have `logical_date = None`, so `context["logical_date"].isoformat()` will crash. Use `context["dag_run"].run_after` (or `pendulum.now("UTC")`) for `fetch_date`. See https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
- **Done when:** running `ingest__pokemons` produces a Parquet file in the archive **and** loads the DB from it; deleting the `raw.pokemons` table and re-running the *load* (no API call) restores the data from Parquet.

### ☐ Chunk 2.3 — Add a "rebuild raw from archive" path ⏱ ~45 min
- **Goal:** reconstruct the whole `raw` layer from files with zero API calls.
- **Do:** a small DAG or task that truncates+reloads each `raw.*` from `read_parquet('archive/raw/<entity>/**/*.parquet')`.
- **Done when:** dropping the local DB and running this rebuilds all raw tables offline.

### ☐ Chunk 2.4 — Replicate the pattern to the other ingest DAGs ⏱ ~1 chunk each
- **Goal:** apply 2.2's structure to `species`, `moves`, `generations`, `version_groups`, `catalogue`.
- **⚠️** While doing `generations` and `version_groups`: also add the missing idempotency guard (`WHERE id NOT IN (...)`) and switch from contiguous-ID count math to set-difference (copy the approach `ingest__moves` already uses).
- **Done when:** every ingest DAG archives to Parquet first, loads from file, and is safely re-runnable. Tick one box per entity.
  - ☐ pokemon_species
  - ☐ moves
  - ☐ generations (+ idempotency fix)
  - ☐ version_groups (+ idempotency fix)
  - ☐ pokemon_catalogue

### ☐ Chunk 2.5 — (Optional, later) move the archive to object storage ⏱ ~1 evening
- **Goal:** true durability across rebuilds/CI — local files don't survive a clean machine.
- **🔑 Read first:** DuckDB httpfs / S3 — https://duckdb.org/docs/extensions/httpfs/overview . Cloudflare R2 is S3-compatible and has a generous free tier.
- **Do:** point `COPY`/`read_parquet` at `s3://...` (or `r2://`), store credentials in an Airflow connection. Make the archive path a config var so local vs remote is one switch.
- **Done when:** ingest writes to remote storage and a fresh local DB rebuilds raw from it.

---

## Phase 3 — Publish marts to MotherDuck (serving layer) ⏱ ~1–2 evenings

Only the gold layer goes to the cloud → stays inside the free tier.

### ☐ Chunk 3.1 — Decide: MotherDuck serving vs bundled file (no code) ⏱ ~15 min
- **🔑 Read first:** the trade-off — live endpoint (no redeploy to refresh, good for NL-to-SQL) vs zero-dependency static snapshot.
- **Do:** write your decision + reason in this doc. If you want the deployed app to reflect new data without redeploy → MotherDuck. If a snapshot-on-deploy is fine → skip Phase 3 and bundle the file in Phase 4.
- **Done when:** decision recorded.

### ☐ Chunk 3.2 — Write a publish task: local marts → MotherDuck ⏱ ~1 evening
- **Goal:** push only mart tables to MotherDuck, idempotently.
- **🔑 Read first:** DuckDB `ATTACH` for MotherDuck — https://motherduck.com/docs/key-tasks/loading-data-into-motherduck/loading-from-duckdb/
- **Do:** in a task, open the local DuckDB, `ATTACH 'md:poke_db' AS md (TOKEN '<token from conn>')`, then `CREATE OR REPLACE TABLE md.analytics.<mart> AS SELECT * FROM <local mart>` for each mart.
- **Hints:** `CREATE OR REPLACE` = idempotent full swap; marts are tiny so no need for incremental/merge here. Loop over a list of mart names.
- **⚠️** Don't make this a standalone decoupled DAG — trigger it **off your mart assets** (your existing Asset pattern) or as the final step of the transform DAGs, so it only runs when marts actually change.
- **Done when:** after a transform run, MotherDuck's `analytics` schema holds current marts and **only** marts (no staging/intermediate); re-running the publish produces no dupes.

---

## Phase 4 — Streamlit on the serving layer + deploy ⏱ ~1–2 evenings

### ☐ Chunk 4.1 — Make the Streamlit connection match your Phase 3 choice ⏱ ~30–45 min
- **Goal:** Streamlit reads marts read-only, with the smallest possible footprint.
- **Do (if MotherDuck serving):** keep `db.py` on MotherDuck but **swap to a read-only token** (regenerate one in MotherDuck) and point at the `analytics` schema. **Do (if bundled file):** change `connect_to_db()` to `duckdb.connect("poke_db.duckdb", read_only=True)` and ship the file (Git LFS, or fetch from object storage at startup).
- **🔑 Read first:** `read_only=True` matters — https://duckdb.org/docs/connect/concurrency ; Streamlit secrets — https://docs.streamlit.io/develop/concepts/connections/secrets-management
- **Done when:** the app loads all pages against the serving layer with no write capability.

### ☐ Chunk 4.2 — Rotate & minimize secrets ⏱ ~20 min
- **Goal:** the deployed app holds the *least* privilege.
- **Do:** regenerate the MotherDuck token as **read-only** (the current one in `secrets.toml` is read-write); update Groq key if you ever exposed it. Put them in Streamlit Cloud's secrets UI, not the repo.
- **Done when:** the deployed app works with a read-only token; the read-write token is gone from anything deployed.

### ☐ Chunk 4.3 — Deploy & smoke-test ⏱ ~45 min
- **Done when:** the public Streamlit URL loads the Pokedex and renders a card end-to-end.

---

## Phase 5 — (Optional) hardening, when you fancy it ⏱ small chunks

These came out of the earlier pipeline review and pair well with the work above:

- ☐ Remove `trigger_rule="all_done"` from the `mark_*_complete` outlet tasks so assets emit on **success only** (today a failed fetch still triggers the downstream transform).
- ☐ Switch `api_check` sensors to `mode="reschedule"` to free the worker slot while polling.
- ☐ Extract the duplicated `api_check` + DuckDB-connection boilerplate + `default_args` into `include/` (a DAG-factory / shared helpers module).
- ☐ Fix the doc drift in `docs/architecture.md` (it says `ingest__pokemons` is triggered by `staging/stg_pokemon_catalogue`; code uses `raw/pokemon_ids`).
- ☐ Rename `setup__motherduck` → `setup__duckdb` now that the backend is local.
- ☐ (Stretch) Enable the OpenLineage provider for Airflow→dbt lineage.

---

## Appendix — concepts worth a deeper read (for the learning goal)

- **Medallion architecture** (raw/bronze → staging/silver → marts/gold) — why each layer exists.
- **Idempotency & deterministic partitioning** — why retries/backfills are safe only when writes are idempotent.
- **Extract/Load separation** — the immutable raw landing zone; "extract once, load many."
- **Compute/serving separation** — build where it's cheap, serve where it's accessed.
- **DuckDB single-writer model** — the #1 thing that will surprise you moving off a hosted DB.
- **Airflow Assets (data-aware scheduling)** — you already use these; lean into them for the publish trigger.

> **Reminder on scope:** for ~1K static pokemon you don't *need* most of this — you're
> building it to learn patterns you'll reuse at work. Don't mistake the complexity for
> something the dataset demands. If an evening's energy is low, Phase 1 alone already
> fixes your free-tier pain.
