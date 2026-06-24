"""
Seed the local DuckDB database from the Parquet raw archive.

Inverse of raw_md_to_duckdb_backup.py: reloads each include/archive/raw/<table>.parquet
into the local poke_db.duckdb file as raw.<table>. Use it to fill a fresh local database
(e.g. after migrating off MotherDuck) without re-hitting PokeAPI.

DuckDB preserves the JSON type through Parquet, so no casting is needed. Idempotent:
CREATE OR REPLACE reproduces the archived state exactly on every run.

The target database is DUCKDB_PATH if set (same env var dbt reads), else the
in-repo include/transforms/poke_db.duckdb.

Run from anywhere (with the Astro container stopped, so it doesn't fight the write lock):
    .venv/bin/python scripts/seed_raw_from_archive.py
"""

import os
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = REPO_ROOT / "include" / "archive" / "raw"
# Honour DUCKDB_PATH (same env var dbt/profiles.yml reads); fall back to the
# in-repo location so a bare `python scripts/seed_raw_from_archive.py` still works.
DEFAULT_DB_PATH = REPO_ROOT / "include" / "transforms" / "poke_db.duckdb"
DB_PATH = Path(os.environ.get("DUCKDB_PATH", DEFAULT_DB_PATH))
RAW_SCHEMA = "raw"


def main() -> int:
    parquet_files = sorted(ARCHIVE_DIR.glob("*.parquet"))
    if not parquet_files:
        print(f"No Parquet files in {ARCHIVE_DIR} — nothing to seed.", file=sys.stderr)
        return 1

    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        print(f"Seeding {len(parquet_files)} table(s) into {DB_PATH}:\n")

        for pq in parquet_files:
            table = pq.stem
            con.execute(
                f'CREATE OR REPLACE TABLE {RAW_SCHEMA}."{table}" AS '
                f"SELECT * FROM read_parquet('{pq}')"
            )
            loaded = con.execute(f'SELECT count(*) FROM {RAW_SCHEMA}."{table}"').fetchone()[0]
            expected = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
            status = "OK" if loaded == expected else "MISMATCH!"
            print(f"  - {table:<20} parquet={expected:<7} loaded={loaded:<7} {status}")

        print(f"\nDone. Local raw schema seeded at {DB_PATH}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
