"""
Back up the MotherDuck `raw.*` schema to local Parquet files.

One-time / re-runnable safety backup taken before migrating the pipeline off
MotherDuck onto local DuckDB. The exported Parquet files double as the seed for
the raw landing zone (they get reloaded into the local DuckDB file in a later step).

- Token is read from the `motherduck_token` environment variable (never hardcoded).
- Only non-empty tables are exported; empty ones are skipped.
- Each table is written to include/archive/raw/<table>.parquet (overwritten each run).

Run from anywhere:
    .venv/bin/python scripts/raw_md_to_duckdb_backup.py
"""

import os
import sys
from pathlib import Path

import duckdb

# --------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------

DATABASE = "poke_db"
RAW_SCHEMA = "raw"

# Resolve the archive dir relative to the repo root, so cwd doesn't matter.
REPO_ROOT = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = REPO_ROOT / "include" / "archive" / "raw"


# --------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------

def main() -> int:
    token = os.environ.get("motherduck_token")
    if not token:
        print("ERROR: env var 'motherduck_token' is not set. "
              "Export it first:  export motherduck_token=...", file=sys.stderr)
        return 1

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(f"md:{DATABASE}?motherduck_token={token}")

    try:
        tables = [
            row[0]
            for row in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                ORDER BY table_name
                """,
                [RAW_SCHEMA],
            ).fetchall()
        ]

        if not tables:
            print(f"No tables found in schema '{RAW_SCHEMA}' — nothing to back up.")
            return 0

        print(f"Found {len(tables)} table(s) in '{RAW_SCHEMA}':\n")
        exported, skipped = 0, 0

        for t in tables:
            src_count = con.execute(f'SELECT count(*) FROM {RAW_SCHEMA}."{t}"').fetchone()[0]

            if src_count == 0:
                print(f"  - {t:<20} skipped (empty)")
                skipped += 1
                continue

            out_path = ARCHIVE_DIR / f"{t}.parquet"
            con.execute(
                f'COPY (SELECT * FROM {RAW_SCHEMA}."{t}") '
                f"TO '{out_path}' (FORMAT PARQUET)"
            )
            pq_count = con.execute(
                f"SELECT count(*) FROM read_parquet('{out_path}')"
            ).fetchone()[0]

            status = "OK" if pq_count == src_count else "MISMATCH!"
            print(f"  - {t:<20} src={src_count:<7} parquet={pq_count:<7} {status}")
            exported += 1

        print(f"\nDone. Exported {exported} table(s), skipped {skipped} empty. "
              f"Files in: {ARCHIVE_DIR}")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
