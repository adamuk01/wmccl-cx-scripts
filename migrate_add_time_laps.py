#!/usr/bin/env python3
"""
migrate_add_time_laps.py
-------------------------
One-off migration: adds two new columns to the 'results' table of one or
more EXISTING SQLite race DBs, so import_race_results.py can start
recording finish time and laps completed per rider per round.

    laps_completed        INTEGER   -- from D3 'Laps' column
    finish_time_seconds   REAL      -- from D3 'Time' column, parsed to
                                        seconds (NULL for DNF rows)

New databases created with create_db.py from now on already include these
columns, so this script only needs to be run once per EXISTING DB (this
season's U8.db, U10.db, U12.db, Youth.db, Women.db, Masters.db, Seniors.db).

SAFE TO RE-RUN:
    Each DB's schema is checked first -- if a column already exists, it is
    skipped rather than re-added (SQLite errors on adding a duplicate
    column, so this check matters).

    Existing rows get NULL for both new columns (no data loss, no
    guessing); import_race_results.py fills them in going forward as new
    rounds are imported. Historical rounds already imported before this
    migration will simply have no time/laps data unless re-imported.

USAGE:
    python3 migrate_add_time_laps.py --db U8.db U10.db U12.db Youth.db \
        Women.db Masters.db Seniors.db

    # Preview only, no changes written:
    python3 migrate_add_time_laps.py --db U8.db --dry-run
"""

import argparse
import sqlite3
from pathlib import Path


NEW_COLUMNS = {
    "laps_completed": "INTEGER",
    "finish_time_seconds": "REAL",
}


def migrate_db(db_path: Path, dry_run: bool = False):
    print(f"\n{db_path.name}")

    if not db_path.exists():
        print(f"  ❌ ERROR: DB not found, skipping.")
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results'")
    if not cur.fetchone():
        print(f"  ❌ ERROR: no 'results' table found, skipping.")
        conn.close()
        return

    cur.execute("PRAGMA table_info(results)")
    existing_cols = {row[1] for row in cur.fetchall()}

    for col_name, col_type in NEW_COLUMNS.items():
        if col_name in existing_cols:
            print(f"  = '{col_name}' already present, no change.")
            continue

        print(f"  + adding '{col_name} {col_type}'"
              + (" (dry-run, not written)" if dry_run else ""))

        if not dry_run:
            cur.execute(f"ALTER TABLE results ADD COLUMN {col_name} {col_type}")

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    conn.close()


def main():
    ap = argparse.ArgumentParser(
        description="Add laps_completed / finish_time_seconds columns to existing race DBs."
    )
    ap.add_argument("--db", nargs="+", required=True,
                    help="One or more SQLite race DB files (e.g. U8.db U10.db ...)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would change but don't write to the database.")
    args = ap.parse_args()

    for db_file in args.db:
        migrate_db(Path(db_file), dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
