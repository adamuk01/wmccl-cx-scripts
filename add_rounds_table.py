#!/usr/bin/env python3
"""
add_rounds_table.py
--------------------
One-off / idempotent migration: adds a `rounds` table to a league DB so each
round in the season can carry a name, venue and date (needed for the new
HTML league tables — the old CSV export only ever used bare round numbers).

    CREATE TABLE IF NOT EXISTS rounds (
        round_number INTEGER PRIMARY KEY,
        name TEXT,
        venue TEXT,
        date TEXT
    );

Safe to run multiple times, and against DBs that already have the table —
it will just report that nothing changed. Does not touch riders/results.

Once the table exists, use set_round_names.py to populate it each season.

Usage:
  # Single DB
  python3 add_rounds_table.py --db U8.db

  # All seven league DBs in the current directory (default filenames)
  python3 add_rounds_table.py --all

  # Explicit subset, or DBs living elsewhere
  python3 add_rounds_table.py --db U8.db --db Women.db --dir /nas/CX/2026
"""

import argparse
import sqlite3
from pathlib import Path
from typing import List

DEFAULT_DBS = [
    "U8.db",
    "U10.db",
    "U12.db",
    "Youth.db",
    "Women.db",
    "Masters.db",
    "Seniors.db",
]

CREATE_ROUNDS_SQL = """
CREATE TABLE IF NOT EXISTS rounds (
    round_number INTEGER PRIMARY KEY,
    name TEXT,
    venue TEXT,
    date TEXT
);
"""


def add_rounds_table(db_path: Path) -> bool:
    """Adds the rounds table if missing. Returns True if it was newly created."""
    if not db_path.exists():
        print(f"  ⚠  Skipped {db_path.name} (not found)")
        return False

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='rounds'"
        )
        existed = cur.fetchone() is not None

        conn.executescript(CREATE_ROUNDS_SQL)
        conn.commit()
    finally:
        conn.close()

    if existed:
        print(f"  = {db_path.name}: rounds table already present")
    else:
        print(f"  + {db_path.name}: rounds table created")
    return not existed


def main():
    ap = argparse.ArgumentParser(
        description="Add the 'rounds' table to one or more league DBs (idempotent)."
    )
    ap.add_argument("--db", action="append", default=[], help="DB file to migrate (repeatable)")
    ap.add_argument(
        "--all",
        action="store_true",
        help=f"Migrate the default set of DBs: {', '.join(DEFAULT_DBS)}",
    )
    ap.add_argument("--dir", default=".", help="Directory containing the DBs (default: current directory)")
    args = ap.parse_args()

    dbs: List[str] = list(args.db)
    if args.all:
        dbs.extend(DEFAULT_DBS)
    if not dbs:
        raise SystemExit("❌ Specify --db <file> (repeatable) or --all")

    base = Path(args.dir)
    print(f"Migrating {len(dbs)} DB(s) in {base.resolve()}\n")

    created = 0
    for name in dbs:
        if add_rounds_table(base / name):
            created += 1

    print(f"\nDone. {created} DB(s) newly migrated.")


if __name__ == "__main__":
    main()
