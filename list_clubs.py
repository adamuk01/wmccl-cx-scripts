#!/usr/bin/env python3
"""
list_clubs.py
-------------
Scan one or more league SQLite databases and list all club names with the
number of riders in each club, aggregated across ALL the databases.

WHY:
    Sometimes riders don't pick the standard club name (e.g. 'Solihull CC')
    and type something like 'Solihull Cycling Club' instead. This script lets
    you see ALL club names currently in use, plus how many riders use each,
    so you can tidy/fix them in DB Browser.

WHAT IT DOES:
    - For each DB:
        * opens the 'riders' table
        * reads the 'club_name' column
    - Aggregates counts across all DBs
    - Prints a sorted list:
          <club_name> : <total_riders>
      sorted alphabetically by club_name.

USAGE EXAMPLES:
    # Using an explicit DB list
    python3 list_clubs.py --db U8.db U10.db U12.db Youth.db Junior.db Masters.db

    # Using a wildcard pattern (quotes recommended)
    python3 list_clubs.py --db-pattern "*.db"

    # Combine both if you like
    python3 list_clubs.py --db-pattern "U*.db" --db Masters.db

NOTES:
    - Assumes each DB has a 'riders' table with a 'club_name' column.
    - Ignores NULL/blank club names.
"""

import argparse
import glob
import sqlite3
from pathlib import Path
from collections import Counter
import sys


def collect_clubs_from_db(db_path: Path, counter: Counter):
    """Collect club_name values from a single DB into the given Counter."""
    if not db_path.exists():
        print(f"⚠️  WARNING: DB file '{db_path}' does not exist, skipping.")
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    try:
        # Check table exists
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='riders'
        """)
        if not cur.fetchone():
            print(f"⚠️  WARNING: '{db_path.name}' has no 'riders' table, skipping.")
            return

        # Check club_name column exists
        cur.execute("PRAGMA table_info(riders)")
        cols = {row[1] for row in cur.fetchall()}
        if "club_name" not in cols:
            print(f"⚠️  WARNING: '{db_path.name}' has no 'club_name' column, skipping.")
            return

        cur.execute("SELECT club_name FROM riders")
        rows = cur.fetchall()

        for (club,) in rows:
            if club is None:
                continue
            name = club.strip()
            if not name:
                continue
            counter[name] += 1

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="List all club names and rider counts across one or more DBs."
    )
    parser.add_argument(
        "--db",
        nargs="*",
        help="One or more SQLite DB files (e.g. U8.db U10.db ...)."
    )
    parser.add_argument(
        "--db-pattern",
        help="Glob pattern for DB files (e.g. '*.db' or 'U*.db')."
    )
    args = parser.parse_args()

    db_paths: set[Path] = set()

    # From pattern
    if args.db_pattern:
        for p in glob.glob(args.db_pattern):
            db_paths.add(Path(p))

    # From explicit list
    if args.db:
        for p in args.db:
            db_paths.add(Path(p))

    if not db_paths:
        print("❌ ERROR: No database files specified or matched.")
        print("   Use --db-pattern or --db (or both).")
        sys.exit(1)

    db_list = sorted(db_paths, key=lambda p: p.name)

    print("Databases to scan:")
    for db in db_list:
        print(f"  - {db}")
    print()

    club_counter = Counter()

    for db in db_list:
        collect_clubs_from_db(db, club_counter)

    if not club_counter:
        print("No clubs found (or all club_name fields are empty).")
        return

    print("Club name usage across all databases:\n")
    for club in sorted(club_counter.keys(), key=str.lower):
        print(f"{club}: {club_counter[club]}")

    print("\nDone.")


if __name__ == "__main__":
    main()

