#!/usr/bin/env python3
"""
count_riders.py
----------------
Quick utility to count how many riders are stored in each SQLite race database.

WHY:
    After importing riders from CSV (per race/category), this script helps you
    confirm that the number of rows in the 'riders' table matches what you expect
    from your filtered CSV. It's a sanity check that import/filters were correct.

WHAT IT DOES:
    - Connects to each .db file you pass in
    - Counts rows in the 'riders' table using:  SELECT COUNT(*) FROM riders;
    - Prints a simple summary per database

EXAMPLE USAGE:
    # Count one database
    python3 count_riders.py u8.db

    # Count multiple race databases
    python3 count_riders.py u8.db u10.db u12.db youth.db junior.db

EXPECTED OUTPUT:
    Rider counts:
    -----------------------------------
    u8.db                 32 riders
    u10.db                41 riders
    u12.db                38 riders
    youth.db              55 riders
    junior.db             22 riders
    -----------------------------------
    Done.

NOTES:
    - Assumes each DB already has a 'riders' table.
    - Only counts riders; does not check results, rounds, or AP flags.

"""

import sqlite3
from pathlib import Path
import argparse


def count_riders(db_path: Path):
    """Connect to a database and count riders in the 'riders' table."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM riders;")
        (count,) = cur.fetchone()
        print(f"{db_path.name:<20}  {count:>5} riders")
    except sqlite3.Error as e:
        print(f"{db_path.name:<20}  ERROR: {e}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Show rider counts for one or more league DBs."
    )
    parser.add_argument("dbs", nargs="+", help="Paths to SQLite DB files")
    args = parser.parse_args()

    print("\nRider counts:")
    print("-" * 35)
    for db in args.dbs:
        count_riders(Path(db))
    print("-" * 35)
    print("Done.\n")


if __name__ == "__main__":
    main()

