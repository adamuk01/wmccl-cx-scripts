#!/usr/bin/env python3
"""
export_prev_year_stats.py
-------------------------
Export previous year's rider info from one or more OLD SQLite databases into a
single CSV file, so we can later feed this into the new-season DBs.

WHAT IT EXPORTS (one row per rider):
    - source_db                    (which DB the rider came from)
    - firstname
    - surname
    - DOB
    - race_category_current_year
    - average_points

WHY:
    In the new schema we want to populate:
        - race_category_previous_year
        - average_points_last_year
    We do that by matching on (firstname, surname, DOB), using this CSV.

USAGE EXAMPLES:

    # 1) Using a wildcard pattern (recommended)
    python3 export_prev_year_stats.py \
        --db-pattern "U*.db" \
        --out prev_year_stats.csv

    # 2) Explicit DB list
    python3 export_prev_year_stats.py \
        --db u8.db u10.db u12.db youth.db junior.db \
        --out prev_year_stats.csv

NOTES:
    - This version assumes the OLD DBs have a 'riders' table with columns:
        firstname, surname, DOB,
        race_category_current_year,
        average_points
    - Next year, when your "current year" schema changes, you'll only need to
      tweak the SELECT to pull from the new fields (e.g. race_category, average_points).
"""

import sqlite3
import csv
from pathlib import Path
import argparse
import glob
import sys


def export_from_db(db_path: Path, writer, write_header: bool):
    """
    Export riders from a single DB to the given CSV writer.

    Returns number of rows written.
    """
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Adjust these column names if your OLD DB is different
    cur.execute("""
        SELECT
            firstname,
            surname,
            DOB,
            race_category_current_year,
            average_points
        FROM riders
    """)
    rows = cur.fetchall()
    conn.close()

    if write_header:
        writer.writerow([
            "source_db",
            "firstname",
            "surname",
            "DOB",
            "race_category_current_year",
            "average_points",
        ])

    count = 0
    for firstname, surname, dob, cat, avg in rows:
        writer.writerow([
            db_path.name,
            firstname,
            surname,
            dob,
            cat,
            avg,
        ])
        count += 1

    return count


def main():
    parser = argparse.ArgumentParser(
        description="Export previous year's rider stats from one or more DBs into a single CSV."
    )
    parser.add_argument(
        "--db",
        nargs="*",
        help="One or more OLD season SQLite DB files (e.g. u8.db u10.db ...)."
    )
    parser.add_argument(
        "--db-pattern",
        help="Glob pattern for DB files (e.g. 'U*.db' or '*.db'). "
             "If provided, it is combined with any --db arguments."
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output CSV filename (e.g. prev_year_stats.csv)."
    )

    args = parser.parse_args()

    # Collect DB files from pattern and explicit list
    db_paths: set[Path] = set()

    if args.db_pattern:
        # Use glob to expand pattern ourselves; quotes in shell are recommended
        for path_str in glob.glob(args.db_pattern):
            db_paths.add(Path(path_str))

    if args.db:
        for path_str in args.db:
            db_paths.add(Path(path_str))

    if not db_paths:
        print("❌ ERROR: No database files specified or matched.")
        print("   Use either --db-pattern or --db (or both).")
        print("   Examples:")
        print("     python3 export_prev_year_stats.py --db-pattern 'U*.db' --out prev_year_stats.csv")
        print("     python3 export_prev_year_stats.py --db u8.db u10.db --out prev_year_stats.csv")
        sys.exit(1)

    # Sort for deterministic order
    db_list = sorted(db_paths, key=lambda p: p.name)

    print("Databases to export from:")
    for db in db_list:
        print(f"  - {db}")
    print(f"\nOutput CSV: {args.out}")

    out_path = Path(args.out)
    total_riders = 0
    first_db = True

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for db_path in db_list:
            if not db_path.exists():
                print(f"⚠️  WARNING: DB file '{db_path}' does not exist, skipping.")
                continue

            try:
                print(f"\nExporting from {db_path} ...")
                count = export_from_db(db_path, writer, write_header=first_db)
                print(f"  -> {count} riders exported.")
                total_riders += count
                first_db = False  # header only once
            except sqlite3.Error as e:
                print(f"❌ ERROR exporting from {db_path}: {e}")

    print(f"\nDone. Total riders exported across all DBs: {total_riders}")
    print(f"Combined CSV written to: {out_path}")


if __name__ == "__main__":
    main()

