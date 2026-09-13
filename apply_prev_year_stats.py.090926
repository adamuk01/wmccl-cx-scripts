#!/usr/bin/env python3
"""
apply_prev_year_stats.py
------------------------
Read a CSV exported from last year's DB (prev_year_stats.csv) and update
this season's SQLite DB(s), filling:

    - race_category_previous_year
    - average_points_last_year

Matching is done on:
    - firstname
    - surname
    - DOB     (string, e.g. '10/31/20')

USUAL WORKFLOW:
    1) Export from OLD DB:
         python3 export_prev_year_stats.py --db old_season.db --out prev_year_stats.csv

    2) Apply to each NEW race DB:
         python3 apply_prev_year_stats.py --db u8.db   --csv prev_year_stats.csv
         python3 apply_prev_year_stats.py --db u10.db  --csv prev_year_stats.csv
         ...

ASSUMPTIONS:
    - NEW DB has a 'riders' table with:
          id, firstname, surname, DOB,
          race_category_previous_year,
          average_points_last_year
    - Names in NEW DB were normalised (first letter caps, rest lower).
    - DOB format is the same between last year and this year (mm/dd/yy).

"""

import sqlite3
import csv
from pathlib import Path
import argparse
from datetime import date, datetime  # in case you want to extend later


# -------- Helpers --------

def normalise_name(name: str) -> str:
    """
    Normalise a person's name:
      - strip spaces
      - capitalise each word
      - handle simple hyphenated names (SMITH-JONES -> Smith-Jones)
    """
    if not name:
        return ""
    name = name.strip()
    words = name.split()
    fixed = []
    for w in words:
        parts = w.split("-")
        parts = [p.capitalize() if p else p for p in parts]
        fixed.append("-".join(parts))
    return " ".join(fixed)


def safe_float(value, default=None):
    if value is None:
        return default
    v = str(value).strip()
    if v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


# -------- Core logic --------

def apply_prev_year_stats(db_path: Path, csv_path: Path, dry_run: bool = False):
    print(f"\nUpdating {db_path} from {csv_path} ...")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    total_rows = 0
    matched = 0
    not_found = 0
    multi_match = 0

    # Quick check: does DB have the columns we want to update?
    # (This will throw if the schema is very different)
    cur.execute("PRAGMA table_info(riders)")
    cols = {row[1] for row in cur.fetchall()}
    required_cols = {
        "firstname",
        "surname",
        "DOB",
        "race_category_previous_year",
        "average_points_last_year",
    }
    missing = required_cols - cols
    if missing:
        print(f"ERROR: DB {db_path} is missing columns: {missing}")
        conn.close()
        return

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows += 1

            raw_first = (row.get("firstname") or "").strip()
            raw_last = (row.get("surname") or "").strip()
            dob = (row.get("DOB") or "").strip()
            prev_cat = (row.get("race_category_current_year") or "").strip()
            prev_avg = safe_float(row.get("average_points"))

            # Normalise names to match how they are stored in the new DB
            first = normalise_name(raw_first)
            last = normalise_name(raw_last)

            if not first or not last or not dob:
                # Skip incomplete lines silently or log if you want
                continue

            # Find matching rider in NEW DB
            cur.execute(
                """
                SELECT id, firstname, surname, DOB
                FROM riders
                WHERE firstname = ? AND surname = ? AND DOB = ?
                """,
                (first, last, dob)
            )
            matches = cur.fetchall()

            if len(matches) == 0:
                not_found += 1
                # You can uncomment this if you want verbose logging:
                print(f"  NOT FOUND: {first} {last}, DOB={dob}")
                continue
            elif len(matches) > 1:
                multi_match += 1
                print(f"  MULTIPLE matches for {first} {last}, DOB={dob} "
                      f"-> rider IDs {[m[0] for m in matches]} (skipping)")
                continue

            # Exactly one match
            (rider_id, db_first, db_last, db_dob) = matches[0]
            matched += 1

            print(f"  MATCH  #{rider_id}: {db_first} {db_last}, DOB={db_dob} "
                  f"-> prev_cat={prev_cat}, prev_avg={prev_avg}")

            if not dry_run:
                cur.execute(
                    """
                    UPDATE riders
                    SET race_category_previous_year = ?,
                        average_points_last_year    = ?
                    WHERE id = ?
                    """,
                    (prev_cat, prev_avg, rider_id)
                )

    if dry_run:
        print(f"\nDry run complete for {db_path}.")
        print(f"  CSV rows processed   : {total_rows}")
        print(f"  Matched & would update: {matched}")
        print(f"  No match              : {not_found}")
        print(f"  Multiple matches      : {multi_match}")
        conn.rollback()
    else:
        conn.commit()
        print(f"\nDone updating {db_path}.")
        print(f"  CSV rows processed   : {total_rows}")
        print(f"  Matched & updated    : {matched}")
        print(f"  No match             : {not_found}")
        print(f"  Multiple matches     : {multi_match}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Apply previous year's categories & averages to new DB."
    )
    parser.add_argument("--db", required=True, help="Path to NEW season SQLite DB (e.g. u8.db)")
    parser.add_argument("--csv", required=True, help="CSV exported from previous season")
    parser.add_argument("--dry-run", action="store_true", help="Show actions but don't write changes.")
    args = parser.parse_args()

    from pathlib import Path
    import sqlite3

    db_path = Path(args.db)
    csv_path = Path(args.csv)

    # ---- SAFETY CHECK 1: DB file exists ----
    if not db_path.exists():
        print(f"\n❌ ERROR: '{db_path}' does not exist.")
        print("   • Check filename/spelling")
        print("   • Are you in the right folder?")
        print("   • Example mistake: Master.db  ->  Masters.db")
        exit(1)

    # ---- SAFETY CHECK 2: CSV file exists ----
    if not csv_path.exists():
        print(f"\n❌ ERROR: CSV file '{csv_path}' not found.")
        exit(1)

    # ---- SAFETY CHECK 3: DB structure sanity ----
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Does 'riders' table exist?
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='riders'")
    if not cur.fetchone():
        print(f"\n❌ ERROR: The database '{db_path.name}' has no 'riders' table.")
        print("   This is NOT a valid league DB.")
        print("   Did you pick the wrong file? e.g. Master.db instead of Masters.db?")
        conn.close()
        exit(1)

    # Check required columns
    required_columns = {
        "firstname", "surname", "DOB",
        "race_category_previous_year",
        "average_points_last_year",
    }

    cur.execute("PRAGMA table_info(riders)")
    cols = {row[1] for row in cur.fetchall()}
    missing = required_columns - cols

    if missing:
        print(f"\n❌ ERROR: The DB '{db_path.name}' is missing required columns:")
        for c in sorted(missing):
            print(f"   - {c}")
        print("\n💡 Tip: This might be an OLD schema or a typo’d filename.")
        print("   Example mistake: Master.db (typo) vs Masters.db (correct)")
        conn.close()
        exit(1)

    conn.close()

    # ---- If we get here, DB is valid. Run update. ----
    apply_prev_year_stats(Path(args.db), Path(args.csv), dry_run=args.dry_run)


if __name__ == "__main__":
    main()

