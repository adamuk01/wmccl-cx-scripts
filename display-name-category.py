#!/usr/bin/env python3
"""
list_rider_categories.py
-------------------------
Query a league SQLite database and print each rider's race number,
first name, last name, and current race category.

USAGE:
    python3 list_rider_categories.py --db U8.db

    # Sort by name instead of race number
    python3 list_rider_categories.py --db Masters.db --sort name

    # Write to a CSV file instead of printing to screen
    python3 list_rider_categories.py --db Youth.db --out youth_categories.csv
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path


def fetch_riders(db_path: Path, sort_by: str):
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Sanity check schema
    cur.execute("PRAGMA table_info(riders)")
    cols = {row[1] for row in cur.fetchall()}
    required = {"race_number", "firstname", "surname", "race_category"}
    missing = required - cols
    if missing:
        print(f"❌ ERROR: '{db_path.name}' riders table is missing columns: {missing}")
        conn.close()
        sys.exit(1)

    order_col = {
        "number": "race_number",
        "name": "surname, firstname",
    }.get(sort_by, "race_number")

    cur.execute(f"""
        SELECT race_number, firstname, surname, race_category
        FROM riders
        ORDER BY {order_col}
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def main():
    ap = argparse.ArgumentParser(
        description="List race number, name, and current category for riders in a DB."
    )
    ap.add_argument("--db", required=True, help="SQLite DB file (e.g. U8.db, Masters.db)")
    ap.add_argument("--sort", choices=["number", "name"], default="number",
                    help="Sort by race number (default) or by surname/firstname")
    ap.add_argument("--out", help="Optional CSV output file (default: print to screen)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ ERROR: DB file not found: {db_path}")
        sys.exit(1)

    rows = fetch_riders(db_path, args.sort)

    if args.out:
        out_path = Path(args.out)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["race_number", "firstname", "surname", "race_category"])
            for r in rows:
                writer.writerow(r)
        print(f"✅ Wrote {len(rows)} riders to {out_path}")
    else:
        print(f"\n{'No.':<6}{'First':<15}{'Last':<20}{'Category':<10}")
        print("-" * 51)
        for race_no, first, last, cat in rows:
            print(f"{str(race_no):<6}{(first or ''):<15}{(last or ''):<20}{(cat or ''):<10}")
        print(f"\nTotal riders: {len(rows)}")


if __name__ == "__main__":
    main()
