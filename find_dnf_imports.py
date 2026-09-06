#!/usr/bin/env python3
"""
find_dnf_imports.py
-------------------
Scans race CSVs and cross-references against the database to find riders
who had DNF/DNS/DSQ in the Time column but were imported as FIN results.

Reports:
  - Which riders are affected
  - Which round / database
  - What their current DB record looks like
  - An SQL fix statement for each

Usage:
  python3 find_dnf_imports.py --db Seniors.db --csv round1/Seniors-results.csv --round 1
  python3 find_dnf_imports.py --db Seniors.db --csv round1/Seniors-results.csv --round 1 --fix
"""

import argparse
import csv
import sqlite3
from pathlib import Path

NON_FINISHER_STATUSES = {"dnf", "dns", "dsq", "dq", "otl", "did not finish",
                          "did not start", "disqualified", "lapped"}

NONLEAGUE_THRESHOLD = 900


def is_non_finisher(time_str):
    if time_str is None:
        return False
    return str(time_str).strip().lower() in NON_FINISHER_STATUSES


def scan(db_path, csv_path, round_no, fix=False):
    print(f"\n{'='*65}")
    print(f"  DB: {db_path}   Round: {round_no}   CSV: {csv_path}")
    print(f"{'='*65}")

    # Load CSV
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Find DNF rows with a valid race number
    dnf_race_nos = []
    for r in rows:
        raw_time = r.get("Time", "")
        if not is_non_finisher(raw_time):
            continue
        try:
            race_no = int(str(r.get("Race No", "")).strip())
        except ValueError:
            continue
        if race_no >= NONLEAGUE_THRESHOLD:
            continue  # non-league, irrelevant
        dnf_race_nos.append((race_no, r.get("Name", "?").strip()))

    if not dnf_race_nos:
        print("  No DNF/DNS/DSQ league riders found in CSV.")
        return

    print(f"  Found {len(dnf_race_nos)} DNF/DNS/DSQ league rider(s) in CSV:\n")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    fixes = []

    for race_no, csv_name in dnf_race_nos:
        cur = conn.execute(
            "SELECT id, firstname, surname FROM riders WHERE race_number = ?",
            (race_no,)
        )
        rider = cur.fetchone()
        if not rider:
            print(f"  ⚠️  Race No {race_no} ({csv_name}) — not found in riders table, skipping.")
            continue

        rider_id   = rider["id"]
        rider_name = f"{rider['firstname']} {rider['surname']}".strip()

        cur2 = conn.execute(
            """SELECT status, cat_position, overall_position, points
               FROM results WHERE rider_id = ? AND round = ?""",
            (rider_id, round_no)
        )
        result = cur2.fetchone()

        if not result:
            print(f"  ℹ️  Race No {race_no} ({rider_name}) — no result record in DB for round {round_no} (already clean).")
            continue

        if result["status"] != "FIN":
            print(f"  ✅  Race No {race_no} ({rider_name}) — status already '{result['status']}', no fix needed.")
            continue

        print(f"  ❌  Race No {race_no} ({rider_name})")
        print(f"       Current DB record: status={result['status']}, "
              f"cat_pos={result['cat_position']}, "
              f"overall_pos={result['overall_position']}, "
              f"points={result['points']}")

        sql = (f"UPDATE results SET status='DNF', points=NULL, "
               f"cat_position=NULL, overall_position=NULL "
               f"WHERE rider_id={rider_id} AND round={round_no};")
        print(f"       Fix SQL: {sql}")
        fixes.append((rider_id, round_no, rider_name))

    if fixes and fix:
        print(f"\n  Applying {len(fixes)} fix(es)...")
        for rider_id, round_no, name in fixes:
            conn.execute(
                """UPDATE results
                   SET status='DNF', points=NULL, cat_position=NULL, overall_position=NULL
                   WHERE rider_id=? AND round=?""",
                (rider_id, round_no)
            )
        conn.commit()
        print(f"  ✅  Done — {len(fixes)} record(s) updated.")
    elif fixes:
        print(f"\n  Run with --fix to apply the {len(fixes)} correction(s) above.")

    conn.close()


def main():
    ap = argparse.ArgumentParser(
        description="Find and optionally fix DNF riders imported as FIN results."
    )
    ap.add_argument("--db", required=True, help="SQLite DB file")
    ap.add_argument("--csv", required=True, action="append", metavar="CSV",
                    help="Results CSV file (repeat for multiple rounds)")
    ap.add_argument("--round", required=True, type=int, action="append",
                    help="Round number matching each --csv (repeat to match)")
    ap.add_argument("--fix", action="store_true",
                    help="Apply corrections to the database (default: dry run)")
    args = ap.parse_args()

    if len(args.csv) != len(args.round):
        raise SystemExit("❌ Number of --csv and --round arguments must match.")

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")

    for csv_file, round_no in zip(args.csv, args.round):
        csv_path = Path(csv_file)
        if not csv_path.exists():
            print(f"⚠️  CSV not found: {csv_path} — skipping.")
            continue
        scan(db_path, csv_path, round_no, fix=args.fix)

    print("\nDone.")


if __name__ == "__main__":
    main()
