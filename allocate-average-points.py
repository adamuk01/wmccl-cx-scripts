#!/usr/bin/env python3
"""
set_average_points.py
---------------------
Set or clear Average Points (AP) for a rider in a given round.

DEFAULT BEHAVIOUR:
  - Prompts for confirmation after showing rider details.

NON-INTERACTIVE MODE:
  - Use --no-prompt to skip confirmation (safe for batch scripts).

AP IMPLEMENTATION:
  - points = 999
  - is_ap  = 1
  - status = 'AP'

CLEAR AP:
  - points = NULL
  - is_ap  = 0
  - status = 'FIN'

Safe to re-run (UPSERT on rider_id + round).
"""

import argparse
import sqlite3
from pathlib import Path
import sys


AP_POINTS_MARKER = 999


def confirm(prompt: str) -> bool:
    """Ask user to confirm; default No."""
    ans = input(prompt).strip().lower()
    return ans in ("y", "yes")


def main():
    ap = argparse.ArgumentParser(description="Set/Clear Average Points (AP) for a rider in a given round.")
    ap.add_argument("--db", required=True, help="SQLite DB file (e.g. U12.db)")
    ap.add_argument("--round", type=int, required=True, help="Round number (e.g. 11)")
    ap.add_argument("--race-number", type=int, required=True, help="League rider number")
    ap.add_argument("--clear", action="store_true", help="Clear AP instead of setting it")
    ap.add_argument("--dry-run", action="store_true", help="Show what would happen but don't write changes")
    ap.add_argument("--no-prompt", action="store_true",
                    help="Do not prompt for confirmation (batch mode)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ ERROR: DB file not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Schema checks
    for table in ("riders", "results"):
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cur.fetchone():
            print(f"❌ ERROR: DB missing '{table}' table.")
            conn.close()
            sys.exit(1)

    # Find rider
    cur.execute("""
        SELECT id, firstname, surname, club_name
        FROM riders
        WHERE race_number = ?
    """, (args.race_number,))
    rider = cur.fetchone()
    if not rider:
        print(f"❌ ERROR: No rider with race_number={args.race_number} in {db_path.name}")
        conn.close()
        sys.exit(1)

    rider_id, firstname, surname, club = rider

    # Existing result (if any)
    cur.execute("""
        SELECT points, is_ap, status
        FROM results
        WHERE rider_id = ? AND round = ?
    """, (rider_id, args.round))
    existing = cur.fetchone()

    if args.clear:
        new_points = None
        new_is_ap = 0
        new_status = "FIN"
        action = "CLEAR AP"
    else:
        new_points = AP_POINTS_MARKER
        new_is_ap = 1
        new_status = "AP"
        action = "SET AP"

    print(f"\n{action}")
    print(f"  DB        : {db_path.name}")
    print(f"  Round     : {args.round}")
    print(f"  Rider     : #{args.race_number} {firstname} {surname}")
    if club:
        print(f"  Club      : {club}")

    if existing:
        old_points, old_is_ap, old_status = existing
        print(f"  Existing  : points={old_points}, is_ap={old_is_ap}, status={old_status}")
    else:
        print("  Existing  : <no result row>")

    print(f"  New       : points={new_points}, is_ap={new_is_ap}, status={new_status}")

    # Confirmation
    if not args.no_prompt:
        if not confirm("\nProceed? [y/N]: "):
            print("Aborted.")
            conn.close()
            sys.exit(0)

    if args.dry_run:
        print("\nDry run: no changes written.")
        conn.rollback()
        conn.close()
        return

    # Upsert
    cur.execute(
        """
        INSERT INTO results (rider_id, round, cat_position, overall_position, points, is_ap, status)
        VALUES (?, ?, NULL, NULL, ?, ?, ?)
        ON CONFLICT(rider_id, round) DO UPDATE SET
            points = excluded.points,
            is_ap  = excluded.is_ap,
            status = excluded.status
        """,
        (rider_id, args.round, new_points, new_is_ap, new_status)
    )

    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

