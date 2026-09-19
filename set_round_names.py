#!/usr/bin/env python3
"""
set_round_names.py
-------------------
Populate/update the `rounds` table (round_number, name, venue, date) in one
or more league DBs from a single shared season CSV, so all 7 category DBs
stay in sync on round names/venues/dates rather than being edited by hand
in each one separately.

Requires the `rounds` table to already exist in the target DB(s) —
run add_rounds_table.py first.

CSV format (header required), e.g. rounds-2026.csv:

    round,name,venue,date
    1,Round 1,Sutton Park,2026-10-04
    2,Round 2,Cannon Hill Park,2026-10-18
    3,Round 3,Waseley Hills,2026-11-01

  - "round" and "name" are required columns; "venue" and "date" are optional
    (leave the cell blank if unknown).
  - "round" must be an integer.
  - "date" is stored as free text (ISO format recommended) — not validated.
  - Re-running with an updated CSV upserts: existing rows for the same
    round_number are overwritten, so fixing a venue name or adding a
    late-added round is just a re-run. Rounds already in the DB but not
    present in the CSV are left alone, unless --replace is given.

Usage:
  # Apply to every league DB in the current directory
  python3 set_round_names.py --csv rounds-2026.csv --all

  # Apply to specific DBs only
  python3 set_round_names.py --csv rounds-2026.csv --db U8.db --db Women.db

  # Wipe each DB's rounds table first, then reload from the CSV
  # (e.g. a round was cancelled/renumbered and stale rows need clearing)
  python3 set_round_names.py --csv rounds-2026.csv --all --replace
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import List, Tuple

DEFAULT_DBS = [
    "U8.db",
    "U10.db",
    "U12.db",
    "Youth.db",
    "Women.db",
    "Masters.db",
    "Seniors.db",
]

RoundRow = Tuple[int, str, str, str]


def load_rounds_csv(csv_path: Path) -> List[RoundRow]:
    if not csv_path.exists():
        raise SystemExit(f"❌ CSV not found: {csv_path}")

    rows: List[RoundRow] = []
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = {h.strip() for h in (reader.fieldnames or [])}
        required = {"round", "name"}
        missing = required - fieldnames
        if missing:
            raise SystemExit(f"❌ CSV missing required column(s): {sorted(missing)}")

        for i, row in enumerate(reader, start=2):  # start=2: header is line 1
            raw_round = (row.get("round") or "").strip()
            if raw_round == "":
                continue  # skip blank lines
            try:
                round_number = int(raw_round)
            except ValueError:
                raise SystemExit(f"❌ CSV line {i}: 'round' must be an integer, got {raw_round!r}")

            name = (row.get("name") or "").strip()
            venue = (row.get("venue") or "").strip()
            date = (row.get("date") or "").strip()
            rows.append((round_number, name, venue, date))

    if not rows:
        raise SystemExit("❌ CSV had no usable rows")

    # catch duplicate round numbers early, rather than letting the last one silently win
    seen = set()
    dupes = set()
    for round_number, *_ in rows:
        if round_number in seen:
            dupes.add(round_number)
        seen.add(round_number)
    if dupes:
        raise SystemExit(f"❌ CSV has duplicate round number(s): {sorted(dupes)}")

    return rows


def ensure_rounds_table(conn: sqlite3.Connection, db_name: str):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rounds'")
    if not cur.fetchone():
        raise SystemExit(f"❌ {db_name}: no 'rounds' table found. Run add_rounds_table.py first.")


def apply_rounds(db_path: Path, rounds: List[RoundRow], replace: bool) -> bool:
    if not db_path.exists():
        print(f"  ⚠  Skipped {db_path.name} (not found)")
        return False

    conn = sqlite3.connect(str(db_path))
    try:
        ensure_rounds_table(conn, db_path.name)
        cur = conn.cursor()

        if replace:
            cur.execute("DELETE FROM rounds")

        cur.executemany(
            """
            INSERT INTO rounds (round_number, name, venue, date)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(round_number) DO UPDATE SET
                name  = excluded.name,
                venue = excluded.venue,
                date  = excluded.date
            """,
            rounds,
        )
        conn.commit()
    finally:
        conn.close()

    print(f"  ✓ {db_path.name}: {len(rounds)} round(s) set")
    return True


def main():
    ap = argparse.ArgumentParser(
        description="Set round names/venues/dates in one or more league DBs from a shared CSV."
    )
    ap.add_argument("--csv", required=True, help="Season rounds CSV (round,name,venue,date)")
    ap.add_argument("--db", action="append", default=[], help="DB file to update (repeatable)")
    ap.add_argument(
        "--all",
        action="store_true",
        help=f"Update the default set of DBs: {', '.join(DEFAULT_DBS)}",
    )
    ap.add_argument("--dir", default=".", help="Directory containing the DBs (default: current directory)")
    ap.add_argument(
        "--replace",
        action="store_true",
        help="Clear each DB's rounds table before loading (default: upsert, leaving other rows alone)",
    )
    args = ap.parse_args()

    dbs: List[str] = list(args.db)
    if args.all:
        dbs.extend(DEFAULT_DBS)
    if not dbs:
        raise SystemExit("❌ Specify --db <file> (repeatable) or --all")

    rounds = load_rounds_csv(Path(args.csv))

    base = Path(args.dir)
    print(f"Loaded {len(rounds)} round(s) from {args.csv}")
    print(f"Applying to {len(dbs)} DB(s) in {base.resolve()}{' (REPLACE mode)' if args.replace else ''}\n")

    updated = 0
    for name in dbs:
        if apply_rounds(base / name, rounds, args.replace):
            updated += 1

    print(f"\nDone. {updated}/{len(dbs)} DB(s) updated.")


if __name__ == "__main__":
    main()
