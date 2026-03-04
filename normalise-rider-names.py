#!/usr/bin/env python3
import sqlite3
from pathlib import Path
import argparse

def normalise_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    words = name.split()
    fixed_words = []
    for word in words:
        parts = word.split("-")
        parts = [p.capitalize() if p else p for p in parts]
        fixed_words.append("-".join(parts))
    return " ".join(fixed_words)

def normalise_names_in_db(db_path: Path, dry_run=False):
    print(f"\nNormalising names in {db_path} ...")
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("SELECT id, firstname, surname, race_number FROM riders")
    rows = cur.fetchall()

    updates = 0
    for rider_id, first, last, race_no in rows:
        new_first = normalise_name(first)
        new_last  = normalise_name(last)

        if new_first == first and new_last == last:
            continue  # no change

        print(f"  Rider {race_no}: '{first} {last}' -> '{new_first} {new_last}'")

        if not dry_run:
            cur.execute(
                "UPDATE riders SET firstname = ?, surname = ? WHERE id = ?",
                (new_first, new_last, rider_id)
            )
        updates += 1

    if dry_run:
        print(f"Dry run: {updates} name(s) WOULD be updated.")
        conn.rollback()
    else:
        conn.commit()
        print(f"Updated {updates} rider name(s).")

    conn.close()

def main():
    parser = argparse.ArgumentParser(
        description="Normalise rider names (capitalise nicely) in one or more DBs."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show changes but do not write them.")
    parser.add_argument("dbs", nargs="+", help="SQLite DB files (e.g. u8.db u10.db ...)")

    args = parser.parse_args()

    for db in args.dbs:
        normalise_names_in_db(Path(db), dry_run=args.dry_run)

if __name__ == "__main__":
    main()

