#!/usr/bin/env python3
"""
assign_categories_auto.py
-------------------------
Assign race categories based on YOB (Year of Birth) and gender,
automatically detecting the current CX season from today's date.

Season assumption:
    - Season runs from August (month 8) to July the following year.
    - Example:
        * Jan–Jul 2026  -> season 2025–26 (season_end_year = 2026)
        * Aug–Dec 2026  -> season 2026–27 (season_end_year = 2027)

HOW IT WORKS:
    - Compute season_end_year from today's date (or override via CLI)
    - Compute age = season_end_year - YOB
    - Map age to category:
        <=6        -> U6
        7–8        -> U8
        9–10       -> U10
        11–12      -> U12
        13–14      -> U14
        15–16      -> U16
        17–18      -> Jun
        19–24      -> U23
        25–40      -> Sen
        41–45      -> M40
        46–50      -> M45
        51–55      -> M50
        56–60      -> M55
        61–65      -> M60
        66–70      -> M65
        >=71       -> M70
    - Append 'M' or 'F' based on gender (male/female).

UPDATES:
    - Updates riders.race_category  (your "current year" category field)

USAGE:
    # Auto-detect current season, update one DB
    python3 assign_categories_auto.py u8.db

    # Auto-detect season, multiple DBs, but don't write (test only)
    python3 assign_categories_auto.py --dry-run u8.db u10.db youth.db

    # Force a specific season end year (e.g. 2026 for 2025–26)
    python3 assign_categories_auto.py --season-end-year 2026 u8.db

NOTE:
    - Requires riders table with fields: id, YOB, gender, firstname, surname, race_number, race_category
"""

import sqlite3
import argparse
from datetime import date


# ----------------------------
# Category logic based on age
# ----------------------------

def get_base_category_from_age(age: int | None) -> str | None:
    """Return base category (without gender suffix) given age at season end."""
    if age is None:
        return None

    if age <= 6:
        return "U6"
    elif 7 <= age <= 8:
        return "U8"
    elif 9 <= age <= 10:
        return "U10"
    elif 11 <= age <= 12:
        return "U12"
    elif 13 <= age <= 14:
        return "U14"
    elif 15 <= age <= 16:
        return "U16"
    elif 17 <= age <= 18:
        return "Jun"
    elif 19 <= age <= 24:
        return "U23"
    elif 25 <= age <= 40:
        return "Sen"
    elif 41 <= age <= 45:
        return "M40"
    elif 46 <= age <= 50:
        return "M45"
    elif 51 <= age <= 55:
        return "M50"
    elif 56 <= age <= 60:
        return "M55"
    elif 61 <= age <= 65:
        return "M60"
    elif 66 <= age <= 70:
        return "M65"
    elif age >= 71:
        return "M70"
    return None


def gender_suffix(gender: str | None) -> str:
    """Convert gender value to 'M' or 'F', or '' if unknown."""
    if not gender:
        return ""
    g = gender.strip().lower()
    if g in ("female", "f"):
        return "F"
    if g in ("male", "m"):
        return "M"
    return ""


# ----------------------------
# Season detection
# ----------------------------

def detect_season_end_year(today: date | None = None, season_start_month: int = 8) -> int:
    """
    Detect the current season's end year.

    If today is before August -> in season that ends current_year
    If today is August or later -> in season that ends next_year
    """
    if today is None:
        today = date.today()

    if today.month < season_start_month:
        # Jan–Jul -> still in season that ends this calendar year
        return today.year
    else:
        # Aug–Dec -> in season that ends next calendar year
        return today.year + 1


# ----------------------------
# Main update logic
# ----------------------------

def update_race_categories_for_db(db_path: str, season_end_year: int, dry_run: bool = False):
    print(f"\nProcessing {db_path} for season ending {season_end_year} ...")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT id, YOB, gender, firstname, surname, race_number FROM riders")
    rows = cur.fetchall()

    updated = 0
    skipped = 0

    for rider_id, yob, gender, first, last, race_no in rows:
        if yob is None:
            print(f"  SKIP  #{race_no} {first} {last}: YOB is NULL")
            skipped += 1
            continue

        try:
            age = season_end_year - int(yob)
        except (TypeError, ValueError):
            print(f"  SKIP  #{race_no} {first} {last}: invalid YOB={yob}")
            skipped += 1
            continue

        base_cat = get_base_category_from_age(age)
        if not base_cat:
            print(f"  SKIP  #{race_no} {first} {last}: age={age} not in any category")
            skipped += 1
            continue

        cat = base_cat + gender_suffix(gender)

        print(f"  SET   #{race_no} {first} {last}: {cat} (YOB={yob}, age={age}, gender={gender})")

        if not dry_run:
            cur.execute(
                "UPDATE riders SET race_category = ? WHERE id = ?",
                (cat, rider_id)
            )

        updated += 1

    if dry_run:
        print(f"\nDry run complete for {db_path}. {updated} would be updated, {skipped} skipped.")
        conn.rollback()
    else:
        conn.commit()
        print(f"\nDone {db_path}. Updated: {updated}, Skipped: {skipped}")

    conn.close()


# ----------------------------
# CLI
# ----------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Assign race categories based on YOB and gender, with season auto-detection."
    )
    parser.add_argument(
        "dbs",
        nargs="+",
        help="One or more SQLite DB files (e.g. u8.db u10.db youth.db)."
    )
    parser.add_argument(
        "--season-end-year",
        type=int,
        help="Override auto-detected season end year (e.g. 2026 for 2025–26)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change, but do not write to the database."
    )

    args = parser.parse_args()

    if args.season_end_year:
        season_end = args.season_end_year
        print(f"Using user-specified season end year: {season_end}")
    else:
        season_end = detect_season_end_year()
        # For info: derive start year
        season_start = season_end - 1
        print(f"Auto-detected season: {season_start}-{str(season_end)[-2:]} "
              f"(season_end_year={season_end})")

    for db in args.dbs:
        update_race_categories_for_db(db, season_end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

