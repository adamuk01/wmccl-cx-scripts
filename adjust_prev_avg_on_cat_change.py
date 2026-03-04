#!/usr/bin/env python3
"""
adjust_prev_avg_on_cat_change.py
--------------------------------
Adjust previous year's average points for riders whose category has changed
between last season and this season.

USE CASES:
    - U6 -> U8   : subtract a few points so they don't start at the very front
    - Sen -> M40 : add a few points so they don't start at the very back

WHAT IT DOES:
    - Reads each rider in the DB from 'riders' table
    - Looks at:
        race_category_previous_year
        race_category            (current season)
        average_points_last_year
    - If the **base category** has changed (ignoring gender suffix M/F),
      it adjusts average_points_last_year by a user-specified delta.

EXAMPLE:
    # Subtract 5 points for all riders whose category changed in this DB
    python3 adjust_prev_avg_on_cat_change.py --db U8.db --delta -5

    # Add 5 points in Masters DB
    python3 adjust_prev_avg_on_cat_change.py --db Masters.db --delta 5

    # Dry-run (no DB writes), just show what would change
    python3 adjust_prev_avg_on_cat_change.py --db U8.db --delta -5 --dry-run

ASSUMPTIONS:
    - 'riders' table has at least:
        id, race_number, firstname, surname,
        race_category_previous_year,
        race_category,
        average_points_last_year
    - Categories look like:
        U6M, U8F, U10M, U14F, JunM, SenM, M40M, etc.
      i.e. base like 'U8', 'Sen', 'M40' plus optional 'M'/'F'.
"""

import argparse
import sqlite3
from pathlib import Path
import sys


def base_category(cat: str | None) -> str | None:
    """
    Extract the "base" category, ignoring gender suffix.

    Examples:
        'U8M'   -> 'U8'
        'U14F'  -> 'U14'
        'SenM'  -> 'Sen'
        'M40M'  -> 'M40'
        'U6'    -> 'U6'
        None    -> None
        ''      -> None
    """
    if not cat:
        return None
    cat = cat.strip()
    if not cat:
        return None
    last = cat[-1].upper()
    if last in ("M", "F"):
        return cat[:-1]
    return cat


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


def adjust_prev_average(db_path: Path, delta: float, dry_run: bool = False):
    print(f"\nProcessing {db_path} with delta {delta:+} ...")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Quick schema sanity check
    cur.execute("PRAGMA table_info(riders)")
    cols = {row[1] for row in cur.fetchall()}
    required = {
        "id",
        "race_number",
        "firstname",
        "surname",
        "race_category_previous_year",
        "race_category",
        "average_points_last_year",
    }
    missing = required - cols
    if missing:
        print(f"❌ ERROR: DB '{db_path.name}' is missing required columns: {missing}")
        conn.close()
        sys.exit(1)

    cur.execute("""
        SELECT
            id,
            race_number,
            firstname,
            surname,
            race_category_previous_year,
            race_category,
            average_points_last_year
        FROM riders
    """)
    rows = cur.fetchall()

    processed = 0
    changed = 0
    skipped_no_prev_avg = 0
    skipped_no_cat = 0

    for (
        rider_id,
        race_no,
        first,
        last,
        prev_cat,
        curr_cat,
        prev_avg,
    ) in rows:
        processed += 1

        base_prev = base_category(prev_cat)
        base_curr = base_category(curr_cat)

        if not base_prev or not base_curr:
            skipped_no_cat += 1
            continue

        if base_prev == base_curr:
            # Category hasn't changed (ignoring gender) => no adjustment
            continue

        prev_avg_val = safe_float(prev_avg)
        if prev_avg_val is None:
            skipped_no_prev_avg += 1
            continue

        new_avg = prev_avg_val + delta
        # Clamp to valid points range
        new_avg = max(0.0, min(100.0, new_avg))

        print(
            f"  RIDER #{race_no:>4} {first} {last}: "
            f"{prev_cat} -> {curr_cat}, "
            f"avg {prev_avg_val:.2f} -> {new_avg:.2f}"
        )

        if not dry_run:
            cur.execute(
                """
                UPDATE riders
                SET average_points_last_year = ?
                WHERE id = ?
                """,
                (new_avg, rider_id),
            )

        changed += 1

    if dry_run:
        print("\nDry run complete.")
        print(f"  Riders processed        : {processed}")
        print(f"  Adjusted (would change) : {changed}")
        print(f"  Skipped (no cats)       : {skipped_no_cat}")
        print(f"  Skipped (no prev avg)   : {skipped_no_prev_avg}")
        conn.rollback()
    else:
        conn.commit()
        print("\nDone.")
        print(f"  Riders processed      : {processed}")
        print(f"  Adjusted             : {changed}")
        print(f"  Skipped (no cats)    : {skipped_no_cat}")
        print(f"  Skipped (no prev avg): {skipped_no_prev_avg}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Adjust previous year's average points when category has changed."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="SQLite DB file to adjust (e.g. U8.db, Masters.db)",
    )
    parser.add_argument(
        "--delta",
        type=float,
        required=True,
        help="Amount to add to average_points_last_year (use negative to subtract)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change but don't write to the database.",
    )

    args = parser.parse_args()
    db_path = Path(args.db)

    if not db_path.exists():
        print(f"❌ ERROR: DB file '{db_path}' does not exist.")
        sys.exit(1)

    adjust_prev_average(db_path, delta=args.delta, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

