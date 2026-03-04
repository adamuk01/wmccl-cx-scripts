#!/usr/bin/env python3
"""
import_riders_from_reg_csv.py

Validate and import riders from a *full* registration CSV into ONE race DB.

Typical workflow:
  1) Clean spaces with your fixer script -> corrected_X.csv
  2) Validate:
       python import_riders_from_reg_csv.py --csv corrected_X.csv --validate-only
  3) If OK, import into the DB for a specific race:
       python import_riders_from_reg_csv.py --db u8.db --csv corrected_X.csv \
           --category-filter "Under 8"

Notes:
  - CSV is the full RiderHQ export (all categories).
  - --category-filter uses 'Membership type (G)' to pick which riders go into
    this DB (e.g. 'Under 8', 'Under 10', 'Youth', etc.).
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_int(value, default=None):
    if value is None:
        return default
    v = str(value).strip()
    if v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


from datetime import datetime, date

def parse_yob_from_dob(dob_str):
    """
    dob_str is in format 'mm/dd/yy' (e.g. '10/31/20').

    Logic:
      - Use the current year to decide century.
      - Let current_year = 2026 -> last_two = 26
      - If yy > last_two  -> 1900 + yy  (e.g. 48 -> 1948, 59 -> 1959)
      - If yy <= last_two -> 2000 + yy  (e.g. 20 -> 2020, 08 -> 2008)

    This assumes:
      - no riders born before 1900 (safe)
      - no riders born in the future (we'll sanity check that)
    """
    dob_str = (dob_str or "").strip()
    if not dob_str:
        return None

    try:
        month_str, day_str, year_str = dob_str.split("/")
        mm = int(month_str)
        dd = int(day_str)
        yy = int(year_str)

        today = date.today()
        last_two = today.year % 100  # e.g. 26 for 2026

        if yy > last_two:
            year = 1900 + yy
        else:
            year = 2000 + yy

        # sanity check date is real
        dt = datetime(year, mm, dd)

        # extra sanity: no future YOB
        if year > today.year:
            return None

        return year

    except Exception:
        return None



def normalise_name(name: str) -> str:
    """
    Normalise a person's name:
      - strip leading/trailing spaces
      - make each word capitalised (first letter upper, rest lower)
      - handle simple hyphenated parts (SMITH-JONES -> Smith-Jones)

    Note: this is intentionally simple; it won't handle exotic cases like 'McFadden'
    perfectly, but it's good enough for most CX league data.
    """
    if not name:
        return ""

    name = name.strip()
    words = name.split()

    fixed_words = []
    for word in words:
        # Handle hyphenated bits: 'SMITH-JONES' -> 'Smith-Jones'
        parts = word.split("-")
        parts = [p.capitalize() if p else p for p in parts]
        fixed_words.append("-".join(parts))

    return " ".join(fixed_words)



# ---------------------------------------------------------------------------
# Validation (only checks fields we actually care about)
# ---------------------------------------------------------------------------

def validate_csv(csv_file):
    """
    Validate the registration CSV.

    We care about:
      - Membership number (league race number, must be unique)
      - First name
      - Last name
      - Club/Team (or Another Club...)
      - Date of birth (non-blank and correct mm/dd/yy format)
      - Are you a member of British Cycling?
      - Membership ID (if they ARE a BC member)

    Also checks:
      - duplicate rows (identical CSV line)
      - duplicate person (First name + Last name + DOB)
      - duplicate Membership number
    """
    issues = []
    seen_rows = set()
    seen_people = set()         # (first_name, last_name, dob)
    seen_membership_numbers = {}  # membership_number -> (line_num, first_name, last_name, dob)

    with open(csv_file, 'r', newline='') as file:
        reader = csv.DictReader(file)

        for line_num, row in enumerate(reader, start=2):
            row_tuple = tuple(row.items())
            if row_tuple in seen_rows:
                issues.append(f"Line {line_num}: Duplicate row found.")
            else:
                seen_rows.add(row_tuple)

            membership_number = (row.get("Membership number") or "").strip()
            first_name = (row.get("First name") or "").strip()
            last_name = (row.get("Last name") or "").strip()
            dob = (row.get("Date of birth") or "").strip()
            club_dropdown = (row.get("Club/Team") or "").strip()
            alt_free = (row.get("Another Club/Team (not listed)") or "").strip()
            club = alt_free or club_dropdown

            bc_member = (row.get("Are you a member of British Cycling?") or "").strip()
            bc_id = (row.get("Membership ID") or "").strip()

            # Required fields
            required_values = {
                "Membership number": membership_number,
                "First name": first_name,
                "Last name": last_name,
                "Club/Team/Another Club": club,
                "Date of birth": dob,
            }
            for field_name, value in required_values.items():
                if not value:
                    issues.append(f"Line {line_num}: '{field_name}' is blank.")

            # DOB format
            if dob and parse_yob_from_dob(dob) is None:
                issues.append(
                    f"Line {line_num}: 'Date of birth' has invalid format '{dob}' "
                    f"(expected mm/dd/yy)."
                )

            # BC membership logic
            if bc_member.lower().startswith("y") and not bc_id:
                issues.append(
                    f"Line {line_num}: 'Are you a member of British Cycling?' is "
                    f"'{bc_member}' but 'Membership ID' is blank."
                )

            # Duplicate person (same name + DOB)
            person = (first_name, last_name, dob)
            if person in seen_people:
                issues.append(
                    f"Line {line_num}: Duplicate person: "
                    f"'{first_name} {last_name}', DOB {dob}."
                )
            else:
                seen_people.add(person)

            # Duplicate Membership number
            if membership_number:
                if membership_number in seen_membership_numbers:
                    prev_line, prev_first, prev_last, prev_dob = seen_membership_numbers[membership_number]
                    issues.append(
                        f"Line {line_num}: Duplicate Membership number '{membership_number}'. "
                        f"Previously seen on line {prev_line} "
                        f"for '{prev_first} {prev_last}', DOB {prev_dob}."
                    )
                else:
                    seen_membership_numbers[membership_number] = (
                        line_num, first_name, last_name, dob
                    )

    return issues



# ---------------------------------------------------------------------------
# Import logic
# ---------------------------------------------------------------------------

def import_riders(db_path, csv_path, category_filter=None):
    """
    Import riders from CSV into the 'riders' table of db_path.

    - If category_filter is provided, only import rows where
      'Membership type (G)' == category_filter.
    - Uses:
        race_number           <- Membership number
        BC_number             <- Membership ID (if BC member == Yes)
        firstname, surname
        gender
        club_name             <- Club/Team or Another Club...(not listed)
        DOB                   <- Date of birth
        YOB                   <- derived from DOB
        IBX                   <- One Bike? ('Y'/'N'/NULL)
    - Leaves race_category, race_category_previous_year, average_points_last_year
      as NULL for now.
    """
    print(f"\nImporting riders from {csv_path} into {db_path}")
    if category_filter:
        print(f"  Only importing rows where 'Membership type (G)' == '{category_filter}'")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    total_rows = 0
    imported_count = 0
    skipped_no_number = 0

    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                total_rows += 1

                # Optional category filter (by registration category)
                if category_filter:
                    reg_cat = (row.get("Membership type (G)") or "").strip()
                    if reg_cat != category_filter:
                        continue  # skip this rider entirely

                membership_number = (row.get("Membership number") or "").strip()
                race_number = safe_int(membership_number)
                if race_number is None:
                    skipped_no_number += 1
                    continue

                raw_firstname = (row.get("First name") or "").strip()
                raw_surname   = (row.get("Last name") or "").strip()

                firstname = normalise_name(raw_firstname)
                surname  = normalise_name(raw_surname)
                gender = (row.get("gender") or "").strip()

                # Club/Team vs Another Club logic
                club_dropdown = (row.get("Club/Team") or "").strip()
                alt_free = (row.get("Another Club/Team (not listed)") or "").strip()
                if alt_free:
                    club_name = alt_free
                else:
                    club_name = club_dropdown

                dob = (row.get("Date of birth") or "").strip()
                yob = parse_yob_from_dob(dob)  # may be None if DOB was weird (but validator should catch it)

                # One Bike? -> IBX field (Y/N/None)
                one_bike_raw = (row.get("One Bike?") or "").strip().lower()
                if one_bike_raw.startswith("y"):
                    ibx_val = "Y"
                elif one_bike_raw.startswith("n"):
                    ibx_val = "N"
                else:
                    ibx_val = None

                # BC membership -> BC_number
                bc_member = (row.get("Are you a member of British Cycling?") or "").strip()
                bc_number = None
                if bc_member.lower().startswith("y"):
                    bc_number = safe_int(row.get("Membership ID"))

                # Check if rider already exists by race_number
                cur.execute(
                    "SELECT id FROM riders WHERE race_number = ?",
                    (race_number,)
                )
                existing = cur.fetchone()

                if existing is None:
                    # Insert new rider
                    cur.execute(
                        """
                        INSERT INTO riders (
                            race_number, BC_number,
                            firstname, surname, gender,
                            club_name,
                            race_category,
                            race_category_previous_year,
                            average_points_last_year,
                            DOB, YOB, IBX
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            race_number,
                            bc_number,
                            firstname,
                            surname,
                            gender,
                            club_name,
                            None,   # race_category (current year) - set later
                            None,   # race_category_previous_year
                            None,   # average_points_last_year
                            dob,
                            yob,
                            ibx_val
                        )
                    )
                else:
                    rider_id = existing[0]
                    # Update basic details (keep grid-related fields as they are)
                    cur.execute(
                        """
                        UPDATE riders
                        SET BC_number = ?,
                            firstname = ?,
                            surname = ?,
                            gender = ?,
                            club_name = ?,
                            DOB = ?,
                            YOB = ?,
                            IBX = ?
                        WHERE id = ?
                        """,
                        (
                            bc_number,
                            firstname,
                            surname,
                            gender,
                            club_name,
                            dob,
                            yob,
                            ibx_val,
                            rider_id
                        )
                    )

                imported_count += 1

        conn.commit()

        print("\nImport summary:")
        print(f"  Total CSV rows processed: {total_rows}")
        if category_filter:
            print(f"  (Rows not matching '{category_filter}' were ignored silently)")
        print(f"  Riders inserted/updated  : {imported_count}")
        print(f"  Rows skipped (no number) : {skipped_no_number}")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate and/or import riders from a registration CSV."
    )
    parser.add_argument(
        "--db",
        help="SQLite DB path (e.g. u8.db). "
             "Required if you want to import, ignored for --validate-only."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to registration CSV (full RiderHQ export)."
    )
    parser.add_argument(
        "--category-filter",
        help="Only import rows where 'Membership type (G)' equals this value "
             "(e.g. 'Under 8')."
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only run validation on the CSV and exit (no DB import)."
    )

    args = parser.parse_args()
    csv_path = Path(args.csv)

    # 1) Validation first
    print(f"Validating CSV: {csv_path}")
    issues = validate_csv(csv_path)

    if issues:
        print("\nValidation failed. Issues found:")
        for issue in issues:
            print(issue)

        if args.validate_only:
            # Just report issues, don't import
            return
        else:
            print("\nNot importing because validation failed. "
                  "Fix the CSV or run with --validate-only first.")
            return
    else:
        print("CSV file is valid. No issues found.")

    # 2) Import (if requested)
    if args.validate_only:
        return

    if not args.db:
        print("\nERROR: --db is required to import into a database.")
        return

    db_path = Path(args.db)
    import_riders(db_path, csv_path, category_filter=args.category_filter)


if __name__ == "__main__":
    main()

