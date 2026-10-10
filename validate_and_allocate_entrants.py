#!/usr/bin/env python3
"""
validate_and_allocate_entrants_multi_db.py
------------------------------------------
Validate and clean an entrants CSV against MULTIPLE race databases,
and allocate race numbers to NON-league riders.

Key rules:
- "Has membership" == TRUE  → rider IS a league rider → MUST have Membership number
- Missing Membership number for league rider → FAIL, but attempt to suggest correct number
  using First name + Last name + Date of birth against DBs.
- Sex on the entry form must agree with the gender held in the DB for that number.
- Entry type (RiderHQ category) must match the category implied by the rider's
  date of birth and sex, using the same age bands as assign-category.py.

Usage:
  python3 validate_and_allocate_entrants_multi_db.py \
    --db U8.db --db U10.db --db U12.db --db Youth.db --db Seniors.db --db Masters.db \
    --csv entrants.csv \
    --out corrected_entrants.csv \
    --add-allocated-flag

  Optional: --season-end-year 2027   (default: auto-detected from today's date,
                                      Aug-Dec 2026 -> 2027, as in assign-category.py)
            --skip-category-check    (turn the entry-type check off)
            --allow-category-mismatch "Mark Shepherd"
                                     (pre-agreed exception; repeatable. The mismatch is
                                      printed as a note instead of failing validation)
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Set, Tuple, List
from datetime import datetime, date


NONLEAGUE_START = 900


def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return " ".join(str(s).strip().split())


def safe_int(s: Optional[str]) -> Optional[int]:
    s = norm(s)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def norm_name(s: str) -> str:
    return norm(s).lower()


def norm_dob(dob: str) -> str:
    """
    Normalise DOB to YYYY-MM-DD where possible.
    Handles formats like:
      07-Sep-12
      10/31/20
      31/10/2012
      2012-09-07

    Two-digit years: strptime("%y") maps 00-68 -> 2000s, 69-99 -> 1900s, which
    puts older riders (e.g. "55") in the future. Any parsed date later than
    today is therefore pulled back 100 years.
    """
    d = norm(dob)
    if not d:
        return ""
    fmts = [
        "%d-%b-%y",
        "%d-%b-%Y",
        "%m/%d/%y",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            parsed = datetime.strptime(d, f)
        except ValueError:
            continue
        if parsed.date() > date.today():
            try:
                parsed = parsed.replace(year=parsed.year - 100)
            except ValueError:
                continue
        return parsed.strftime("%Y-%m-%d")
    return d.lower()


def normalise_sex(s: Optional[str]) -> str:
    """
    Normalise a gender/sex value down to 'M', 'F', or '' if unknown/blank.
    Handles DB values like 'Male'/'Female' and CSV values like 'M'/'F'.
    """
    s = norm(s).lower()
    if s.startswith("f"):
        return "F"
    if s.startswith("m"):
        return "M"
    return ""


# ----------------------------------------------------------------------
# Category check
# ----------------------------------------------------------------------

def detect_season_end_year(today: Optional[date] = None, season_start_month: int = 8) -> int:
    """Same rule as assign-category.py: Aug-Dec -> next year, Jan-Jul -> this year."""
    if today is None:
        today = date.today()
    return today.year if today.month < season_start_month else today.year + 1


def base_category_from_age(age: int) -> Optional[str]:
    """
    Age at season end -> base category. KEEP IN SYNC with
    get_base_category_from_age() in assign-category.py.
    """
    if age <= 6:
        return "U6"
    if age <= 8:
        return "U8"
    if age <= 10:
        return "U10"
    if age <= 12:
        return "U12"
    if age <= 14:
        return "U14"
    if age <= 16:
        return "U16"
    if age <= 18:
        return "Jun"
    if age <= 22:
        return "U23"
    if age <= 40:
        return "Sen"
    if age <= 45:
        return "M40"
    if age <= 50:
        return "M45"
    if age <= 55:
        return "M50"
    if age <= 60:
        return "M55"
    if age <= 65:
        return "M60"
    if age <= 70:
        return "M65"
    return "M70"


# RiderHQ entry types (lower-cased, whitespace-collapsed) -> display name.
RIDERHQ_ENTRY_TYPES: Dict[str, str] = {
    t.lower(): t
    for t in [
        "Junior Female",
        "Junior Open",
        "Masters 40-49 Open",
        "Masters 50+ Open",
        "Senior/Masters Female",
        "Senior Open",
        "Under 10",
        "Under 12",
        "Under 8",
        "Youth (U-14 & U-16)",
    ]
}

# DB base categories (men / sex-agnostic) -> permitted RiderHQ entry type(s).
# Masters 40-49 = DB M40 + M45 (DB age is calendar age + 1); 50+ = M50 upwards.
_BASE_TO_ENTRY: Dict[str, Set[str]] = {
    "U6": {"Under 8"},
    "U8": {"Under 8"},
    "U10": {"Under 10"},
    "U12": {"Under 12"},
    "U14": {"Youth (U-14 & U-16)"},
    "U16": {"Youth (U-14 & U-16)"},
    "U23": {"Senior Open"},
    "Sen": {"Senior Open"},
    "M40": {"Masters 40-49 Open"},
    "M45": {"Masters 40-49 Open"},
    "M50": {"Masters 50+ Open"},
    "M55": {"Masters 50+ Open"},
    "M60": {"Masters 50+ Open"},
    "M65": {"Masters 50+ Open"},
    "M70": {"Masters 50+ Open"},
}


def expected_entry_types(base: str, sexes: List[str]) -> Set[str]:
    """
    Entry types acceptable for a rider with this base category.
    `sexes` is ['M'], ['F'] or ['M','F'] (unknown -> accept either).
    Edit here if the league ever lets riders race up/across categories.
    """
    allowed: Set[str] = set()
    for sx in sexes:
        if base == "Jun":
            allowed.add("Junior Female" if sx == "F" else "Junior Open")
        elif base in ("U23", "Sen") or base.startswith("M"):
            if sx == "F":
                allowed.add("Senior/Masters Female")
            else:
                allowed |= _BASE_TO_ENTRY[base]
        else:
            allowed |= _BASE_TO_ENTRY[base]
    return allowed


def load_db_indexes(
    db_paths: List[Path],
) -> Tuple[Set[int], Dict[Tuple[str, str, str], List[int]], Dict[int, str]]:
    """
    Returns:
      - all race_numbers across all DBs
      - mapping: (firstname, surname, dob) -> [race_numbers]
      - mapping: race_number -> normalised gender ('M'/'F') on record in the DB
    """
    race_numbers: Set[int] = set()
    name_dob_index: Dict[Tuple[str, str, str], List[int]] = {}
    gender_by_race_number: Dict[int, str] = {}

    for db in db_paths:
        conn = sqlite3.connect(str(db))
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(riders)")
        cols = {r[1] for r in cur.fetchall()}
        has_dob = "DOB" in cols
        has_gender = "gender" in cols

        select_cols = [
            "race_number",
            "firstname",
            "surname",
            "DOB" if has_dob else "NULL",
            "gender" if has_gender else "NULL",
        ]
        cur.execute(f"SELECT {', '.join(select_cols)} FROM riders")
        rows = cur.fetchall()

        for rn, fn, sn, dob, gender in rows:
            if rn is None:
                continue
            try:
                rn_i = int(rn)
            except Exception:
                continue

            race_numbers.add(rn_i)

            key = (norm_name(fn or ""), norm_name(sn or ""), norm_dob(dob or ""))
            if all(key):
                name_dob_index.setdefault(key, []).append(rn_i)

            g = normalise_sex(gender or "")
            if g:
                gender_by_race_number[rn_i] = g

        conn.close()

    for k in name_dob_index:
        name_dob_index[k] = sorted(set(name_dob_index[k]))

    return race_numbers, name_dob_index, gender_by_race_number


def next_free(start: int, used: Set[int]) -> int:
    n = start
    while n in used:
        n += 1
    return n


def truthy(s: str) -> bool:
    return norm(s).upper() in ("TRUE", "YES", "Y", "1")


def main():
    ap = argparse.ArgumentParser(description="Validate entrants CSV against multiple DBs.")
    ap.add_argument("--db", action="append", required=True, help="SQLite DB path (repeatable)")
    ap.add_argument("--csv", required=True, help="Entrants CSV")
    ap.add_argument("--out", required=True, help="Corrected output CSV")
    ap.add_argument("--add-allocated-flag", action="store_true",
                    help="Add _allocated_nonleague YES/NO column")
    ap.add_argument("--start-nonleague", type=int, default=NONLEAGUE_START)
    ap.add_argument("--season-end-year", type=int,
                    help="Season end year used for age bands (default: auto-detect, "
                         "e.g. 2027 for the 2026-27 season)")
    ap.add_argument("--skip-category-check", action="store_true",
                    help="Do not check Entry type against DOB-derived category")
    ap.add_argument("--allow-category-mismatch", action="append", default=[],
                    metavar="'First Last'",
                    help="Rider (full name, case-insensitive, repeatable) pre-agreed to enter "
                         "a different category; their category mismatch is noted, not an error")
    args = ap.parse_args()
    allowed_mismatch_names = {norm_name(n) for n in args.allow_category_mismatch}
    used_exemptions: Set[str] = set()

    db_paths = [Path(p) for p in args.db]
    csv_path = Path(args.csv)
    out_path = Path(args.out)

    for p in db_paths:
        if not p.exists():
            raise SystemExit(f"❌ DB not found: {p}")
    if not csv_path.exists():
        raise SystemExit(f"❌ CSV not found: {csv_path}")

    season_end = args.season_end_year or detect_season_end_year()

    print("Loading riders from DBs:")
    for p in db_paths:
        print(f"  - {p.name}")

    db_numbers, name_dob_index, gender_by_race_number = load_db_indexes(db_paths)

    issues: List[str] = []
    seen_rows = set()
    seen_numbers: Dict[int, int] = {}
    seen_csv_name_dob: Dict[Tuple[str, str, str], int] = {}
    cleaned_rows: List[Dict[str, str]] = []

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        required_cols = [
            "First name", "Last name", "Date of birth",
            "Membership number", "Has membership"
        ]
        for c in required_cols:
            if c not in headers:
                raise SystemExit(f"❌ Missing required column '{c}'")

        has_sex_col = "sex" in headers
        if not has_sex_col:
            print("⚠️  WARNING: entrants CSV has no 'sex' column — gender cross-check against DB will be skipped.")

        check_category = not args.skip_category_check
        if check_category:
            if "Entry type" not in headers:
                print("⚠️  WARNING: entrants CSV has no 'Entry type' column — category check skipped.")
                check_category = False
            else:
                print(f"Category check on: season ending {season_end} "
                      f"(rider age = {season_end} - year of birth)")

        for line_no, row in enumerate(reader, start=2):
            clean = {h: norm(row.get(h, "")) for h in headers}

            row_key = tuple(clean.items())
            if row_key in seen_rows:
                issues.append(f"Line {line_no}: Duplicate row.")
            else:
                seen_rows.add(row_key)

            first = clean["First name"]
            last = clean["Last name"]
            dob_raw = clean["Date of birth"]
            dob_n = norm_dob(dob_raw)

            if not first or not last or not dob_raw:
                issues.append(f"Line {line_no}: Missing First name / Last name / Date of birth.")

            csv_key = (norm_name(first), norm_name(last), dob_n)
            if all(csv_key):
                if csv_key in seen_csv_name_dob:
                    issues.append(
                        f"Line {line_no}: Duplicate entrant '{first} {last}' DOB={dob_raw}."
                    )
                else:
                    seen_csv_name_dob[csv_key] = line_no

            rn = safe_int(clean.get("Membership number"))
            has_membership = truthy(clean.get("Has membership"))

            if rn is not None:
                if rn in seen_numbers:
                    issues.append(
                        f"Line {line_no}: Duplicate Membership number {rn} "
                        f"(also line {seen_numbers[rn]})."
                    )
                else:
                    seen_numbers[rn] = line_no

                if has_membership and rn not in db_numbers:
                    issues.append(
                        f"Line {line_no}: League rider '{first} {last}' (DOB={dob_raw}) "
                        f"Membership number {rn} not found in any DB."
                    )

                if has_sex_col:
                    db_gender = gender_by_race_number.get(rn)
                    entry_gender = normalise_sex(clean.get("sex", ""))
                    if db_gender and entry_gender and db_gender != entry_gender:
                        issues.append(
                            f"Line {line_no}: GENDER MISMATCH for '{first} {last}' "
                            f"(Membership number {rn}): DB has '{db_gender}', "
                            f"entry form says '{entry_gender}'. Check before importing results."
                        )
            else:
                if has_membership:
                    matches = name_dob_index.get(csv_key, [])
                    if len(matches) == 1:
                        issues.append(
                            f"Line {line_no}: League rider missing Membership number. "
                            f"Suggested race number: {matches[0]}."
                        )
                    elif len(matches) > 1:
                        issues.append(
                            f"Line {line_no}: League rider missing Membership number. "
                            f"Multiple DB matches: {matches}."
                        )
                    else:
                        issues.append(
                            f"Line {line_no}: League rider missing Membership number. "
                            f"No DB match for '{first} {last}' DOB={dob_raw}."
                        )

            # ---- Entry type vs DOB-derived category -------------------
            if check_category and dob_raw:
                entry_raw = clean.get("Entry type", "")
                entry_name = RIDERHQ_ENTRY_TYPES.get(entry_raw.lower())
                who = f"'{first} {last}'" + (f" (bib {clean['Bib number']})" if clean.get("Bib number") else "")

                if not entry_raw:
                    issues.append(f"Line {line_no}: CATEGORY CHECK: {who} has no Entry type.")
                elif entry_name is None:
                    issues.append(
                        f"Line {line_no}: CATEGORY CHECK: {who} has unrecognised "
                        f"Entry type '{entry_raw}'."
                    )
                elif len(dob_n) == 10 and dob_n[4] == "-":
                    age = season_end - int(dob_n[:4])
                    base = base_category_from_age(age)

                    entry_sex = normalise_sex(clean.get("sex", "")) if has_sex_col else ""
                    sex = entry_sex or gender_by_race_number.get(rn, "") if rn is not None else entry_sex
                    sexes = [sex] if sex else ["M", "F"]

                    allowed = expected_entry_types(base, sexes)
                    full_name = norm_name(f"{first} {last}")
                    if entry_name not in allowed and full_name in allowed_mismatch_names:
                        used_exemptions.add(full_name)
                        print(f"ℹ️  Line {line_no}: category mismatch ALLOWED for {who}: "
                              f"entered '{entry_name}', DOB implies {base}{sex}.")
                    elif entry_name not in allowed:
                        issues.append(
                            f"Line {line_no}: CATEGORY MISMATCH for {who}: entered "
                            f"'{entry_name}', but DOB {dob_n} = age {age} at end of "
                            f"{season_end} season ({base}{sex}). Expected: "
                            f"{' / '.join(sorted(allowed))}."
                        )
                else:
                    issues.append(
                        f"Line {line_no}: CATEGORY CHECK: could not read DOB '{dob_raw}' "
                        f"for {who}."
                    )

            cleaned_rows.append(clean)

    for n in sorted(allowed_mismatch_names - used_exemptions):
        print(f"ℹ️  --allow-category-mismatch '{n}' was not needed "
              f"(no mismatch found for that name).")

    if issues:
        print("\n❌ VALIDATION FAILED\n")
        for i in issues:
            print(" -", i)
        raise SystemExit(1)

    used_numbers = set(seen_numbers) | set(db_numbers)
    next_id = args.start_nonleague
    allocated = 0

    for r in cleaned_rows:
        rn = safe_int(r.get("Membership number"))
        has_membership = truthy(r.get("Has membership"))

        if rn is None and not has_membership:
            next_id = next_free(next_id, used_numbers)
            r["Membership number"] = str(next_id)
            used_numbers.add(next_id)
            allocated += 1
            if args.add_allocated_flag:
                r["_allocated_nonleague"] = "YES"
        else:
            if args.add_allocated_flag:
                r["_allocated_nonleague"] = "NO"

    out_headers = list(headers)
    if args.add_allocated_flag and "_allocated_nonleague" not in out_headers:
        out_headers.append("_allocated_nonleague")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_headers)
        writer.writeheader()
        for r in cleaned_rows:
            writer.writerow({h: r.get(h, "") for h in out_headers})

    print("\n✅ VALIDATION PASSED")
    print(f"✅ Wrote corrected CSV: {out_path}")
    print(f"✅ Allocated non-league numbers: {allocated}")


if __name__ == "__main__":
    main()
