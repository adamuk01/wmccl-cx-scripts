#!/usr/bin/env python3
"""
validate_and_allocate_entrants_multi_db.py
------------------------------------------
Validate and clean an entrants CSV against MULTIPLE race databases,
and allocate race numbers to NON-league riders.

Key rule:
- "Has membership" == TRUE  → rider IS a league rider → MUST have Membership number
- Missing Membership number for league rider → FAIL, but attempt to suggest correct number
  using First name + Last name + Date of birth against DBs.

Membership number = WMCCL race number.

Usage:
  python3 validate_and_allocate_entrants_multi_db.py \
    --db U8.db --db U10.db --db U12.db --db Youth.db --db Seniors.db --db Masters.db \
    --csv entrants.csv \
    --out corrected_entrants.csv \
    --add-allocated-flag
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Set, Tuple, List
from datetime import datetime


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
            return datetime.strptime(d, f).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return d.lower()


def load_db_indexes(db_paths: List[Path]) -> Tuple[Set[int], Dict[Tuple[str, str, str], List[int]]]:
    """
    Returns:
      - all race_numbers across all DBs
      - mapping: (firstname, surname, dob) -> [race_numbers]
    """
    race_numbers: Set[int] = set()
    name_dob_index: Dict[Tuple[str, str, str], List[int]] = {}

    for db in db_paths:
        conn = sqlite3.connect(str(db))
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(riders)")
        cols = {r[1] for r in cur.fetchall()}
        has_dob = "DOB" in cols

        if has_dob:
            cur.execute("SELECT race_number, firstname, surname, DOB FROM riders")
            rows = cur.fetchall()
        else:
            cur.execute("SELECT race_number, firstname, surname FROM riders")
            rows = [(rn, fn, sn, "") for rn, fn, sn in cur.fetchall()]

        for rn, fn, sn, dob in rows:
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

        conn.close()

    for k in name_dob_index:
        name_dob_index[k] = sorted(set(name_dob_index[k]))

    return race_numbers, name_dob_index


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
    args = ap.parse_args()

    db_paths = [Path(p) for p in args.db]
    csv_path = Path(args.csv)
    out_path = Path(args.out)

    for p in db_paths:
        if not p.exists():
            raise SystemExit(f"❌ DB not found: {p}")
    if not csv_path.exists():
        raise SystemExit(f"❌ CSV not found: {csv_path}")

    print("Loading riders from DBs:")
    for p in db_paths:
        print(f"  - {p.name}")

    db_numbers, name_dob_index = load_db_indexes(db_paths)

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

            cleaned_rows.append(clean)

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

