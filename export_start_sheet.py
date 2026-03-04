#!/usr/bin/env python3
"""
export_start_sheet.py
---------------------
Create a start sheet CSV (for sign-on) from the validated/corrected entrants CSV.

Adds 1BX from the race DB (riders table) based on Membership number (= race_number).

Output headers (exact order):
  Entry type,Bib number,First name,Last name,Has membership,Entered by,Amount paid,sex,
  Date of birth,club,Are you a member of British Cycling?,Membership ID,emergency_contact_det,
  Membership number,1BX

Sorting:
  - Entry type (asc)
  - sex (Female before Male, then other)
  - Last name (asc, case-insensitive)
  - First name (asc)
  - Membership number (asc)

Notes:
  - Does NOT output _allocated_nonleague
  - If rider not found in DB (e.g. non-league), 1BX is left blank
  - DB column for 1 bike is expected to be 'IBX' in riders table (as per your schema)
    (we output it as '1BX' column name in the start sheet)
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict, Optional


OUTPUT_HEADERS = [
    "Entry type",
    "Bib number",
    "First name",
    "Last name",
    "Has membership",
    "Entered by",
    "Amount paid",
    "sex",
    "Date of birth",
    "club",
    "Are you a member of British Cycling?",
    "Membership ID",
    "emergency_contact_det",
    "Membership number",
    "1BX",
]

def normalise_1bx(val: Optional[str]) -> str:
    v = norm(val).lower()
    if v in ("y", "yes", "true", "t", "1"):
        return "Yes"
    return "No"


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


def load_1bx_lookup(db_path: Path) -> Dict[int, str]:
    """
    race_number -> IBX value (e.g. 'Yes'/'No'/'' depending on how you store it)
    """
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Verify column exists; if not, return empty dict with a warning
    cur.execute("PRAGMA table_info(riders)")
    cols = {r[1] for r in cur.fetchall()}
    if "IBX" not in cols:
        conn.close()
        print("WARNING: riders table has no 'IBX' column. 1BX will be blank.")
        return {}

    cur.execute("SELECT race_number, IBX FROM riders")
    out = {}
    for rn, ibx in cur.fetchall():
        if rn is None:
            continue
        try:
            out[int(rn)] = normalise_1bx(ibx)
        except Exception:
            continue

    conn.close()
    return out


def sex_sort_key(sex: str) -> int:
    """
    Sort order: Female, Male, other/blank
    """
    s = norm(sex).lower()
    if s.startswith("f"):
        return 0
    if s.startswith("m"):
        return 1
    return 9


def main():
    ap = argparse.ArgumentParser(description="Export start sheet CSV (sign-on) with 1BX column from DB.")
    ap.add_argument("--db", required=True, help="Race DB for this start sheet (e.g. Youth.db, Seniors.db)")
    ap.add_argument("--entrants", required=True, help="Validated/corrected entrants CSV")
    ap.add_argument("--out", required=True, help="Output start sheet CSV")
    ap.add_argument("--entry-type", default=None,
                help="Only include rows where 'Entry type' matches exactly (e.g. 'Under 8').")
    args = ap.parse_args()

    db_path = Path(args.db)
    entrants_path = Path(args.entrants)
    out_path = Path(args.out)

    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")
    if not entrants_path.exists():
        raise SystemExit(f"❌ Entrants CSV not found: {entrants_path}")

    lookup_1bx = load_1bx_lookup(db_path)

    with entrants_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        in_headers = reader.fieldnames or []

        # Ensure required columns exist in input (we'll still output blanks if missing)
        required_in = [h for h in OUTPUT_HEADERS if h != "1BX"]
        missing = [h for h in required_in if h not in in_headers]
        if missing:
            raise SystemExit(f"❌ Entrants CSV missing required columns: {missing}")

        rows = []
        for r in reader:
            rn = safe_int(r.get("Membership number"))
            if args.entry_type is not None:
                if norm(r.get("Entry type", "")) != args.entry_type:
                    continue
            ibx = lookup_1bx.get(rn, "No") if rn is not None else "No"



            out_row = {h: norm(r.get(h, "")) for h in OUTPUT_HEADERS if h != "1BX"}
            out_row["1BX"] = ibx

            rows.append(out_row)

    # Sort rows
    rows.sort(key=lambda r: (
        norm(r.get("Entry type", "")).lower(),
        sex_sort_key(r.get("sex", "")),
        norm(r.get("Last name", "")).lower(),
        norm(r.get("First name", "")).lower(),
        safe_int(r.get("Membership number")) or 10**9,
    ))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=OUTPUT_HEADERS)
        writer.writeheader()
        for r in rows:
            # ensure only the required headers are written (no _allocated_nonleague etc.)
            writer.writerow({h: r.get(h, "") for h in OUTPUT_HEADERS})

    print(f"✅ Wrote start sheet: {out_path}  (rows={len(rows)})")


if __name__ == "__main__":
    main()

