#!/usr/bin/env python3
"""
find_duplicate_accounts.py
---------------------------
Audit already-processed entrants CSV(s) for riders who were allocated a
NON-LEAGUE number (>= --threshold, default 900) but who actually match an
existing LEAGUE rider (race_number < threshold) by name + DOB in one of your
race DBs.

WHY:
    Some riders have two RiderHQ accounts -- one used to register for the
    league (which holds their real Membership number) and another used to
    enter a specific race (e.g. a parent's login), which RiderHQ reports as
    'Has membership=false'. validate_and_allocate_entrants.py now catches
    this going forward, but rounds that already ran before the fix may have
    silently allocated these riders a fresh 900+ number instead of using
    their real league number. Because import_race_results.py filters out any
    race_no >= --nonleague-threshold entirely, those riders' results for
    that round were never imported at all.

    This script re-checks the entrants CSV(s) you already produced for a
    round (the 'corrected_*.csv' files from validate_and_allocate_entrants.py,
    or the original RiderHQ export) against your league DBs, and reports any
    900+ entries that should really have used a real league number.

WHAT IT DOES:
    - Loads (firstname, surname, DOB) -> [race_number] for every LEAGUE rider
      (race_number < threshold) across all DBs you pass in (same in-memory
      lookup pattern used in validate_and_allocate_entrants.py).
    - Reads each entrants CSV you pass in with --entrants (can pass more than
      one, e.g. one per round, if you kept dated copies).
    - For every row with a Membership number >= threshold, checks it against
      the league index by normalised name + DOB.
    - Reports any match: this rider almost certainly has a duplicate RiderHQ
      account, and this round's entry/result should be corrected to use
      their real league race_number instead.

USAGE:
    python3 find_duplicate_accounts.py \
        --db U8.db --db U10.db --db U12.db --db Youth.db \
        --db Seniors.db --db Masters.db --db Women.db \
        --entrants corrected_WMCCLRiderEntry_Round1.csv \
        --out duplicate_account_report.csv

    # Check several archived rounds at once:
    python3 find_duplicate_accounts.py \
        --db U8.db --db U10.db --db U12.db --db Youth.db \
        --db Seniors.db --db Masters.db --db Women.db \
        --entrants Round1/corrected_WMCCLRiderEntry.csv \
        --entrants Round2/corrected_WMCCLRiderEntry.csv \
        --out duplicate_account_report.csv

NOTES:
    - Matching is name+DOB based, normalised the same way as
      validate_and_allocate_entrants.py (case-insensitive names, DOB parsed
      from multiple common formats and compared as YYYY-MM-DD), so it copes
      with the same 'mm/dd/yy' vs 'DD-Mon-YYYY' style mismatches you've hit
      elsewhere (apply_prev_year_stats.py, convert-DOB.py).
    - This does NOT modify any CSV or DB. It only reports. Fixing an
      affected round means correcting that round's entrants CSV (and/or
      re-running import_race_results.py) with the real race_number, per
      rider, after you've confirmed the match by hand.
    - A rider can legitimately show up more than once across rounds if you
      pass multiple --entrants files; each occurrence is reported separately
      so you can see which round(s) were affected.
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime


def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return " ".join(str(s).strip().split())


def norm_name(s: str) -> str:
    return norm(s).lower()


def safe_int(s: Optional[str]) -> Optional[int]:
    s = norm(s)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _resolve_two_digit_year(yy: int) -> int:
    """
    Resolve a 2-digit year the same way initial-riderHQ-data-import.py's
    parse_yob_from_dob() does: relative to TODAY's year, not a fixed pivot.

    strptime's built-in '%y' uses a FIXED pivot (00-68 -> 2000-2068,
    69-99 -> 1900-1999), which silently mis-centuries anyone born before
    ~1969 in a 2-digit-year format -- e.g. DOB year '58' becomes 2058
    instead of 1958. That breaks name+DOB matching against a 4-digit-year
    DOB for the same person (exactly the Andrew Reid / Masters 60+ case).
    """
    today = datetime.today()
    last_two = today.year % 100
    return (1900 + yy) if yy > last_two else (2000 + yy)


def norm_dob(dob: str) -> str:
    """
    Normalise DOB to YYYY-MM-DD where possible.
    Handles formats seen across this codebase:
      07-Sep-12, 4-DEC-2015, 10/31/20, 31/10/2012, 2/3/58, 2012-09-07
    """
    d = norm(dob)
    if not d:
        return ""

    # 4-digit-year formats are unambiguous -- try these first.
    for f in ("%d-%b-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(d, f).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # 2-digit-year formats: parse for day/month, then resolve the century
    # ourselves rather than trusting strptime's fixed 68/69 pivot.
    for f in ("%d-%b-%y", "%m/%d/%y"):
        try:
            dt = datetime.strptime(d, f)
        except ValueError:
            continue
        year = _resolve_two_digit_year(dt.year % 100)
        try:
            dt = dt.replace(year=year)
        except ValueError:
            continue
        return dt.strftime("%Y-%m-%d")

    return d.lower()


def load_league_index(
    db_paths: List[Path], threshold: int
) -> Tuple[Dict[Tuple[str, str, str], List[int]], Set[int]]:
    """
    Build (firstname, surname, DOB) -> [race_number] for LEAGUE riders only
    (race_number < threshold), across all given DBs.

    Also returns the full set of league race_numbers, for a quick sanity
    check that DBs loaded OK.
    """
    name_dob_index: Dict[Tuple[str, str, str], List[int]] = {}
    league_numbers: Set[int] = set()

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

            if rn_i >= threshold:
                # Not a real league number -- don't index it as one.
                continue

            league_numbers.add(rn_i)

            key = (norm_name(fn or ""), norm_name(sn or ""), norm_dob(dob or ""))
            if all(key):
                name_dob_index.setdefault(key, []).append(rn_i)

        conn.close()

    for k in name_dob_index:
        name_dob_index[k] = sorted(set(name_dob_index[k]))

    return name_dob_index, league_numbers


def audit_entrants_csv(
    csv_path: Path,
    league_index: Dict[Tuple[str, str, str], List[int]],
    threshold: int,
) -> List[Dict[str, str]]:
    """
    Scan one entrants CSV for rows with an allocated non-league number
    (Membership number >= threshold) that match a real league rider by
    name+DOB. Returns a list of report rows (dicts).
    """
    findings: List[Dict[str, str]] = []

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        required = ["First name", "Last name", "Date of birth", "Membership number"]
        missing = [c for c in required if c not in headers]
        if missing:
            print(f"  SKIPPING {csv_path.name}: missing column(s) {missing}")
            return findings

        for line_no, row in enumerate(reader, start=2):
            first = norm(row.get("First name"))
            last = norm(row.get("Last name"))
            dob_raw = norm(row.get("Date of birth"))
            rn = safe_int(row.get("Membership number"))

            if rn is None or rn < threshold:
                continue  # not a non-league allocation, nothing to check

            key = (norm_name(first), norm_name(last), norm_dob(dob_raw))
            matches = league_index.get(key, [])
            if not matches:
                continue  # genuinely looks like a new non-league entrant

            findings.append({
                "source_file": csv_path.name,
                "line": str(line_no),
                "first_name": first,
                "last_name": last,
                "dob": dob_raw,
                "allocated_nonleague_number": str(rn),
                "matched_league_numbers": ";".join(str(m) for m in matches),
            })

    return findings


def main():
    ap = argparse.ArgumentParser(
        description="Find entrants allocated a non-league (900+) number who actually "
                    "match an existing league rider by name+DOB (duplicate RiderHQ account)."
    )
    ap.add_argument("--db", action="append", required=True,
                    help="SQLite league DB path (repeatable, e.g. --db U8.db --db Youth.db ...)")
    ap.add_argument("--entrants", action="append", required=True,
                    help="Entrants CSV to audit (repeatable). Use the 'corrected_*.csv' "
                         "file produced for each round, or the raw RiderHQ export.")
    ap.add_argument("--threshold", type=int, default=900,
                    help="Non-league number threshold (default: 900, matches "
                         "import_race_results.py --nonleague-threshold default)")
    ap.add_argument("--out", default="duplicate_account_report.csv",
                    help="Output CSV report path (default: duplicate_account_report.csv)")
    args = ap.parse_args()

    db_paths = [Path(p) for p in args.db]
    entrant_paths = [Path(p) for p in args.entrants]

    for p in db_paths:
        if not p.exists():
            raise SystemExit(f"❌ DB not found: {p}")
    for p in entrant_paths:
        if not p.exists():
            raise SystemExit(f"❌ Entrants CSV not found: {p}")

    print("Loading league riders (race_number < {}) from DBs:".format(args.threshold))
    for p in db_paths:
        print(f"  - {p.name}")

    league_index, league_numbers = load_league_index(db_paths, args.threshold)
    print(f"  -> {len(league_numbers)} league riders indexed, "
          f"{len(league_index)} unique name+DOB keys.\n")

    all_findings: List[Dict[str, str]] = []
    for p in entrant_paths:
        print(f"Auditing {p} ...")
        findings = audit_entrants_csv(p, league_index, args.threshold)
        print(f"  -> {len(findings)} suspected duplicate-account entrant(s) found.")
        all_findings.extend(findings)

    out_path = Path(args.out)
    fieldnames = [
        "source_file", "line", "first_name", "last_name", "dob",
        "allocated_nonleague_number", "matched_league_numbers",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in all_findings:
            w.writerow(row)

    print(f"\nTotal suspected duplicate-account entrants: {len(all_findings)}")
    if all_findings:
        print("\nSummary:")
        for row in all_findings:
            print(
                f"  {row['source_file']} line {row['line']}: "
                f"{row['first_name']} {row['last_name']} (DOB={row['dob']}) "
                f"was allocated #{row['allocated_nonleague_number']} but matches "
                f"league number(s) {row['matched_league_numbers']}"
            )
        print(f"\nFull report written to: {out_path}")
        print(
            "\nNext step: for each of these, confirm by hand it's the same person, "
            "then correct that round's data to use their real league race_number "
            "(update the entrants CSV and/or re-run import_race_results.py so their "
            "result is imported under the correct number instead of being filtered "
            "out as non-league)."
        )
    else:
        print(f"No issues found. Empty report written to: {out_path}")


if __name__ == "__main__":
    main()
