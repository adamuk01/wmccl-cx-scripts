#!/usr/bin/env python3
"""
verify_reimport.py
-------------------
Snapshot a DB's results before a re-import, then compare against it
afterwards to confirm scoring didn't change -- only the new
laps_completed / finish_time_seconds fields did.

WHY:
    Re-running import_race_results.py against round 1/2 CSVs (after
    migrate_add_time_laps.py) should reproduce identical cat_position,
    overall_position, points and status -- same CSV in, same flags,
    same deterministic ranking. This script proves that rather than
    relying on eyeballing it.

FIELDS COMPARED (must be identical before vs after):
    cat_position, overall_position, points, is_ap, status

FIELDS REPORTED BUT NOT COMPARED (expected to change -- that's the point):
    laps_completed, finish_time_seconds

USAGE:
    # 1) Before re-importing, snapshot the rounds you're about to redo:
    python3 verify_reimport.py snapshot --db Masters.db --rounds 1 2 \
        --out masters_before.json

    # 2) Migrate + re-import as normal...

    # 3) Compare current state back against the snapshot:
    python3 verify_reimport.py compare --db Masters.db --rounds 1 2 \
        --snapshot masters_before.json
"""

import argparse
import json
import sqlite3
from pathlib import Path


SCORING_FIELDS = ["cat_position", "overall_position", "points", "is_ap", "status"]
TIME_FIELDS = ["laps_completed", "finish_time_seconds"]


def load_rows(db_path: Path, rounds):
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(results)")
    cols = {row[1] for row in cur.fetchall()}
    has_time_cols = set(TIME_FIELDS).issubset(cols)

    select_cols = ["r.race_number", "res.round"] + SCORING_FIELDS
    if has_time_cols:
        select_cols += TIME_FIELDS

    placeholders = ",".join("?" for _ in rounds)
    cur.execute(f"""
        SELECT {", ".join(select_cols)}
        FROM results res
        JOIN riders r ON r.id = res.rider_id
        WHERE res.round IN ({placeholders})
        ORDER BY r.race_number, res.round
    """, rounds)

    rows = {}
    for record in cur.fetchall():
        race_number, round_no = record[0], record[1]
        rest = record[2:]
        d = dict(zip(select_cols[2:], rest))
        rows[(race_number, round_no)] = d

    conn.close()
    return rows, has_time_cols


def cmd_snapshot(args):
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")

    rows, has_time_cols = load_rows(db_path, args.rounds)

    out = {
        "db": db_path.name,
        "rounds": args.rounds,
        "rows": {f"{k[0]}:{k[1]}": v for k, v in rows.items()},
    }

    out_path = Path(args.out)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"Snapshot: {db_path.name}, rounds {args.rounds}")
    print(f"  Rows captured : {len(rows)}")
    print(f"  Time columns present in DB : {has_time_cols}"
          + ("" if has_time_cols else "  (not migrated yet -- that's fine for a 'before' snapshot)"))
    print(f"  Written to    : {out_path}")


def cmd_compare(args):
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")

    snap_path = Path(args.snapshot)
    if not snap_path.exists():
        raise SystemExit(f"❌ Snapshot not found: {snap_path}")

    snapshot = json.loads(snap_path.read_text(encoding="utf-8"))
    before_rows = {
        tuple(int(p) for p in k.split(":")): v
        for k, v in snapshot["rows"].items()
    }

    rounds = args.rounds or snapshot["rounds"]
    after_rows, has_time_cols = load_rows(db_path, rounds)

    print(f"Compare: {db_path.name}, rounds {rounds}")
    print(f"  Snapshot rows : {len(before_rows)}")
    print(f"  Current rows  : {len(after_rows)}")

    all_keys = sorted(set(before_rows) | set(after_rows))

    scoring_diffs = []
    missing_before = []
    missing_after = []
    laps_filled = 0
    time_filled = 0
    laps_total = 0
    time_total = 0

    for key in all_keys:
        race_number, round_no = key
        b = before_rows.get(key)
        a = after_rows.get(key)

        if b is None:
            missing_before.append(key)
            continue
        if a is None:
            missing_after.append(key)
            continue

        row_diffs = []
        for field in SCORING_FIELDS:
            if b.get(field) != a.get(field):
                row_diffs.append(f"{field}: {b.get(field)!r} -> {a.get(field)!r}")
        if row_diffs:
            scoring_diffs.append((race_number, round_no, row_diffs))

        if has_time_cols:
            laps_total += 1
            time_total += 1
            if a.get("laps_completed") is not None:
                laps_filled += 1
            if a.get("finish_time_seconds") is not None:
                time_filled += 1

    print("\n--- Scoring fields (should be UNCHANGED) ---")
    if not scoring_diffs and not missing_before and not missing_after:
        print("  ✅ No differences found -- scoring is identical before/after.")
    else:
        if scoring_diffs:
            print(f"  ⚠️  {len(scoring_diffs)} row(s) with scoring differences:")
            for race_number, round_no, diffs in scoring_diffs:
                print(f"    #{race_number} round {round_no}: " + "; ".join(diffs))
        if missing_before:
            print(f"  ⚠️  {len(missing_before)} row(s) present now but NOT in snapshot: {missing_before}")
        if missing_after:
            print(f"  ⚠️  {len(missing_after)} row(s) in snapshot but MISSING now: {missing_after}")

    print("\n--- Time/laps fields (EXPECTED to now be populated) ---")
    if has_time_cols:
        print(f"  laps_completed filled       : {laps_filled} / {laps_total}")
        print(f"  finish_time_seconds filled  : {time_filled} / {time_total}")
        if laps_filled < laps_total or time_filled < time_total:
            print("  (Some rows still NULL -- normal for DNF rows/finish_time_seconds, "
                  "otherwise worth checking those specific rows.)")
    else:
        print("  DB has no laps_completed/finish_time_seconds columns -- "
              "run migrate_add_time_laps.py first.")

    print("\nDone.")


def main():
    ap = argparse.ArgumentParser(
        description="Snapshot/compare results before and after a re-import, to confirm scoring is unchanged."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    sp_snap = sub.add_parser("snapshot", help="Capture current results for given rounds.")
    sp_snap.add_argument("--db", required=True)
    sp_snap.add_argument("--rounds", type=int, nargs="+", required=True)
    sp_snap.add_argument("--out", required=True, help="Output JSON file path.")
    sp_snap.set_defaults(func=cmd_snapshot)

    sp_cmp = sub.add_parser("compare", help="Compare current results against a snapshot.")
    sp_cmp.add_argument("--db", required=True)
    sp_cmp.add_argument("--snapshot", required=True, help="Snapshot JSON file from the 'snapshot' command.")
    sp_cmp.add_argument("--rounds", type=int, nargs="+",
                        help="Defaults to the rounds stored in the snapshot.")
    sp_cmp.set_defaults(func=cmd_compare)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
