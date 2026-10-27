#!/usr/bin/env python3
"""
apply_prev_year_from_history.py
-------------------------------
Fill each rider's  race_category_previous_year  and  average_points_last_year
in the CURRENT season's race DB(s) straight from wmccl_history.db.

This replaces the two-step  export_prev_year_stats.py  ->  apply_prev_year_stats.py
route. No CSV and no name/DOB re-matching is needed: riders are looked up by the
permanent  riders.person_id  (set by assign_person_ids.py), so name changes
(Zofia -> Zosia), a corrected DOB or a new race number don't lose anybody.

WHAT IT TAKES FROM THE HISTORY
    For each rider with a person_id, their MOST RECENT earlier season:
        race_category  ->  race_category_previous_year
        avg_points     ->  average_points_last_year
    (avg_points = the average of their real race points that season; average-
    points rounds are not part of it -- the same figure the old export used.)

    Normally that is last season. If the rider skipped last season it uses the
    newest season they did ride, and says so (GAP) so you can decide.

SAFE TO RE-RUN
    Like apply_prev_year_stats.py, a rider is only filled when BOTH fields are
    currently blank; anything already set (including hand corrections) is left
    alone and reported. Use --force to overwrite deliberately.

CATEGORY-CHANGE ADJUSTMENT (optional, replaces adjust_prev_avg_on_cat_change.py)
    --cat-change-delta -5   subtracts 5 from the average of riders THIS RUN fills
                            whose base category changed (U6->U8, U10->U12, Sen->M40 ...),
                            exactly as adjust_prev_avg_on_cat_change.py does -- but only
                            for riders filled now, so re-running later for late entries
                            can never adjust anybody twice.
    Use one delta per DB (the old script was run per DB too, e.g. -10 for the
    youth DBs, -5 Women/Seniors). Averages are never pushed below 0.

DRY RUN BY DEFAULT: nothing is written without --apply.

USAGE
    apply_prev_year_from_history.py --history wmccl_history.db --season 2026-27 \\
        --db U8.db --cat-change-delta -10
    ... (look at the report) ... then add --apply

    Needs assign_person_ids.py --apply to have been run first. Riders with no
    person_id are new riders: they have no history and are left blank.
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

from adjust_prev_avg_on_cat_change import base_category


def is_blank(v) -> bool:
    return v is None or str(v).strip() == ""


def prev_season_label(season: str) -> str:
    """'2026-27' -> '2025-26' (to spot riders whose latest season is older than that)."""
    start = int(season[:4]) - 1
    return f"{start}-{str(start + 1)[-2:]}"


def main():
    ap = argparse.ArgumentParser(
        description="Fill previous-year category and average points from wmccl_history.db.")
    ap.add_argument("--history", required=True, help="wmccl_history.db")
    ap.add_argument("--season", required=True, help="THIS season, e.g. 2026-27 (only earlier seasons are used)")
    ap.add_argument("--db", nargs="+", required=True, help="This season's race DB(s)")
    ap.add_argument("--cat-change-delta", type=float, default=None,
                    help="Adjust the average of riders filled in THIS run whose base category changed "
                         "(e.g. -10 for youth DBs, -5 for Women/Seniors, +5 Masters).")
    ap.add_argument("--force", action="store_true",
                    help="Overwrite riders that already have previous-year values.")
    ap.add_argument("--report", help="Write a CSV of every rider and what was (or would be) done.")
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry run).")
    args = ap.parse_args()

    hp = Path(args.history)
    if not hp.exists():
        sys.exit(f"❌ history DB not found: {hp}")
    hist = sqlite3.connect(f"file:{hp}?mode=ro", uri=True)

    # most recent season before this one, per person
    latest = {}
    for pid, season, cat, avg in hist.execute(
            "SELECT person_id, season, race_category, avg_points FROM season_stats "
            "WHERE season < ? ORDER BY season", (args.season,)):
        latest[pid] = (season, cat, avg)
    last_season = prev_season_label(args.season)

    print(f"=== apply_prev_year_from_history {args.season} -- {'APPLY' if args.apply else 'DRY RUN (nothing written)'} ===")
    print(f"History: {len(latest)} people with an earlier season. Normal source season: {last_season}\n")

    report = [["db", "race_number", "name", "result", "category_now", "prev_category", "prev_average",
               "source_season", "note"]]
    grand = {"filled": 0, "adjusted": 0, "skipped_set": 0, "no_person_id": 0, "no_history": 0, "gap": 0}
    gaps = []

    for dbf in args.db:
        dbp = Path(dbf)
        if not dbp.exists():
            sys.exit(f"❌ DB not found: {dbp}")
        conn = sqlite3.connect(str(dbp))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(riders)")}
        missing = {"person_id", "race_category_previous_year", "average_points_last_year"} - cols
        if missing:
            sys.exit(f"❌ {dbp.name} is missing column(s) {sorted(missing)} -- run assign_person_ids.py --apply first")

        c = {"filled": 0, "adjusted": 0, "skipped_set": 0, "no_person_id": 0, "no_history": 0, "gap": 0}
        rows = conn.execute(
            "SELECT id, race_number, firstname, surname, race_category, person_id, "
            "race_category_previous_year, average_points_last_year FROM riders "
            "WHERE race_number IS NOT NULL AND race_number < 900 ORDER BY race_number").fetchall()

        for rid, rn, fn, sn, cat, pid, pcat, pavg in rows:
            name = f"{fn or ''} {sn or ''}".strip()
            if pid is None:
                c["no_person_id"] += 1
                report.append([dbp.name, rn, name, "no person_id (new rider)", cat, "", "", "", ""])
                continue
            if pid not in latest:
                c["no_history"] += 1
                report.append([dbp.name, rn, name, "no earlier season", cat, "", "", "", ""])
                continue
            if not args.force and not (is_blank(pcat) and is_blank(pavg)):
                c["skipped_set"] += 1
                report.append([dbp.name, rn, name, "already set (left alone)", cat, pcat, pavg, "", ""])
                continue

            season, hcat, havg = latest[pid]
            new_avg = havg
            note = []
            if season != last_season:
                c["gap"] += 1
                note.append(f"GAP: last rode {season}")
                gaps.append(f"{dbp.name} #{rn} {name}: using {season} ({hcat}, avg {havg})")
            if (args.cat_change_delta is not None and new_avg is not None
                    and base_category(cat) != base_category(hcat)):
                new_avg = max(0.0, round(new_avg + args.cat_change_delta, 2))
                c["adjusted"] += 1
                note.append(f"category changed {hcat} -> {cat}: {havg} {args.cat_change_delta:+g} = {new_avg}")
            c["filled"] += 1
            report.append([dbp.name, rn, name, "filled", cat, hcat, new_avg, season, "; ".join(note)])
            if args.apply:
                conn.execute("UPDATE riders SET race_category_previous_year=?, average_points_last_year=? WHERE id=?",
                             (hcat, new_avg, rid))

        if args.apply:
            conn.commit()
        conn.close()
        for k in c:
            grand[k] += c[k]
        print(f"{dbp.name:12s} filled {c['filled']:3d}   category-change adjusted {c['adjusted']:3d}   "
              f"already set {c['skipped_set']:3d}   new riders (no person_id) {c['no_person_id']:3d}   "
              f"no earlier season {c['no_history']:3d}")

    print(f"\nTOTAL filled {grand['filled']}, adjusted {grand['adjusted']}, already set {grand['skipped_set']}, "
          f"new riders {grand['no_person_id']}, no earlier season {grand['no_history']}")
    if gaps:
        print(f"\n-- {len(gaps)} rider(s) skipped last season; their NEWEST season was used (check you are happy): --")
        for g in gaps:
            print("  " + g)
    if args.report:
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(report)
        print(f"\nReport written to {args.report}")
    print("\nWritten." if args.apply else "\nDry run only. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
