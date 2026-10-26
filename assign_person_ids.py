#!/usr/bin/env python3
"""
assign_person_ids.py
--------------------
Link this season's RETURNING riders to their permanent person_id in the
central history DB (wmccl_history.db), by adding / filling a nullable
`person_id` column on each race DB's riders table.

  * Only riders who already exist in the history get a person_id. A rider
    with no match is a NEW rider this season and stays NULL -- they receive
    their person_id when their first season is archived (archive_season.py).
    No empty person records are created here.
  * Matching rules are the shared ones in rider_history.py: DOB is the
    anchor; BC number is supporting evidence only. Anything ambiguous goes
    on the REVIEW list and is NOT linked until you resolve it.
  * SAFE BY DEFAULT: a dry run. Nothing is written unless --apply.
  * SAFE TO RE-RUN: fills NULLs only; an existing person_id that disagrees
    with the match is reported as a CONFLICT and left alone (use --force to
    overwrite). Late joiners are picked up simply by running it again.

It also audits the previous-year carry-forward you've already applied
(race_category_previous_year / average_points_last_year) against what the
history says that person actually did last season.

USAGE
  assign_person_ids.py --season 2026-27 --history wmccl_history.db \
      --db U8.db U10.db U12.db Youth.db Women.db Masters.db Seniors.db
  ... review the report, add any decisions to person_overrides.csv, then
  assign_person_ids.py ... --overrides person_overrides.csv --apply

OVERRIDES (same file and format as archive_season.py)
  season,source_db,race_number,decision     decision = new | person:<id> | skip

--report FILE   also write every non-routine finding to a CSV.
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import rider_history as rh

AVG_TOLERANCE = 0.5   # |average_points_last_year - history avg_points| above this is listed


def load_overrides(path: Optional[str]) -> Dict[Tuple[str, str, int], str]:
    out: Dict[Tuple[str, str, int], str] = {}
    if not path:
        return out
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"❌ overrides file not found: {p}")
    with p.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                out[(row["season"].strip(), Path(row["source_db"].strip()).name,
                     int(row["race_number"]))] = row["decision"].strip().lower()
            except (KeyError, ValueError):
                raise SystemExit(f"❌ bad overrides row: {row}")
    return out


def latest_season_row(hc: sqlite3.Connection, person_id: int, before_season: str):
    """Most recent history row for this person from a season BEFORE the one being assigned."""
    return hc.execute(
        "SELECT season, source_db, race_number, race_category, avg_points FROM season_stats "
        "WHERE person_id=? AND season<? ORDER BY season DESC LIMIT 1",
        (person_id, before_season)).fetchone()


def describe(hc, pid: int) -> str:
    p = hc.execute("SELECT firstname, surname, DOB, BC_number FROM person WHERE person_id=?", (pid,)).fetchone()
    if not p:
        return f"person {pid}"
    s = hc.execute("SELECT season, source_db, race_number FROM season_stats WHERE person_id=? "
                   "ORDER BY season DESC LIMIT 1", (pid,)).fetchone()
    where = f", latest {s[0]} {s[1]} #{s[2]}" if s else ""
    return f"person {pid}: {p[0]} {p[1]}, DOB {p[2]}, BC {p[3] or '-'}{where}"


def main():
    ap = argparse.ArgumentParser(description="Link returning riders to their person_id.")
    ap.add_argument("--season", required=True, help="the season being assigned, e.g. 2026-27")
    ap.add_argument("--history", required=True, help="wmccl_history.db (read only)")
    ap.add_argument("--db", nargs="+", required=True, help="this season's race DBs")
    ap.add_argument("--overrides", help="person_overrides.csv")
    ap.add_argument("--report", help="write findings to this CSV")
    ap.add_argument("--force", action="store_true", help="overwrite a person_id that disagrees with the match")
    ap.add_argument("--apply", action="store_true", help="actually write (default: dry run)")
    args = ap.parse_args()

    hp = Path(args.history)
    if not hp.exists():
        raise SystemExit(f"❌ history DB not found: {hp}")
    hc = sqlite3.connect(f"file:{hp}?mode=ro", uri=True)
    overrides = load_overrides(args.overrides)
    used = set()

    totals = dict(linked=0, already=0, new=0, review=0, conflict=0, skipped=0, override=0)
    findings: List[List] = []       # kind, db, number, name, dob, bc, detail
    audit_cat: List[List] = []
    audit_missing: List[List] = []
    audit_nomatch: List[List] = []
    audit_avg: List[List] = []
    cat_change_deltas: List[float] = []
    never_raced_prev: List[List] = []
    seen: Dict[int, Tuple[str, int]] = {}   # person_id -> (db, race_number) within this run
    per_db = []
    updates: List[Tuple[Path, int, int]] = []   # (db path, riders.id, person_id)

    for db_file in args.db:
        dbp = Path(db_file)
        if not dbp.exists():
            raise SystemExit(f"❌ DB not found: {dbp}")
        rc = sqlite3.connect(f"file:{dbp}?mode=ro", uri=True)
        cols = {r[1] for r in rc.execute("PRAGMA table_info(riders)")}
        has_col = "person_id" in cols
        sel = ("SELECT id, race_number, BC_number, firstname, surname, gender, DOB, race_category, "
               "race_category_previous_year, average_points_last_year"
               + (", person_id" if has_col else ", NULL") + " FROM riders ORDER BY race_number")
        n = dict(linked=0, new=0, review=0)

        for (rid, race_no, bc_raw, first, sur, gender, dob_raw, cat, cat_prev, avg_ly, cur_pid) in rc.execute(sel).fetchall():
            if race_no is None:
                continue
            name = f"{rh.norm(first)} {rh.norm(sur)}".strip()
            dob = rh.norm_dob(dob_raw)
            bc = rh.safe_int(bc_raw)
            key = (args.season, dbp.name, int(race_no))
            pid: Optional[int] = None

            dec = overrides.get(key)
            if dec is not None:
                used.add(key)
                totals["override"] += 1
                if dec == "skip":
                    totals["skipped"] += 1
                    continue
                if dec == "new":
                    totals["new"] += 1; n["new"] += 1
                    continue
                pid = int(dec.split(":")[-1])
                if not hc.execute("SELECT 1 FROM person WHERE person_id=?", (pid,)).fetchone():
                    raise SystemExit(f"❌ override {key}: person_id {pid} does not exist")
            else:
                m = rh.match_rider(hc, first, sur, dob, bc, gender)
                if m.kind == rh.NEW:
                    totals["new"] += 1; n["new"] += 1
                    if cat_prev:   # says they were registered last year but we have no history for them
                        if avg_ly in (None, 0, 0.0):    # never raced -> deliberately not in the history
                            never_raced_prev.append([dbp.name, race_no, name])
                        else:
                            audit_nomatch.append([dbp.name, race_no, name, dob, bc, cat_prev, avg_ly, m.reason])
                    continue
                if m.kind == rh.REVIEW:
                    totals["review"] += 1; n["review"] += 1
                    cand = " | ".join(describe(hc, c) for c in m.candidates)
                    findings.append(["REVIEW", dbp.name, race_no, name, dob, bc,
                                     m.reason + (f" -- vs {cand}" if cand else "")])
                    continue
                pid = m.person_id
                for fl in m.flags:
                    findings.append(["FLAG", dbp.name, race_no, name, dob, bc, fl])

            # same person twice in THIS season is never auto-linked
            if pid in seen:
                totals["review"] += 1; n["review"] += 1
                o = seen[pid]
                findings.append(["REVIEW", dbp.name, race_no, name, dob, bc,
                                 f"person {pid} already linked to {o[0]} #{o[1]} this season -- "
                                 f"duplicate account / two numbers?"])
                continue
            seen[pid] = (dbp.name, int(race_no))

            # existing person_id?
            if cur_pid is not None:
                if cur_pid == pid:
                    totals["already"] += 1
                    continue
                totals["conflict"] += 1
                findings.append(["CONFLICT", dbp.name, race_no, name, dob, bc,
                                 f"DB already has person_id {cur_pid}, match says {pid}"
                                 + ("" if args.force else " (left alone; --force to overwrite)")])
                if not args.force:
                    continue

            totals["linked"] += 1; n["linked"] += 1
            updates.append((dbp, rid, pid))

            # -- audit against last season's history --
            last = latest_season_row(hc, pid, args.season)
            if last:
                _, ldb, lnum, lcat, lavg = last
                if not rh.norm(cat_prev):
                    audit_missing.append([dbp.name, race_no, name, cat, f"history: {lcat} (avg {lavg:.1f})" if lavg is not None else f"history: {lcat}"])
                else:
                    if rh.norm(cat_prev) != rh.norm(lcat):
                        audit_cat.append([dbp.name, race_no, name, cat_prev, lcat])
                    if avg_ly is not None and lavg is not None:
                        diff = float(avg_ly) - float(lavg)
                        if rh.norm(cat_prev) == rh.norm(cat):
                            if abs(diff) > AVG_TOLERANCE:      # same category: should match
                                audit_avg.append([dbp.name, race_no, name, round(float(avg_ly), 2), round(float(lavg), 2)])
                        else:                                   # moved category: an adjustment is expected
                            cat_change_deltas.append(round(diff, 1))
        per_db.append((dbp.name, n["linked"], n["new"], n["review"], has_col))
        rc.close()

    stale = [k for k in overrides if k[0] == args.season and k not in used]

    # ---------------- report ----------------
    print(f"\n=== assign_person_ids {args.season} -- {'APPLY' if args.apply else 'DRY RUN (nothing written)'} ===")
    print(f"\n{'DB':<10}{'to link':>9}{'new rider':>11}{'review':>8}   person_id column")
    for name, lk, nw, rv, hascol in per_db:
        print(f"{name:<10}{lk:>9}{nw:>11}{rv:>8}   {'present' if hascol else 'will be added'}")
    print(f"\nLinked (new this run): {totals['linked']}   already linked: {totals['already']}   "
          f"new riders (no history yet): {totals['new']}   REVIEW: {totals['review']}   "
          f"conflicts: {totals['conflict']}   by override: {totals['override']}")

    def show(title, rows, fmt, limit=40):
        if not rows:
            return
        print(f"\n-- {title}: {len(rows)} --")
        for r in rows[:limit]:
            print("  " + fmt(r))
        if len(rows) > limit:
            print(f"  ... and {len(rows) - limit} more (use --report for the full list)")

    show("REVIEW (held out until resolved)", [f for f in findings if f[0] == "REVIEW"],
         lambda r: f"{r[1]} #{r[2]} {r[3]} (DOB {r[4]}, BC {r[5]}): {r[6]}")
    show("CONFLICT", [f for f in findings if f[0] == "CONFLICT"],
         lambda r: f"{r[1]} #{r[2]} {r[3]}: {r[6]}")
    show("FLAG (linked, but glance at it)", [f for f in findings if f[0] == "FLAG"],
         lambda r: f"{r[1]} #{r[2]} {r[3]}: {r[6]}")
    print("\n===== AUDIT of last-year carry-forward already in these DBs =====")
    show("Matched to history but race_category_previous_year is BLANK (carry-forward missed them)",
         audit_missing, lambda r: f"{r[0]} #{r[1]} {r[2]} now {r[3]} -> {r[4]}")
    show("race_category_previous_year DISAGREES with last season's history",
         audit_cat, lambda r: f"{r[0]} #{r[1]} {r[2]}: DB says '{r[3]}', history says '{r[4]}'")
    show("SAME category as last year but average_points_last_year differs from history by more than "
         f"{AVG_TOLERANCE}", audit_avg,
         lambda r: f"{r[0]} #{r[1]} {r[2]}: DB {r[3]} vs history {r[4]}")
    if cat_change_deltas:
        from collections import Counter
        dist = ", ".join(f"{d:+g}: {c}" for d, c in sorted(Counter(round(x) for x in cat_change_deltas).items()))
        print(f"\n-- Category-changers ({len(cat_change_deltas)}): DB average minus history average (the category-change "
              f"adjustment, expected) -- {dist}")
    if never_raced_prev:
        print(f"\n-- {len(never_raced_prev)} rider(s) have a previous-year category but average 0: registered last season but "
              f"never raced, so (by design) not in the history. Expected; no action.")
    show("Has a previous-year category and a real average but NO match in history (not found?)",
         audit_nomatch, lambda r: f"{r[0]} #{r[1]} {r[2]} (DOB {r[3]}, BC {r[4]}): prev '{r[5]}' avg {r[6]} -- {r[7]}")
    if stale:
        print(f"\n-- {len(stale)} override(s) matched no rider (typo in race number?) --")
        for k in stale:
            print(f"  {k}")

    if args.report:
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["kind", "source_db", "race_number", "name", "dob", "bc", "detail"])
            w.writerows(findings)
            for r in audit_missing: w.writerow(["AUDIT-missing-prev", r[0], r[1], r[2], "", "", f"{r[3]} -> {r[4]}"])
            for r in audit_cat:     w.writerow(["AUDIT-category", r[0], r[1], r[2], "", "", f"DB '{r[3]}' vs history '{r[4]}'"])
            for r in audit_avg:     w.writerow(["AUDIT-average", r[0], r[1], r[2], "", "", f"DB {r[3]} vs history {r[4]}"])
            for r in audit_nomatch: w.writerow(["AUDIT-nomatch", r[0], r[1], r[2], r[3], r[4], f"prev '{r[5]}' avg {r[6]}; {r[7]}"])
        print(f"\nReport written to {args.report}")

    if not args.apply:
        print("\nDry run only. Re-run with --apply to write.")
        return

    # ---------------- apply ----------------
    by_db: Dict[Path, List[Tuple[int, int]]] = {}
    for dbp, rid, pid in updates:
        by_db.setdefault(dbp, []).append((rid, pid))
    for dbp in [Path(d) for d in args.db]:
        conn = sqlite3.connect(str(dbp))
        cols = {r[1] for r in conn.execute("PRAGMA table_info(riders)")}
        if "person_id" not in cols:
            conn.execute("ALTER TABLE riders ADD COLUMN person_id INTEGER")
        conn.executemany("UPDATE riders SET person_id=? WHERE id=?", [(pid, rid) for rid, pid in by_db.get(dbp, [])])
        conn.execute("CREATE INDEX IF NOT EXISTS idx_riders_person ON riders(person_id)")
        conn.commit()
        conn.close()
    print(f"\n✅ person_id written for {len(updates)} rider(s).")
    if totals["review"]:
        print(f"⚠️  {totals['review']} rider(s) were NOT linked (see REVIEW above).")


if __name__ == "__main__":
    main()
