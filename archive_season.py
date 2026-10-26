#!/usr/bin/env python3
"""
archive_season.py
-----------------
Snapshot a finished season from the per-category race DBs into the central
rider-history database (wmccl_history.db), giving every rider a permanent
person_id.

Run it from the folder that contains export_rider_pages.py -- it imports
compute_stats() from there so the history figures can never drift from what
the rider pages show (laps count even on a DNF; time and races count real
finishes only; AP rows never count as races).

SAFE BY DEFAULT: a DRY RUN. Nothing is written unless you pass --apply.
Re-running a season replaces that season's season_stats rows (idempotent);
people are reused, not duplicated.

USAGE
  # 1. Dry run -- prints the match report, writes nothing
  archive_season.py --season 2025-26 --history wmccl_history.db \
      --db U8.db U10.db U12.db Youth.db Women.db Masters.db Seniors.db

  # 2. Resolve any REVIEW lines in person_overrides.csv, then
  archive_season.py --season 2025-26 --history wmccl_history.db \
      --db ... --overrides person_overrides.csv --apply

OVERRIDES FILE (csv, header required)
  season,source_db,race_number,decision
  2025-26,U8.db,123,new            <- different person (e.g. twins)
  2025-26,Masters.db,456,person:17 <- this entry IS person 17
  2025-26,Seniors.db,789,skip      <- leave out of the history

Overrides are applied before any automatic matching and are remembered in
the file, so the same question is never asked twice.

Riders with NO result rows at all (registered but never raced) are left out:
they have no history to record.

--exclude-round N  Leave a round out of the career history altogether (not used for
                 2025-26: Round 9 is kept, shown as AP where AP was awarded).
--rounds N       Force the season length used for "Full House". Default: the number of
                 rounds that had a real finisher in any category.
--page-template  Rider-page path stored per row (default
                 '/league_tables/{season}/riders/{race_number}.html'). The pattern is also
                 saved in schema_info as page_template.
--report FILE    Also write the review/flag list to a CSV.
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import rider_history as rh

try:
    from export_rider_pages import compute_stats, load_results_for_rider
    from league_scoring import rider_avg_this_season, AP_MARKER
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "❌ Cannot import export_rider_pages.py -- run this from the scripts "
        f"folder (or add it to PYTHONPATH).\n   ({e})"
    )


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
                key = (row["season"].strip(), Path(row["source_db"].strip()).name,
                       int(row["race_number"]))
            except (KeyError, ValueError):
                raise SystemExit(f"❌ bad overrides row: {row}")
            out[key] = row["decision"].strip().lower()
    return out


def open_working_db(path: Path, apply: bool) -> sqlite3.Connection:
    """Dry run works on an in-memory copy so nothing on disk can change."""
    if apply:
        conn = sqlite3.connect(str(path))
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    mem = sqlite3.connect(":memory:")
    if path.exists():
        src = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        src.backup(mem)
        src.close()
    return mem


def describe_candidates(hc: sqlite3.Connection, ids: List[int]) -> str:
    """e.g. ' -- vs person 49: Sam Wearing, DOB 2019-05-24, BC -, this season U8.db #65'"""
    out = []
    for pid in ids:
        p = hc.execute("SELECT firstname, surname, DOB, BC_number FROM person WHERE person_id=?", (pid,)).fetchone()
        if not p:
            continue
        s = hc.execute("SELECT season, source_db, race_number FROM season_stats WHERE person_id=? "
                       "ORDER BY season DESC LIMIT 1", (pid,)).fetchone()
        where = f", latest {s[0]} {s[1]} #{s[2]}" if s else ""
        out.append(f"person {pid}: {p[0]} {p[1]}, DOB {p[2]}, BC {p[3] if p[3] else '-'}{where}")
    return (" -- vs " + " | ".join(out)) if out else ""


def main():
    ap = argparse.ArgumentParser(description="Archive a season into the central rider-history DB.")
    ap.add_argument("--season", required=True, help="e.g. 2025-26")
    ap.add_argument("--history", required=True, help="history DB path (created if missing with --apply)")
    ap.add_argument("--db", nargs="+", required=True, help="the season's race DBs")
    ap.add_argument("--overrides", help="person_overrides.csv")
    ap.add_argument("--exclude-round", type=int, action="append", default=[], metavar="N",
                    help="leave this round OUT of the history entirely (races, laps, time, medals, "
                         "averages and the Full House count), e.g. 9 for Regional Champs. "
                         "Repeatable. Recorded in schema_info.")
    ap.add_argument("--round-note", action="append", default=[], metavar="N=TEXT",
                    help='note shown with a round, e.g. 9="Regional Champs: average points awarded". Repeatable.')
    ap.add_argument("--ap-only", action="append", default=[], metavar="ROUND:DB",
                    help="a round where this DB is EXPECTED to hold average points only, e.g. "
                         "9:Masters.db for Regional Champs (repeatable). Silences the 'no real "
                         "finishers' warning for that pair.")
    ap.add_argument("--rounds", type=int, help="force season length (rounds) for Full House")
    ap.add_argument("--page-template", default="/league_tables/{season}/riders/{race_number}.html",
                    help="site-absolute path of a rider page; recorded in schema_info so the page generator can read it")
    ap.add_argument("--report", help="write review/flag list to this CSV")
    ap.add_argument("--apply", action="store_true", help="actually write (default: dry run)")
    args = ap.parse_args()

    hist_path = Path(args.history)
    overrides = load_overrides(args.overrides)
    excluded = set(args.exclude_round)
    round_notes = {}
    for item in args.round_note:
        try:
            r, t = item.split("=", 1)
            round_notes[int(r)] = t.strip()
        except ValueError:
            raise SystemExit(f"❌ --round-note expects N=TEXT, got '{item}'")
    ap_only = set()
    for item in args.ap_only:
        try:
            r, d = item.split(":", 1)
            ap_only.add((int(r), Path(d).name))
        except ValueError:
            raise SystemExit(f"❌ --ap-only expects ROUND:DB (e.g. 9:Masters.db), got '{item}'")
    hc = open_working_db(hist_path, args.apply)
    rh.init_schema(hc)

    hc.execute("INSERT OR REPLACE INTO schema_info(key, value) VALUES ('page_template', ?)",
               (args.page_template,))
    hc.execute("INSERT OR REPLACE INTO schema_info(key, value) VALUES (?, ?)",
               (f"excluded_rounds:{args.season}", ",".join(map(str, sorted(excluded)))))

    # Replace this season's rows (idempotent re-run). Remember who each entry was
    # last time so a 'new' override reuses that person instead of creating another.
    prev_map = {(r[0], r[1]): r[2] for r in hc.execute(
        "SELECT source_db, race_number, person_id FROM season_stats WHERE season=?", (args.season,))}
    prev = len(prev_map)
    hc.execute("DELETE FROM season_stats WHERE season=?", (args.season,))
    hc.execute("DELETE FROM round_results WHERE season=?", (args.season,))
    hc.execute("DELETE FROM season_rounds WHERE season=?", (args.season,))
    round_info = {}   # round -> (name, venue, date), first DB that has it wins

    # Season length = rounds actually HELD, judged league-wide: a round counts if ANY
    # category has a real (non-AP) finisher. A round where nobody raced (e.g. cancelled,
    # everyone given average points) therefore doesn't count towards "Full House", and a
    # round missing from just one category's import is caught by the warnings below.
    held_rounds = set()
    all_rounds = set()
    for f in args.db:
        if not Path(f).exists():
            raise SystemExit(f"❌ DB not found: {f}")
        t = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        held_rounds |= {r[0] for r in t.execute(
            "SELECT DISTINCT round FROM results WHERE status='FIN' AND is_ap=0 AND points<>999")}
        all_rounds |= {r[0] for r in t.execute("SELECT DISTINCT round FROM results")}
        t.close()
    held_rounds -= excluded
    all_rounds -= excluded
    league_rounds = args.rounds or len(held_rounds)
    not_held = sorted(all_rounds - held_rounds)
    warnings: List[str] = []

    persons_before = hc.execute("SELECT COUNT(*) FROM person").fetchone()[0]
    counts = {"auto": 0, "new": 0, "override": 0, "review": 0, "skipped_override": 0,
              "never_raced": 0, "flags": 0}
    report: List[List] = []   # [kind, source_db, race_number, name, dob, bc, detail]
    per_db: List[Tuple] = []
    used_overrides = set()

    for db_file in args.db:
        dbp = Path(db_file)
        if not dbp.exists():
            raise SystemExit(f"❌ DB not found: {dbp}")
        rc = sqlite3.connect(f"file:{dbp}?mode=ro", uri=True)
        rc.row_factory = None
        try:
            for rn, nm, vn, dt in rc.execute("SELECT round_number, name, venue, date FROM rounds"):
                round_info.setdefault(rn, (nm, vn, dt))
        except sqlite3.Error:
            pass
        # rounds this category is expected to RACE: a round it is AP-only for (--ap-only) isn't one
        season_len = args.rounds or len(held_rounds - {r for (r, d) in ap_only if d == dbp.name})
        max_round = rc.execute("SELECT COALESCE(MAX(round),0) FROM results").fetchone()[0]
        real_by_round = dict(rc.execute(
            "SELECT round, COUNT(*) FROM results WHERE status='FIN' AND is_ap=0 AND points<>999 GROUP BY round"))
        empty = sorted(r for r in held_rounds if not real_by_round.get(r) and (r, dbp.name) not in ap_only)
        if empty:
            warnings.append(f"{dbp.name}: no real finishers in round(s) {', '.join(map(str, empty))}, "
                            f"although other categories raced -- results not imported? Totals, medals "
                            f"and Full House will be wrong until fixed.")

        riders = rc.execute("""
            SELECT id, race_number, BC_number, firstname, surname, gender,
                   race_category, race_category_previous_year,
                   average_points_last_year, DOB
            FROM riders ORDER BY race_number
        """).fetchall()

        n_rows = 0
        tot_races = tot_laps = 0
        tot_time = 0.0

        for (rid, race_no, bc_raw, first, sur, gender, cat, cat_prev, avg_ly, dob_raw) in riders:
            if race_no is None:
                continue
            history_all = load_results_for_rider(rc, rid, max(max_round, season_len, *excluded or [0]))
            # AP rows never carry laps or time, even if the source row happens to hold some
            history_all = [(r, cp, op, pt, ap_, st_, None, None) if (ap_ or pt == AP_MARKER) else
                           (r, cp, op, pt, ap_, st_, lp, tm)
                           for (r, cp, op, pt, ap_, st_, lp, tm) in history_all]
            history = [h for h in history_all if h[0] not in excluded]
            # someone with no real finish at all (never raced / only ever AP) has no history
            if not any(h[5] == "FIN" and not h[4] and h[3] != AP_MARKER for h in history):
                counts["never_raced"] += 1
                continue

            name = f"{rh.norm(first)} {rh.norm(sur)}".strip()
            dob_iso = rh.norm_dob(dob_raw)
            bc = rh.safe_int(bc_raw)
            key = (args.season, dbp.name, int(race_no))

            # -- decide the person ------------------------------------------------
            person_id: Optional[int] = None
            decision = overrides.get(key)
            if decision is not None:
                used_overrides.add(key)
                if decision == "skip":
                    counts["skipped_override"] += 1
                    continue
                if decision == "new":
                    person_id = prev_map.get((dbp.name, int(race_no)))
                    kind = "override-new"
                elif decision.startswith("person:") or decision.isdigit():
                    pid = int(decision.split(":")[-1])
                    if not hc.execute("SELECT 1 FROM person WHERE person_id=?", (pid,)).fetchone():
                        raise SystemExit(f"❌ override {key}: person_id {pid} does not exist")
                    person_id = pid
                    kind = "override-link"
                else:
                    raise SystemExit(f"❌ override {key}: unknown decision '{decision}'")
                counts["override"] += 1
            elif (pp := prev_map.get((dbp.name, int(race_no)))) is not None and dob_iso and \
                    (hc.execute("SELECT DOB FROM person WHERE person_id=?", (pp,)).fetchone() or (None,))[0] == dob_iso:
                # Re-run of the same season: this entry (same DB + race number) was this person last
                # time and the DOB still agrees -- keep the id, and pick up any corrected name.
                person_id = pp
                kind = "continuity"
                hc.execute("UPDATE person SET firstname=?, surname=? WHERE person_id=?",
                           (rh.norm(first), rh.norm(sur), pp))
                counts["auto"] += 1
            else:
                m = rh.match_rider(hc, first, sur, dob_iso, bc, gender)
                if m.kind == rh.REVIEW:
                    counts["review"] += 1
                    report.append(["REVIEW", dbp.name, race_no, name, dob_iso, bc,
                                   m.reason + describe_candidates(hc, m.candidates)])
                    continue
                if m.kind == rh.AUTO:
                    # same person twice in ONE season is never auto-linked
                    dup = hc.execute(
                        "SELECT source_db, race_number FROM season_stats WHERE person_id=? AND season=?",
                        (m.person_id, args.season)).fetchone()
                    if dup:
                        counts["review"] += 1
                        report.append(["REVIEW", dbp.name, race_no, name, dob_iso, bc,
                                       f"already in this season as {dup[0]} #{dup[1]} "
                                       f"(person_id {m.person_id}) -- duplicate account / two numbers?"])
                        continue
                    person_id = m.person_id
                    counts["auto"] += 1
                    for fl in m.flags:
                        counts["flags"] += 1
                        report.append(["FLAG", dbp.name, race_no, name, dob_iso, bc, fl])
                    kind = "auto"
                else:
                    kind = "new"

            if person_id is None:
                cur = hc.execute(
                    "INSERT INTO person(firstname, surname, DOB, gender, BC_number, first_season) "
                    "VALUES (?,?,?,?,?,?)",
                    (rh.norm(first), rh.norm(sur), dob_iso, rh.norm(gender), bc, args.season))
                person_id = cur.lastrowid
                if kind == "new":
                    counts["new"] += 1
            else:
                if bc is not None:
                    hc.execute("UPDATE person SET BC_number=? WHERE person_id=?", (bc, person_id))
                hc.execute("UPDATE person SET gender=? WHERE person_id=? AND (gender IS NULL OR gender='')",
                           (rh.norm(gender), person_id))

            # -- stats (same function the rider pages use) ----------------------------
            st = compute_stats(history, {}, season_length=season_len,
                               race_category=cat, race_category_previous_year=cat_prev,
                               average_points_last_year=avg_ly)
            tiers = list(st["medal_rounds"].values())
            # AP value exactly as the league tables compute it at season end
            pts_map = {(rid, h[0]): (h[3], h[4]) for h in history}
            ap_value = rider_avg_this_season(rid, pts_map, max_round)
            ap_rounds = sum(1 for h in history if h[4] or h[3] == AP_MARKER)
            hc.execute("""
                INSERT INTO season_stats(person_id, season, source_db, race_number, race_category,
                    race_category_previous_year, BC_number_entered, rounds_in_season,
                    races_completed, laps, time_seconds, avg_points, best_points,
                    best_cat_position, golds, silvers, bronzes, full_house, page_path,
                    ap_rounds, ap_value)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (person_id, args.season, dbp.name, int(race_no), cat, cat_prev, bc, season_len,
                  st["races_completed"], st["total_laps"], st["total_time_seconds"],
                  st["avg_points"], st["best_points"], st["best_cat_position"],
                  tiers.count(1), tiers.count(2), tiers.count(3),
                  1 if st["milestone_badge"] == "Full House" else 0,
                  args.page_template.format(season=args.season, race_number=int(race_no)),
                  ap_rounds, ap_value))
            for (rnd, cpos, opos, pts, is_ap, status, laps_, time_) in history_all:
                hc.execute("""INSERT INTO round_results(season, source_db, race_number, round, status, is_ap,
                                  points, effective_points, cat_position, overall_position, laps,
                                  time_seconds, counts_for_career)
                              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (args.season, dbp.name, int(race_no), rnd, status,
                            1 if (is_ap or pts == AP_MARKER) else 0, pts,
                            ap_value if (is_ap or pts == AP_MARKER) else pts,
                            cpos, opos, laps_, time_, 0 if rnd in excluded else 1))
            n_rows += 1
            tot_races += st["races_completed"]
            tot_laps += st["total_laps"]
            tot_time += st["total_time_seconds"]

        per_db.append((dbp.name, season_len, len(riders), n_rows, tot_races, tot_laps, tot_time / 3600))
        rc.close()

    for rn in sorted(set(round_info) | set(range(1, max(all_rounds | excluded | {0}) + 1))):
        nm, vn, dt = round_info.get(rn, (None, None, None))
        hc.execute("INSERT OR REPLACE INTO season_rounds(season, round, name, venue, date, counts_for_career, note) "
                   "VALUES (?,?,?,?,?,?,?)",
                   (args.season, rn, nm, vn, dt, 0 if rn in excluded else 1, round_notes.get(rn)))

    # unused overrides for this season = probably a typo'd race number
    stale = [k for k in overrides if k[0] == args.season and k not in used_overrides]

    # People left with no season rows (e.g. only rode an excluded round) carry no history
    cur = hc.execute("DELETE FROM person WHERE NOT EXISTS "
                     "(SELECT 1 FROM season_stats s WHERE s.person_id=person.person_id)")
    orphans = cur.rowcount
    persons_after = hc.execute("SELECT COUNT(*) FROM person").fetchone()[0]

    # -- report -----------------------------------------------------------------
    mode = "APPLY" if args.apply else "DRY RUN (nothing written)"
    print(f"\n=== archive_season {args.season} -- {mode} ===")
    if prev:
        print(f"Replacing {prev} existing season_stats rows for {args.season}.")
    print(f"\n{'DB':<12}{'rounds':>7}{'riders':>8}{'archived':>10}{'races':>8}{'laps':>8}{'hours':>8}")
    for name, sl, nr, na, races, laps, hrs in per_db:
        print(f"{name:<12}{sl:>7}{nr:>8}{na:>10}{races:>8}{laps:>8}{hrs:>8.1f}")
    print(f"\nPeople: {persons_before} -> {persons_after} "
          f"(new this run: {counts['new']}, auto-matched: {counts['auto']}, "
          f"by override: {counts['override']})")
    print(f"Registered but never raced (left out): {counts['never_raced']}")
    if orphans:
        print(f"People removed because they have no season rows left: {orphans}")

    if excluded:
        print(f"\nRounds LEFT OUT of the history (by --exclude-round): {', '.join(map(str, sorted(excluded)))}")
    print(f"\nSeason length for Full House: {league_rounds} round(s) held "
          f"(rounds {', '.join(map(str, sorted(held_rounds)))})")
    if not_held:
        print(f"Rounds with NO real finishers in any category (cancelled? everyone on average points): "
              f"{', '.join(map(str, not_held))}")
    if warnings:
        print("\n⚠️  DATA WARNINGS")
        for w in warnings:
            print(f"  {w}")

    reviews = [r for r in report if r[0] == "REVIEW"]
    flags = [r for r in report if r[0] == "FLAG"]
    if flags:
        print(f"\n-- {len(flags)} FLAG(S): matched automatically, but worth a glance --")
        for r in flags:
            print(f"  {r[1]} #{r[2]} {r[3]} (DOB {r[4]}): {r[6]}")
    if reviews:
        print(f"\n-- {len(reviews)} REVIEW: HELD OUT of the history until resolved --")
        for r in reviews:
            print(f"  {r[1]} #{r[2]} {r[3]} (DOB {r[4]}, BC {r[5]}): {r[6]}")
        print("  Resolve each in the overrides file (new / person:<id> / skip) and re-run.")
    if stale:
        print(f"\n-- {len(stale)} override(s) matched no rider (typo in race number?) --")
        for k in stale:
            print(f"  {k}")
    if not (flags or reviews):
        print("\nNo flags or review cases.")

    if args.report and report:
        with open(args.report, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["kind", "source_db", "race_number", "name", "dob", "bc", "detail"])
            w.writerows(report)
        print(f"\nReport written to {args.report}")

    if args.apply:
        hc.commit()
        print(f"\n✅ Written to {hist_path}")
        if reviews:
            print(f"⚠️  {len(reviews)} rider(s) were NOT archived (see REVIEW above).")
    else:
        hc.rollback()
        print("\nDry run only. Re-run with --apply to write.")
    hc.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
