#!/usr/bin/env python3
"""
most_improved_rider.py
----------------------
Calculate the Most Improved Rider Award for a CX league season.

The award is calculated per eligible database (category group).
Eligible categories: U12, Youth, Junior/Senior, Vets (Masters).

Rules:
  - Only league riders (race_number < 900) are eligible.
  - Riders must have completed >= 2/3 of all valid league rounds.
  - Riders must have >= 2 finishes in the early-season window (first 4 valid rounds).
  - Riders must have >= 2 finishes in the late-season window (last 4 valid rounds).
  - Percentile score per finish: 1.0 = winner, 0.0 = last place, scaled by field size.
    Percentile is calculated within the rider's race_category AND gender group per round.
  - Improvement Score = late-season avg percentile − early-season avg percentile.
  - Ties broken by: (1) higher late-season avg, (2) more total finishes,
    (3) highest single late-season percentile.

Usage:
  python3 most_improved_rider.py --db U12.db --db Youth.db --db Seniors.db --db Masters.db
  python3 most_improved_rider.py --db Seniors.db --max-rounds 12 --early-window 4 --late-window 4
  python3 most_improved_rider.py --db U12.db --csv most_improved_u12.csv
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from collections import defaultdict


NONLEAGUE_THRESHOLD = 900
DEFAULT_EARLY_WINDOW = 4
DEFAULT_LATE_WINDOW  = 4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_valid_rounds(conn):
    """
    Return a sorted list of round numbers that have at least one FIN result.
    These are the 'valid' league rounds (cancelled/empty rounds are excluded).
    """
    cur = conn.execute("""
        SELECT DISTINCT round
        FROM results
        WHERE status = 'FIN'
        ORDER BY round
    """)
    return [row[0] for row in cur.fetchall()]


def get_league_riders(conn):
    """
    Return all league riders as a dict: rider_id -> {firstname, surname, race_category, gender, race_number}
    League riders have race_number < NONLEAGUE_THRESHOLD.
    """
    cur = conn.execute("""
        SELECT id, race_number, firstname, surname, race_category, gender
        FROM riders
        WHERE race_number < ?
    """, (NONLEAGUE_THRESHOLD,))
    riders = {}
    for row in cur.fetchall():
        riders[row[0]] = {
            "id": row[0],
            "race_number": row[1],
            "firstname":   row[2] or "",
            "surname":     row[3] or "",
            "race_category": row[4] or "?",
            "gender":      row[5] or "?",
        }
    return riders


def get_all_finishes(conn, rider_ids):
    """
    Return all FIN results for the given rider_ids.
    Returns dict: rider_id -> list of {round, cat_position, overall_position, points}
    """
    if not rider_ids:
        return {}
    placeholders = ",".join("?" * len(rider_ids))
    cur = conn.execute(f"""
        SELECT rider_id, round, cat_position, overall_position, points
        FROM results
        WHERE status = 'FIN'
          AND rider_id IN ({placeholders})
        ORDER BY rider_id, round
    """, list(rider_ids))
    finishes = defaultdict(list)
    for row in cur.fetchall():
        finishes[row[0]].append({
            "round":            row[1],
            "cat_position":     row[2],
            "overall_position": row[3],
            "points":           row[4],
        })
    return finishes


def compute_field_sizes(conn, valid_rounds):
    """
    For each (round, race_category, gender) group compute the field size.

    Women's single-table races store cat_position as the rider's position
    among ALL female finishers, not per sub-category. We detect this when a
    rider's cat_position exceeds the per-category count, so we also pre-compute
    a per-(round, gender) total that the percentile logic can fall back to.

    Returns two dicts:
      by_cat    : (round, race_category, gender) -> count   [primary]
      by_gender : (round, gender)                -> count   [fallback for single-table F]
    """
    placeholders = ",".join("?" * len(valid_rounds))

    cur = conn.execute(f"""
        SELECT res.round, r.race_category, r.gender, COUNT(*) AS cnt
        FROM results res
        JOIN riders r ON r.id = res.rider_id
        WHERE res.status = 'FIN'
          AND r.race_number < {NONLEAGUE_THRESHOLD}
          AND res.round IN ({placeholders})
        GROUP BY res.round, r.race_category, r.gender
    """, valid_rounds)
    by_cat = {(row[0], row[1], row[2]): row[3] for row in cur.fetchall()}

    cur2 = conn.execute(f"""
        SELECT res.round, r.gender, COUNT(*) AS cnt
        FROM results res
        JOIN riders r ON r.id = res.rider_id
        WHERE res.status = 'FIN'
          AND r.race_number < {NONLEAGUE_THRESHOLD}
          AND res.round IN ({placeholders})
        GROUP BY res.round, r.gender
    """, valid_rounds)
    by_gender = {(row[0], row[1]): row[2] for row in cur2.fetchall()}

    return by_cat, by_gender


def percentile_score(cat_position, field_size):
    """
    Map a cat_position to a percentile [0.0, 1.0].
    1st place -> 1.0, last place -> 0.0.
    If field_size == 1, the sole finisher scores 1.0.
    """
    if field_size is None or field_size <= 0:
        return None
    if field_size == 1:
        return 1.0
    return 1.0 - (cat_position - 1) / (field_size - 1)


def linear_regression_slope(xs, ys):
    """
    Compute the slope of the least-squares regression line through (xs, ys).
    xs = round numbers (x-axis), ys = percentile scores (y-axis).
    Returns the slope (percentile gain per round) or None if fewer than 3 points.
    A positive slope means the rider improved over time.
    """
    n = len(xs)
    if n < 3:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    numerator   = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return None
    return numerator / denominator


# ---------------------------------------------------------------------------
# Main calculation
# ---------------------------------------------------------------------------

def calculate_most_improved(conn, db_label, max_rounds,
                             early_window_size, late_window_size,
                             min_threshold_fraction=2/3,
                             single_table=False,
                             min_window_finishes=3,
                             min_improvement=0.0,
                             min_early_avg=0.0):
    """
    Run the Most Improved calculation for one database.

    single_table=True  : women's single-table mode — percentile is based on
                         overall_position within the full gender group, not
                         cat_position within the sub-category. Use this for
                         the Women's DB where all females race as one table.

    Returns a list of result dicts, sorted by improvement score descending.
    """

    import math

    valid_rounds = get_valid_rounds(conn)
    total_valid  = len(valid_rounds)

    if total_valid == 0:
        print(f"  [{db_label}] No valid rounds found — skipping.")
        return []

    # Windows are fixed by round NUMBER, not list position.
    # Early = rounds 1..early_window_size  (e.g. 1-4)
    # Late  = rounds (max_rounds - late_window_size + 1)..max_rounds  (e.g. 9-12)
    early_rounds = set(range(1, early_window_size + 1))
    late_start   = max_rounds - late_window_size + 1
    late_rounds  = set(range(late_start, max_rounds + 1))

    # Only count valid (run) rounds for participation threshold
    min_finishes_total = math.ceil(total_valid * min_threshold_fraction)

    league_riders = get_league_riders(conn)
    if not league_riders:
        print(f"  [{db_label}] No league riders found — skipping.")
        return []

    all_finishes          = get_all_finishes(conn, list(league_riders.keys()))
    field_by_cat, field_by_gender = compute_field_sizes(conn, valid_rounds)

    def get_position_and_field(rnd, cat, gender, cat_position, overall_position):
        """
        Return (position, field_size) to use for percentile calculation.

        single_table mode  → use overall_position vs full gender field.
        normal mode        → use cat_position vs per-category field, with a
                             safety fallback to gender-wide field if cat_position
                             would exceed the category count (prevents negatives).
        """
        if single_table:
            pos = overall_position
            fs  = field_by_gender.get((rnd, gender))
        else:
            pos    = cat_position
            fs_cat = field_by_cat.get((rnd, cat, gender))
            if fs_cat is None:
                fs = field_by_gender.get((rnd, gender))
            elif cat_position is not None and cat_position > fs_cat:
                fs = field_by_gender.get((rnd, gender), fs_cat)
            else:
                fs = fs_cat
        return pos, fs

    results = []

    for rider_id, rider in league_riders.items():
        cat      = rider["race_category"]
        gender   = rider["gender"]
        finishes = all_finishes.get(rider_id, [])

        # Only count finishes in valid rounds
        valid_finishes = [f for f in finishes if f["round"] in set(valid_rounds)]

        # Participation check
        if len(valid_finishes) < min_finishes_total:
            continue

        # Window finishes
        early_finishes = [f for f in valid_finishes if f["round"] in early_rounds]
        late_finishes  = [f for f in valid_finishes if f["round"] in late_rounds]

        if len(early_finishes) < min_window_finishes or len(late_finishes) < min_window_finishes:
            continue

        # Build per-round score detail for ALL valid finishes (used for display + averages)
        round_detail = {}
        for f in valid_finishes:
            pos, fs = get_position_and_field(
                f["round"], cat, gender, f["cat_position"], f["overall_position"]
            )
            pct = percentile_score(pos, fs) if (fs and pos is not None) else None
            window = ""
            if f["round"] in early_rounds:
                window = "E"
            if f["round"] in late_rounds:
                window = window + "L"
            round_detail[f["round"]] = {
                "cat_position":     f["cat_position"],
                "overall_position": f["overall_position"],
                "position_used":    pos,        # what was actually scored
                "field_size":       fs,
                "points":           f.get("points"),
                "percentile":       pct,
                "window":           window,
            }

        def avg_percentile_from_detail(window_flag):
            scores = [
                d["percentile"]
                for d in round_detail.values()
                if window_flag in d["window"] and d["percentile"] is not None
            ]
            return (sum(scores) / len(scores)) if scores else None

        early_avg = avg_percentile_from_detail("E")
        late_avg  = avg_percentile_from_detail("L")

        if early_avg is None or late_avg is None:
            continue

        if early_avg < min_early_avg:
            continue

        improvement = late_avg - early_avg

        # Regression slope across all valid finishes
        reg_xs = sorted(round_detail.keys())
        reg_ys = [round_detail[rnd]["percentile"] for rnd in reg_xs
                  if round_detail[rnd]["percentile"] is not None]
        reg_xs = [rnd for rnd in reg_xs
                  if round_detail[rnd]["percentile"] is not None]
        slope = linear_regression_slope(reg_xs, reg_ys)

        # Predicted improvement across the full season span using slope
        # (slope * (last_round - first_round)) gives total percentile gain)
        if slope is not None and len(reg_xs) >= 2:
            season_span  = reg_xs[-1] - reg_xs[0]
            slope_improvement = slope * season_span
        else:
            slope_improvement = None

        # Highest single late-season percentile (for tie-break)
        late_percentiles = [
            d["percentile"]
            for d in round_detail.values()
            if "L" in d["window"] and d["percentile"] is not None
        ]
        best_late_pct = max(late_percentiles) if late_percentiles else 0.0

        results.append({
            "db_label":       db_label,
            "rider_id":       rider_id,
            "race_number":    rider["race_number"],
            "name":           f"{rider['firstname']} {rider['surname']}".strip(),
            "race_category":  cat,
            "gender":         gender,
            "total_finishes": len(valid_finishes),
            "early_finishes": len(early_finishes),
            "late_finishes":  len(late_finishes),
            "early_avg_pct":  early_avg,
            "late_avg_pct":   late_avg,
            "improvement":    improvement,
            "best_late_pct":  best_late_pct,
            "round_detail":       round_detail,
            "valid_rounds":       valid_rounds,
            "single_table":       single_table,
            "slope":              slope,               # percentile gain per round number
            "slope_improvement":  slope_improvement,   # slope * season span
        })

    if not results:
        print(f"  [{db_label}] No eligible riders found (check participation, window, and baseline thresholds).")
        return []

    # Sort: improvement desc, then late_avg desc, then total_finishes desc, then best_late_pct desc
    results.sort(key=lambda r: (
        -r["improvement"],
        -r["late_avg_pct"],
        -r["total_finishes"],
        -r["best_late_pct"],
    ))

    # Mark each rider as qualifying (improvement >= threshold) or not
    for r in results:
        r["qualifies"] = r["improvement"] >= min_improvement

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_round_breakdown(r, valid_rounds, label="", single_table=False):
    """Print a per-round breakdown table for one rider."""
    if label:
        print(f"  {label}")
    pos_label = "OvPos" if single_table else "CatPos"
    header = f"  {'Rd':<4} {'Window':<7} {pos_label:<7} {'Field':<6} {'Pts':<6} {'Pct%':<8}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    rd = r.get("round_detail", {})
    for rnd in valid_rounds:
        if rnd not in rd:
            continue
        d = rd[rnd]
        win  = d["window"] or "-"
        pos  = str(d["position_used"]) if d.get("position_used") is not None else "-"
        fld  = str(d["field_size"])    if d["field_size"]   is not None else "-"
        pts  = str(d["points"])        if d["points"]       is not None else "-"
        pct  = f"{d['percentile']*100:.1f}%" if d["percentile"] is not None else "-"
        marker = " ◀ early" if win == "E" else (" ◀ late" if win == "L" else "")
        print(f"  {rnd:<4} {win:<7} {pos:<7} {fld:<6} {pts:<6} {pct:<8}{marker}")
    print()


def print_results(db_label, results, top_n=10, detail_top_n=3, min_season_gain=0.05):
    """
    Print results with regression as primary method, window as secondary check.
    min_season_gain : minimum slope_improvement to count as a winner (default 5%).
    """
    W = 72
    print(f"\n{'='*W}")
    print(f"  Most Improved Rider — {db_label}")
    print(f"{'='*W}")
    if not results:
        print("  No eligible riders.")
        return

    valid_rounds = results[0].get("valid_rounds", [])

    # ── Regression ranking (PRIMARY) ──────────────────────────────────────────
    reg_results = sorted(
        [r for r in results if r.get("slope_improvement") is not None],
        key=lambda r: (-r["slope_improvement"], -r["late_avg_pct"], -r["total_finishes"])
    )
    reg_winner = (reg_results[0]
                  if reg_results and reg_results[0]["slope_improvement"] >= min_season_gain
                  else None)

    print(f"  PRIMARY — Improvement trend across all rounds")
    reg_header = (f"  {'Rank':<5} {'Name':<25} {'Cat':<6} {'Gen':<4} "
                  f"{'Fin':<5} {'Slope/Rd':<10} {'SeasonGain':<12} {'AvgPct'}")
    print(reg_header)
    print("  " + "-" * (len(reg_header) - 2))
    for i, r in enumerate(reg_results[:top_n], start=1):
        si  = r["slope_improvement"]
        sl  = r["slope"]
        pcts = [d["percentile"] for d in r["round_detail"].values()
                if d["percentile"] is not None]
        avg = sum(pcts) / len(pcts) if pcts else 0
        qualmark = " 📈" if (i == 1 and reg_winner) else (" ✗" if si < min_season_gain else "")
        print(f"  {i:<5} {r['name']:<25} {r['race_category']:<6} {r['gender']:<4} "
              f"{r['total_finishes']:<5} {sl*100:>+8.3f}%   {si*100:>+8.2f}%"
              f"     {avg*100:.1f}%{qualmark}")
    print()

    if reg_winner:
        rw = reg_winner
        print(f"  📈 WINNER: {rw['name']}  "
              f"(Cat: {rw['race_category']} / {rw['gender']})")
        print(f"     Season gain : {rw['slope_improvement']*100:+.1f}%  "
              f"({rw['slope']*100:+.3f}% per round)")
    else:
        best = reg_results[0] if reg_results else None
        print(f"  ⚠️  NO AWARD — no rider showed sufficient improvement.")
        if best:
            print(f"     Best season gain was {best['slope_improvement']*100:+.1f}% "
                  f"({best['name']}) — below the {min_season_gain*100:.0f}% threshold.")
    print()

    # ── Window method (SECONDARY CHECK) ──────────────────────────────────────
    qualifiers     = [r for r in results if r.get("qualifies", True)]
    non_qualifiers = [r for r in results if not r.get("qualifies", True)]

    print(f"  SECONDARY CHECK — Early/late window comparison")
    win_header = (f"  {'Rank':<5} {'Name':<25} {'Cat':<6} {'Gen':<4} "
                  f"{'Fin':<5} {'Early%':<8} {'Late%':<8} {'Improve':<10} {'BestLate%'}")
    print(win_header)
    print("  " + "-" * (len(win_header) - 2))
    if qualifiers:
        for i, r in enumerate(qualifiers[:top_n], start=1):
            flag = " ✓" if i == 1 else ""
            print(f"  {i:<5} {r['name']:<25} {r['race_category']:<6} {r['gender']:<4} "
                  f"{r['total_finishes']:<5} {r['early_avg_pct']*100:<8.1f} "
                  f"{r['late_avg_pct']*100:<8.1f} {r['improvement']*100:<+10.1f} "
                  f"{r['best_late_pct']*100:.1f}%{flag}")
    else:
        print("  (no qualifying riders)")

    if non_qualifiers:
        shown = non_qualifiers[:max(0, top_n - len(qualifiers))]
        if shown:
            print(f"  --- declined ---")
            for r in shown:
                print(f"  {'':5} {r['name']:<25} {r['race_category']:<6} {r['gender']:<4} "
                      f"{r['total_finishes']:<5} {r['early_avg_pct']*100:<8.1f} "
                      f"{r['late_avg_pct']*100:<8.1f} {r['improvement']*100:<+10.1f} "
                      f"{r['best_late_pct']*100:.1f}%")
    print()

    # ── Agreement check ───────────────────────────────────────────────────────
    win_winner = qualifiers[0] if qualifiers else None
    if reg_winner and win_winner:
        if reg_winner["name"] == win_winner["name"]:
            print(f"  ✅  Both methods agree: {reg_winner['name']} wins.")
        else:
            print(f"  ⚠️  Methods disagree — regression: {reg_winner['name']}, "
                  f"window: {win_winner['name']}. Review breakdowns.")
    elif reg_winner and not win_winner:
        print(f"  ℹ️  Regression finds a winner ({reg_winner['name']}) "
              f"but window method does not. Review breakdown.")
    elif win_winner and not reg_winner:
        print(f"  ℹ️  Window method finds a winner ({win_winner['name']}) "
              f"but regression gain is below threshold. Review breakdown.")
    else:
        print(f"  ⛔  No award this season — neither method finds a qualifying winner.")
    print()

    # ── Per-round breakdowns for top regression riders ────────────────────────
    if reg_winner:
        show_n = min(detail_top_n, len(reg_results))
        print(f"  --- Round-by-round detail (top {show_n} by trend) ---")
        for i, r in enumerate(reg_results[:detail_top_n], start=1):
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"#{i}")
            si = r["slope_improvement"]
            sl = r["slope"]
            print(f"  {medal} {r['name']}  (Cat: {r['race_category']} / {r['gender']})")
            print(f"     Trend: {sl*100:+.3f}% per round   "
                  f"Season gain: {si*100:+.1f}%   "
                  f"Window improvement: {r['improvement']*100:+.1f}%")
            print_round_breakdown(r, valid_rounds, single_table=r.get('single_table', False))


def write_csv(all_results, out_path, max_rounds=12):
    """Write results to CSV with per-round position, points and percentile columns."""
    # Gather the actual rounds used across all results
    all_rounds = set()
    for res_list in all_results:
        for r in res_list:
            all_rounds.update(r.get("valid_rounds", []))
    rounds_sorted = sorted(all_rounds)

    base_fields = [
        "db_label", "rank", "name", "race_number", "race_category", "gender",
        "total_finishes", "early_finishes", "late_finishes",
        "early_avg_pct", "late_avg_pct", "improvement", "best_late_pct",
    ]
    round_fields = []
    for rnd in rounds_sorted:
        round_fields += [f"r{rnd}_window", f"r{rnd}_pos", f"r{rnd}_field",
                         f"r{rnd}_pts", f"r{rnd}_pct"]

    fieldnames = base_fields + round_fields

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for res_list in all_results:
            for rank, r in enumerate(res_list, start=1):
                row = {
                    "db_label":       r["db_label"],
                    "rank":           rank,
                    "name":           r["name"],
                    "race_number":    r["race_number"],
                    "race_category":  r["race_category"],
                    "gender":         r["gender"],
                    "total_finishes": r["total_finishes"],
                    "early_finishes": r["early_finishes"],
                    "late_finishes":  r["late_finishes"],
                    "early_avg_pct":  f"{r['early_avg_pct']:.6f}",
                    "late_avg_pct":   f"{r['late_avg_pct']:.6f}",
                    "improvement":    f"{r['improvement']:.6f}",
                    "best_late_pct":  f"{r['best_late_pct']:.6f}",
                }
                rd = r.get("round_detail", {})
                for rnd in rounds_sorted:
                    d = rd.get(rnd, {})
                    row[f"r{rnd}_window"] = d.get("window", "")
                    row[f"r{rnd}_pos"]    = d.get("cat_position", "")
                    row[f"r{rnd}_field"]  = d.get("field_size", "")
                    row[f"r{rnd}_pts"]    = d.get("points", "")
                    pct = d.get("percentile")
                    row[f"r{rnd}_pct"]    = f"{pct*100:.2f}" if pct is not None else ""
                writer.writerow(row)
    print(f"\nResults written to: {out_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Calculate the Most Improved Rider Award from CX league databases."
    )
    ap.add_argument("--db", action="append", required=True, metavar="DB",
                    help="SQLite DB file (repeat for multiple, e.g. --db U12.db --db Youth.db)")
    ap.add_argument("--max-rounds", type=int, default=12,
                    help="Max rounds in the season (default 12, used for information only)")
    ap.add_argument("--early-window", type=int, default=DEFAULT_EARLY_WINDOW,
                    help=f"Number of early-season rounds (default {DEFAULT_EARLY_WINDOW})")
    ap.add_argument("--late-window", type=int, default=DEFAULT_LATE_WINDOW,
                    help=f"Number of late-season rounds (default {DEFAULT_LATE_WINDOW})")
    ap.add_argument("--top", type=int, default=10,
                    help="How many riders to show in the printed leaderboard (default 10)")
    ap.add_argument("--detail", type=int, default=3,
                    help="How many top riders to show round-by-round breakdown for (default 3)")
    ap.add_argument("--min-improvement", type=float, default=0.0,
                    help="Minimum window improvement score to qualify (default 0.0)")
    ap.add_argument("--min-season-gain", type=float, default=0.05,
                    help="Minimum regression season gain to qualify (default 0.05 = 5%%)")
    ap.add_argument("--min-early-avg", type=float, default=0.0,
                    help="Minimum early-window average percentile to be eligible (default 0.0). "
                         "E.g. 0.30 excludes riders who averaged below 30%% in the early window.")
    ap.add_argument("--min-window-finishes", type=int, default=3,
                    help="Minimum finishes required in each window (default 3)")
    ap.add_argument("--single-table", action="store_true",
                    help="Use overall position vs full gender field (for Women's DB where all females race together)")
    ap.add_argument("--csv", metavar="FILE",
                    help="Optional path to write combined CSV output")
    args = ap.parse_args()

    all_results = []

    for db_file in args.db:
        db_path = Path(db_file)
        if not db_path.exists():
            print(f"⚠️  DB not found: {db_path} — skipping.")
            continue

        db_label = db_path.stem
        print(f"\nProcessing: {db_path}")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        results = calculate_most_improved(
            conn,
            db_label=db_label,
            max_rounds=args.max_rounds,
            early_window_size=args.early_window,
            late_window_size=args.late_window,
            single_table=args.single_table,
            min_window_finishes=args.min_window_finishes,
            min_improvement=args.min_improvement,
            min_early_avg=args.min_early_avg,
        )
        conn.close()

        print_results(db_label, results, top_n=args.top, detail_top_n=args.detail,
                      min_season_gain=args.min_season_gain)
        if results:
            all_results.append(results)

    if args.csv and all_results:
        write_csv(all_results, args.csv)

    print("\nDone.")


if __name__ == "__main__":
    main()
