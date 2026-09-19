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
  - Per-round score (see SCORING below) is averaged over each window.
  - Improvement Score = late-season avg score − early-season avg score.
  - Ties broken by: (1) higher late-season avg, (2) more total finishes,
    (3) highest single late-season score.

SCORING (2026-09-19 — switched from rank-percentile to Pace Grade, now
that results carry laps_completed/finish_time_seconds):

  Originally every round was scored purely by finishing RANK: 1st in the
  category = 1.0, last = 0.0, scaled by how many riders were in the
  field that round (percentile_score()). That's still available
  (--score-basis percentile) and is still used as an automatic fallback
  for any round where time data isn't there to grade with, but it's no
  longer the default because it has two real accuracy problems once you
  have actual pace to compare against:

    1. It's sensitive to who else showed up, not just to the rider's
       own pace. If the strong riders in a category stop attending
       late-season (illness, other commitments, end of a short-course
       category's age window), a rider whose own pace hasn't changed AT
       ALL can look like they've gotten dramatically better OR worse
       just because the field shrank or changed shape around them —
       nothing about their actual riding changed.
    2. It can't tell margin from meaningless order. Moving from 9th to
       6th because two riders slower than you retired, versus moving
       from 9th to 6th because you closed a two-minute gap, score
       identically under rank. A rider padded by a couple of slow new
       arrivals at the back of the field can look like "most improved"
       for barely any real gain in pace at all.

  Now the default (--score-basis auto) scores each round with PACE GRADE
  instead — the rider's pace (seconds/lap) that round compared to the
  fastest pace in their own PACE GROUP that same round (leader = 100%),
  exactly the metric already shown on rider pages and explained in
  pace-grade-explainer.html. It's computed via pace_grade_scoring.py —
  the SAME shared module export_rider_pages.py uses — so this award can
  never disagree with what a rider sees on their own page, and it
  automatically gets the same pace-group handling (Seniors/Masters/Women
  category merging, gender splits, etc.) for free rather than
  re-deriving field-size groupings by hand as the old percentile code
  did. Because it's a continuous measure of actual speed relative to
  that day's fastest rider, it isn't distorted by field size or
  composition the way rank is — a rider who hasn't gotten any faster
  scores the same whether the field is 10 riders or 6.

  Pace Grade needs laps_completed/finish_time_seconds on the result,
  which won't exist for rounds imported before that data started being
  captured. --score-basis auto handles that gracefully: any round with
  no gradeable pace data (older rounds, or a round where nobody in the
  rider's pace group has a leader time — see pace_grade_scoring.py)
  automatically falls back to the old rank-percentile score for THAT
  round only, so a season that's mid-transition to storing race times
  still produces a result rather than losing early rounds outright.
  Each round's round_detail records which method actually scored it
  (see "score_source"), and the printed/CSV output show it, so it's
  never a silent switch.

  Both scores are 0.0-1.0 (percentile always was; Pace Grade is stored
  as a 0-100 percentage internally, same as pace_grade_scoring.py and
  the rider pages, and divided by 100 here purely so the two scales
  line up and existing thresholds like --min-improvement /
  --min-season-gain keep meaning roughly the same size of change).

Usage:
  python3 most_improved_rider.py --db U12.db --db Youth.db --db Seniors.db --db Masters.db
  python3 most_improved_rider.py --db Seniors.db --max-rounds 12 --early-window 4 --late-window 4
  python3 most_improved_rider.py --db U12.db --csv most_improved_u12.csv
  python3 most_improved_rider.py --db Seniors.db --score-basis percentile   # old behaviour
"""

import argparse
import csv
import math
import sqlite3
from pathlib import Path
from collections import defaultdict

from pace_grade_scoring import compute_round_leader_paces, compute_rider_pace_grades


NONLEAGUE_THRESHOLD = 900
DEFAULT_EARLY_WINDOW = 4
DEFAULT_LATE_WINDOW  = 4
SCORE_BASIS_CHOICES = ("auto", "pace", "percentile")


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


def has_time_columns(conn) -> bool:
    """
    True if this DB's results table has the laps_completed /
    finish_time_seconds columns (added by migrate_add_time_laps.py).
    Older un-migrated DBs simply don't have them yet — that's not an
    error, it just means pace-based scoring can't run against this DB
    and everything falls back to rank-percentile.
    """
    cur = conn.execute("PRAGMA table_info(results)")
    cols = {row[1] for row in cur.fetchall()}
    return {"laps_completed", "finish_time_seconds"} <= cols


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


def get_all_finishes(conn, rider_ids, with_time_cols):
    """
    Return all FIN results for the given rider_ids.
    Returns dict: rider_id -> list of {round, cat_position, overall_position,
    points, is_ap, laps_completed, finish_time_seconds}

    laps_completed/finish_time_seconds are only selected when the DB
    actually has those columns (with_time_cols) — older DBs get None
    for both on every row, which flows straight through to "no pace
    grade available, fall back to percentile" without any special-casing
    here.
    """
    if not rider_ids:
        return {}
    placeholders = ",".join("?" * len(rider_ids))

    if with_time_cols:
        cur = conn.execute(f"""
            SELECT rider_id, round, cat_position, overall_position, points,
                   is_ap, laps_completed, finish_time_seconds
            FROM results
            WHERE status = 'FIN'
              AND rider_id IN ({placeholders})
            ORDER BY rider_id, round
        """, list(rider_ids))
    else:
        cur = conn.execute(f"""
            SELECT rider_id, round, cat_position, overall_position, points,
                   is_ap
            FROM results
            WHERE status = 'FIN'
              AND rider_id IN ({placeholders})
            ORDER BY rider_id, round
        """, list(rider_ids))

    finishes = defaultdict(list)
    for row in cur.fetchall():
        if with_time_cols:
            rider_id, rnd, cat_pos, overall_pos, points, is_ap, laps, time_sec = row
        else:
            rider_id, rnd, cat_pos, overall_pos, points, is_ap = row
            laps, time_sec = None, None
        finishes[rider_id].append({
            "round":            rnd,
            "cat_position":     cat_pos,
            "overall_position": overall_pos,
            "points":           points,
            "is_ap":            is_ap,
            "laps_completed":         laps,
            "finish_time_seconds":    time_sec,
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
    xs = round numbers (x-axis), ys = per-round scores (y-axis).
    Returns the slope (score gain per round) or None if fewer than 3 points.
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
                             min_early_avg=0.0,
                             score_basis="auto"):
    """
    Run the Most Improved calculation for one database.

    single_table=True  : women's single-table mode — when a round falls
                         back to percentile scoring, percentile is based
                         on overall_position within the full gender
                         group, not cat_position within the sub-category.
                         Use this for the Women's DB. (Pace-based rounds
                         don't need this flag — pace_grade_scoring.py's
                         PACE_GROUP_OVERRIDES already treats Women.db as
                         one pace group regardless of category.)

    score_basis : "auto" (pace grade where gradeable, else rank
                  percentile for that round — the default), "pace"
                  (pace grade only; rounds with no gradeable time data
                  are simply excluded from that rider's scoring, same as
                  a DNF), or "percentile" (the original rank-based
                  method only, ignoring any time data present).

    Returns a list of result dicts, sorted by improvement score descending.
    """

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

    with_time_cols = has_time_columns(conn)
    if score_basis == "pace" and not with_time_cols:
        raise SystemExit(
            f"❌ {db_label}: --score-basis pace requires laps_completed/"
            f"finish_time_seconds on 'results'.\n"
            f"   Run migrate_add_time_laps.py --db {db_label}.db first, "
            f"or use --score-basis auto/percentile."
        )
    if score_basis in ("auto", "pace") and not with_time_cols:
        print(f"  [{db_label}] No time/laps columns on this DB yet — "
              f"scoring every round by rank percentile (run "
              f"migrate_add_time_laps.py + re-import results to enable "
              f"pace-based scoring).")

    use_pace = with_time_cols and score_basis in ("auto", "pace")

    all_finishes = get_all_finishes(conn, list(league_riders.keys()), with_time_cols)
    field_by_cat, field_by_gender = compute_field_sizes(conn, valid_rounds)

    leader_paces = {}
    if use_pace:
        leader_paces = compute_round_leader_paces(conn, max_rounds, db_label)

    def get_position_and_field(rnd, cat, gender, cat_position, overall_position):
        """
        Return (position, field_size) to use for the percentile fallback.

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

        # Participation check (about actually finishing, not about
        # having a usable score — an AP-row-free "real" finish counts
        # here even if it later can't be scored by either method).
        if len(valid_finishes) < min_finishes_total:
            continue

        # Window finishes
        early_finishes = [f for f in valid_finishes if f["round"] in early_rounds]
        late_finishes  = [f for f in valid_finishes if f["round"] in late_rounds]

        if len(early_finishes) < min_window_finishes or len(late_finishes) < min_window_finishes:
            continue

        # Pace grades for every round this rider has a finish in (only
        # meaningful when use_pace — cheap no-op dict otherwise).
        pace_grades = {}
        if use_pace:
            history = [
                (f["round"], f["cat_position"], f["overall_position"], f["points"],
                 f["is_ap"], "FIN", f["laps_completed"], f["finish_time_seconds"])
                for f in valid_finishes
            ]
            pace_grades = compute_rider_pace_grades(history, leader_paces, db_label, cat)

        # Build per-round score detail for ALL valid finishes (used for display + averages)
        round_detail = {}
        for f in valid_finishes:
            rnd = f["round"]

            pace_pct = pace_grades.get(rnd) if use_pace else None
            pace_score = (pace_pct / 100.0) if pace_pct is not None else None

            pos, fs = get_position_and_field(
                rnd, cat, gender, f["cat_position"], f["overall_position"]
            )
            pct_score = percentile_score(pos, fs) if (fs and pos is not None) else None

            if score_basis == "percentile":
                score, source = pct_score, ("percentile" if pct_score is not None else None)
            elif score_basis == "pace":
                score, source = pace_score, ("pace" if pace_score is not None else None)
            else:  # auto
                if pace_score is not None:
                    score, source = pace_score, "pace"
                else:
                    score, source = pct_score, ("percentile" if pct_score is not None else None)

            window = ""
            if rnd in early_rounds:
                window = "E"
            if rnd in late_rounds:
                window = window + "L"
            round_detail[rnd] = {
                "cat_position":     f["cat_position"],
                "overall_position": f["overall_position"],
                "position_used":    pos,        # rank actually used for the percentile fallback
                "field_size":       fs,
                "points":           f.get("points"),
                "pace_grade_pct":   pace_pct,   # raw Pace Grade (0-100), None if ungradeable
                "percentile":       pct_score,  # rank-based score, always computed when possible
                "score":            score,      # the score actually used (pace_score or pct_score)
                "score_source":     source,     # "pace" | "percentile" | None (ungraded round)
                "window":           window,
            }

        def avg_score_from_detail(window_flag):
            scores = [
                d["score"]
                for d in round_detail.values()
                if window_flag in d["window"] and d["score"] is not None
            ]
            return (sum(scores) / len(scores)) if scores else None

        def graded_count_from_detail(window_flag, source):
            return sum(
                1 for d in round_detail.values()
                if window_flag in d["window"] and d["score_source"] == source
            )

        early_avg = avg_score_from_detail("E")
        late_avg  = avg_score_from_detail("L")

        if early_avg is None or late_avg is None:
            continue

        if early_avg < min_early_avg:
            continue

        improvement = late_avg - early_avg

        # Under --score-basis auto, a rider whose early window fell back to
        # percentile (no time data recorded yet for those older rounds)
        # but whose late window is pace-graded is having "improvement"
        # computed as a pace-grade average MINUS a rank-percentile average
        # — two different scales/meanings, not a clean single measurement.
        # Flag it rather than silently blend it: the number is still the
        # best comparison available (better than not scoring those rounds
        # at all), but it should be read with more caution than a rider
        # whose whole window used one consistent method throughout.
        sources_used = {
            d["score_source"] for d in round_detail.values()
            if d["window"] and d["score_source"] is not None
        }
        mixed_basis = len(sources_used) > 1

        # Regression slope across all valid, scored finishes
        reg_xs = sorted(round_detail.keys())
        reg_ys = [round_detail[rnd]["score"] for rnd in reg_xs
                  if round_detail[rnd]["score"] is not None]
        reg_xs = [rnd for rnd in reg_xs
                  if round_detail[rnd]["score"] is not None]
        slope = linear_regression_slope(reg_xs, reg_ys)

        # Predicted improvement across the full season span using slope
        # (slope * (last_round - first_round)) gives total score gain)
        if slope is not None and len(reg_xs) >= 2:
            season_span  = reg_xs[-1] - reg_xs[0]
            slope_improvement = slope * season_span
        else:
            slope_improvement = None

        # Highest single late-season score (for tie-break)
        late_scores = [
            d["score"]
            for d in round_detail.values()
            if "L" in d["window"] and d["score"] is not None
        ]
        best_late_pct = max(late_scores) if late_scores else 0.0

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
            "score_basis":        score_basis,
            "mixed_basis":        mixed_basis,
            "pace_rounds_early":       graded_count_from_detail("E", "pace"),
            "pace_rounds_late":        graded_count_from_detail("L", "pace"),
            "percentile_rounds_early": graded_count_from_detail("E", "percentile"),
            "percentile_rounds_late":  graded_count_from_detail("L", "percentile"),
            "slope":              slope,               # score gain per round number
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
    header = (f"  {'Rd':<4} {'Window':<7} {'Src':<5} {pos_label:<7} {'Field':<6} "
              f"{'Pts':<6} {'PaceG%':<8} {'Score%':<8}")
    print(header)
    print("  " + "-" * (len(header) - 2))
    rd = r.get("round_detail", {})
    for rnd in valid_rounds:
        if rnd not in rd:
            continue
        d = rd[rnd]
        win  = d["window"] or "-"
        src  = {"pace": "pace", "percentile": "rank", None: "-"}[d["score_source"]]
        pos  = str(d["position_used"]) if d.get("position_used") is not None else "-"
        fld  = str(d["field_size"])    if d["field_size"]   is not None else "-"
        pts  = str(d["points"])        if d["points"]       is not None else "-"
        paceg = f"{d['pace_grade_pct']:.1f}" if d["pace_grade_pct"] is not None else "-"
        pct  = f"{d['score']*100:.1f}%" if d["score"] is not None else "-"
        marker = " ◀ early" if win == "E" else (" ◀ late" if win == "L" else "")
        print(f"  {rnd:<4} {win:<7} {src:<5} {pos:<7} {fld:<6} {pts:<6} {paceg:<8} {pct:<8}{marker}")
    print()


def print_results(db_label, results, top_n=10, detail_top_n=3, min_season_gain=0.05):
    """
    Print results with regression as primary method, window as secondary check.
    min_season_gain : minimum slope_improvement to count as a winner (default 5%).
    """
    W = 78
    print(f"\n{'='*W}")
    print(f"  Most Improved Rider — {db_label}")
    print(f"{'='*W}")
    if not results:
        print("  No eligible riders.")
        return

    valid_rounds = results[0].get("valid_rounds", [])
    basis = results[0].get("score_basis", "auto")
    pace_rounds = sum(r["pace_rounds_early"] + r["pace_rounds_late"] for r in results)
    rank_rounds = sum(r["percentile_rounds_early"] + r["percentile_rounds_late"] for r in results)
    print(f"  Score basis: {basis}  "
          f"(window rounds scored — pace: {pace_rounds}, rank fallback: {rank_rounds})")
    mixed_riders = [r["name"] for r in results if r.get("mixed_basis")]
    if mixed_riders:
        print(f"  ⚠ {len(mixed_riders)} rider(s) have early/late windows scored by DIFFERENT "
              f"methods (some rounds lack time data) — their improvement figure mixes rank-"
              f"percentile and Pace Grade and should be read with caution. Marked '†' below.")

    # ── Regression ranking (PRIMARY) ──────────────────────────────────────────
    reg_results = sorted(
        [r for r in results if r.get("slope_improvement") is not None],
        key=lambda r: (-r["slope_improvement"], -r["late_avg_pct"], -r["total_finishes"])
    )
    reg_winner = (reg_results[0]
                  if reg_results and reg_results[0]["slope_improvement"] >= min_season_gain
                  else None)

    print(f"\n  PRIMARY — Improvement trend across all rounds")
    reg_header = (f"  {'Rank':<5} {'Name':<25} {'Cat':<6} {'Gen':<4} "
                  f"{'Fin':<5} {'Slope/Rd':<10} {'SeasonGain':<12} {'AvgScore'}")
    print(reg_header)
    print("  " + "-" * (len(reg_header) - 2))
    for i, r in enumerate(reg_results[:top_n], start=1):
        si  = r["slope_improvement"]
        sl  = r["slope"]
        scs = [d["score"] for d in r["round_detail"].values() if d["score"] is not None]
        avg = sum(scs) / len(scs) if scs else 0
        qualmark = " 📈" if (i == 1 and reg_winner) else (" ✗" if si < min_season_gain else "")
        mixmark = " †" if r.get("mixed_basis") else ""
        print(f"  {i:<5} {r['name']:<25} {r['race_category']:<6} {r['gender']:<4} "
              f"{r['total_finishes']:<5} {sl*100:>+8.3f}%   {si*100:>+8.2f}%"
              f"     {avg*100:.1f}%{qualmark}{mixmark}")
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
            mixmark = " †" if r.get("mixed_basis") else ""
            print(f"  {i:<5} {r['name']:<25} {r['race_category']:<6} {r['gender']:<4} "
                  f"{r['total_finishes']:<5} {r['early_avg_pct']*100:<8.1f} "
                  f"{r['late_avg_pct']*100:<8.1f} {r['improvement']*100:<+10.1f} "
                  f"{r['best_late_pct']*100:.1f}%{flag}{mixmark}")
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
    """Write results to CSV with per-round position, points, pace grade and score columns."""
    # Gather the actual rounds used across all results
    all_rounds = set()
    for res_list in all_results:
        for r in res_list:
            all_rounds.update(r.get("valid_rounds", []))
    rounds_sorted = sorted(all_rounds)

    base_fields = [
        "db_label", "rank", "name", "race_number", "race_category", "gender",
        "score_basis", "mixed_basis", "total_finishes", "early_finishes", "late_finishes",
        "early_avg_pct", "late_avg_pct", "improvement", "best_late_pct",
        "pace_rounds_early", "pace_rounds_late",
        "percentile_rounds_early", "percentile_rounds_late",
    ]
    round_fields = []
    for rnd in rounds_sorted:
        round_fields += [f"r{rnd}_window", f"r{rnd}_source", f"r{rnd}_pos", f"r{rnd}_field",
                         f"r{rnd}_pts", f"r{rnd}_pace_grade", f"r{rnd}_pct"]

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
                    "score_basis":    r["score_basis"],
                    "mixed_basis":    "yes" if r.get("mixed_basis") else "",
                    "total_finishes": r["total_finishes"],
                    "early_finishes": r["early_finishes"],
                    "late_finishes":  r["late_finishes"],
                    "early_avg_pct":  f"{r['early_avg_pct']:.6f}",
                    "late_avg_pct":   f"{r['late_avg_pct']:.6f}",
                    "improvement":    f"{r['improvement']:.6f}",
                    "best_late_pct":  f"{r['best_late_pct']:.6f}",
                    "pace_rounds_early":       r["pace_rounds_early"],
                    "pace_rounds_late":        r["pace_rounds_late"],
                    "percentile_rounds_early": r["percentile_rounds_early"],
                    "percentile_rounds_late":  r["percentile_rounds_late"],
                }
                rd = r.get("round_detail", {})
                for rnd in rounds_sorted:
                    d = rd.get(rnd, {})
                    row[f"r{rnd}_window"] = d.get("window", "")
                    row[f"r{rnd}_source"] = d.get("score_source", "") or ""
                    row[f"r{rnd}_pos"]    = d.get("cat_position", "")
                    row[f"r{rnd}_field"]  = d.get("field_size", "")
                    row[f"r{rnd}_pts"]    = d.get("points", "")
                    pg = d.get("pace_grade_pct")
                    row[f"r{rnd}_pace_grade"] = f"{pg:.2f}" if pg is not None else ""
                    pct = d.get("score")
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
                    help="Minimum early-window average score to be eligible (default 0.0). "
                         "E.g. 0.30 excludes riders who averaged below 30%% in the early window.")
    ap.add_argument("--min-window-finishes", type=int, default=3,
                    help="Minimum finishes required in each window (default 3)")
    ap.add_argument("--single-table", action="store_true",
                    help="Use overall position vs full gender field for the rank-percentile "
                         "fallback (for Women's DB where all females race together). Only "
                         "affects rounds that fall back to percentile scoring — pace-based "
                         "rounds already handle Women.db correctly via pace_grade_scoring.py.")
    ap.add_argument("--score-basis", choices=SCORE_BASIS_CHOICES, default="auto",
                    help="How to score each round: 'auto' (Pace Grade where the round has "
                         "time/laps data, rank percentile otherwise — default), 'pace' "
                         "(Pace Grade only, ungradeable rounds excluded), or 'percentile' "
                         "(the original rank-only method, ignoring any time data).")
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
            score_basis=args.score_basis,
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
