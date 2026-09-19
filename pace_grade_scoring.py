#!/usr/bin/env python3
"""
pace_grade_scoring.py
----------------------
Shared scoring logic for "Pace Grade" — a parkrun-style age-grade-alike
that lets a rider see how their pace compared to the fastest pace ridden
in their own category that round, regardless of how many laps either of
them completed.

WHY A GRADE INSTEAD OF COMPARING LAPS OR TIME DIRECTLY:
    WMCCL rounds are raced to a time cutoff, not a fixed distance (the
    "bell lap" rule) — the leader finishing triggers one more lap for
    whoever's still out, so different riders in the same round often
    complete different lap counts. Comparing raw finish times or lap
    counts between two such riders isn't meaningful. Comparing PACE
    (seconds per lap) is: it's a rate, so it's fair regardless of how
    many laps each rider personally got through.

THE FORMULA:
    pace                = finish_time_seconds / laps_completed
    pace_grade_percent  = (leader_pace / rider_pace) * 100

    The leader is therefore always exactly 100% (their pace compared to
    itself). Everyone else's grade is how close their own racing pace
    was to the leader's that day — a rider who did fewer laps than
    another rider, but at the same pace, still gets the same grade.
    Grades are NOT age/gender adjusted (unlike parkrun's actual age
    grading, which uses standard times by age+sex) — within-category
    racing already does most of that job here, and keeping the formula
    to "pace vs. today's leader" keeps it something you can explain to
    a rider in one sentence. See claude/pace-grade-explainer.html for
    the rider-facing version of that sentence.

WHO COUNTS AS "THE LEADER" (last changed 2026-09-19 — read this before
touching PACE_GROUP_OVERRIDES or either compute_* function below):
    The leader for a round is the FASTEST PACE among FIN finishers who
    share the rider's PACE GROUP that round — see pace_group_for(). This
    went through two designs before landing here; both are worth knowing
    about since either mistake is easy to reintroduce:

    Design 1 (original): overall_position==1 for the whole round in the
    DB, on the assumption that one DB == one shared physical start. Wrong
    for U8.db (bundles U6+U8) and Youth.db (bundles U14+U16), where the
    two age groups are imported as SEPARATE result CSVs — each import
    ranks only its own field 1..N, so a round could get two different
    rows both stamped overall_position==1, one per heat, and the code had
    no way to tell which belonged to a given rider's actual race. Adam
    caught this in production: top U8 riders and U14 boys were showing
    120%+ grades from being compared against the OTHER age group.

    Design 2: fastest pace within the rider's own exact `race_category`
    string, no merging at all. Correct for U8.db/Youth.db (fixed the bug
    above) but WRONG in the other direction for DBs where several
    `race_category` values genuinely share one start: it fragmented a
    shared field into needlessly narrow (sometimes single-rider) pools.

    Design 3 (current): fastest pace within the rider's PACE GROUP, where
    a pace group is either just their own race_category (the safe
    default — correct for U8.db/Youth.db/U10.db/U12.db, and for gender
    within any of those: girls start after boys in every age group here,
    so e.g. U8M and U8F are graded separately, never merged) or an
    explicit wider group from PACE_GROUP_OVERRIDES for the few DBs where
    Adam confirmed several categories share one actual start. Confirmed
    directly with Adam (2026-09-19), by DB:
        - Seniors.db runs three separate starts, so three pace groups:
          Juniors alone; SenM+U23M together (same gun); M40M+M45M
          together (own gun, started after Senior/U23 — NOT graded
          against them, same reasoning as the girls-start-after-boys
          rule above: a later gun means a different race, even on the
          same course).
        - Masters.db runs TWO pace groups (changed 2026-09-19, was
          originally one group for the whole field — Adam asked to
          split it): M50M+M55M together, and separately
          M60M+M65M+M70M together. Note this split does NOT match
          league_scoring.py's profile_tables() "masters" scoring
          buckets (M50M+M55M / M60M+M65M / M70M — three buckets, M70M
          on its own) — that's the scoring-points grouping, a different
          concept from pace groups, see the warning below. It's a
          coincidence that M50M+M55M happens to match on both sides;
          M60M/M65M/M70M do not.
        - Women.db runs ONE start for every category in it — the whole
          db collapses to a single pace group regardless of category.
        - Every other DB needs no entry: the "own race_category" default
          is already correct for it.
    None of this is the same thing as league_scoring.py's profile_tables()
    scoring groups, and the two must not be conflated — profile_tables()
    groups are about how LEAGUE POINTS get split (e.g. it merges SenM+U23M
    but keeps M40M/M45M apart from each other for scoring purposes), which
    is a different, narrower grouping than "who actually lined up at the
    same start line." Pace Grade cares only about the latter.

    A useful invariant of "fastest pace within the group" as the
    definition: since the leader is BY DEFINITION the minimum pace within
    whatever group is being graded, no rider can ever exceed 100% —
    that's structurally guaranteed by the formula, not something this
    code has to police. If you ever see >100%, the group boundaries in
    PACE_GROUP_OVERRIDES are wrong for that DB (two riders who didn't
    actually share a start got merged), not the formula.

WHAT COUNTS AS GRADEABLE:
    Same "real result" rule as the rest of the rider-page code: DNF rows
    (no reliable finish time) and AP / average-points rows (no time or
    laps recorded at all) get no grade for that round — shown as "—" —
    exactly like they're already excluded from points/position/medal
    calculations elsewhere.

Nothing in here touches the database beyond a single read query, and it
returns plain Python data structures — no HTML, no argv — mirroring
league_scoring.py / team_awards_scoring.py's own convention so this can
be reused by other outputs later (e.g. a club-level "best pace" award)
without risk of disagreeing with export_rider_pages.py.
"""

import sqlite3
from typing import Dict, List, Optional, Tuple


# Which race_category values actually share a start line, keyed by DB
# filename stem (case-insensitive matched against Path(db).stem, same
# convention export_rider_pages.py already uses for db_stem elsewhere).
# A dict value maps race_category -> pace-group key: categories mapped
# to the SAME key are graded against each other; anything not listed
# for a DB that IS in here still falls back to its own race_category
# (see pace_group_for()) rather than silently landing in some default
# bucket, so an unexpected category never gets merged with something by
# accident. The value None means "wildcard — the whole db is one group,
# regardless of race_category" (currently only Women.db).
#
# A DB not listed here at all (U8.db, U10.db, U12.db, Youth.db) needs no
# entry: every category in it defaults to being its own pace group,
# which is already correct — confirmed with Adam that genders never
# share a start in any of these (girls start after boys throughout), and
# U8.db/Youth.db bundle separate age groups that don't share a start
# either (that was the original bug this whole scheme replaced).
PACE_GROUP_OVERRIDES: Dict[str, Optional[Dict[str, str]]] = {
    "seniors": {
        "JunM": "Juniors",
        "SenM": "Sen_U23",
        "U23M": "Sen_U23",
        "M40M": "M40_M45",
        "M45M": "M40_M45",
    },
    "masters": {
        # Split 2026-09-19 (was one group for the whole field) — Adam
        # asked for M50/M55 to be paced separately from M60/M65/M70.
        "M50M": "M50_M55",
        "M55M": "M50_M55",
        "M60M": "M60_M65_M70",
        "M65M": "M60_M65_M70",
        "M70M": "M60_M65_M70",
    },
    "women": None,  # whole db is one pace group, whatever the category
}


def pace_group_for(db_stem: str, race_category: Optional[str]) -> Optional[str]:
    """
    Returns the Pace Grade comparison-group key for a rider: riders with
    the same (db_stem, group key) are graded against each other for a
    round; riders with different keys never are, even in the same
    db/round. Returns None if there's no category to key on at all (no
    safe group to put them in).

    Defaults to the rider's own race_category — i.e. no merging — for
    any db not listed in PACE_GROUP_OVERRIDES, and for any category
    within a listed db that isn't explicitly mapped there. See
    PACE_GROUP_OVERRIDES and the module docstring's "WHO COUNTS AS THE
    LEADER" section for exactly which DBs/categories are merged and why.
    """
    cat = (race_category or "").strip()
    if not cat:
        return None

    stem = db_stem.strip().lower()
    if stem not in PACE_GROUP_OVERRIDES:
        return cat

    overrides = PACE_GROUP_OVERRIDES[stem]
    if overrides is None:
        return f"__all__:{stem}"  # wildcard: whole db is one group

    return overrides.get(cat, cat)


def compute_pace(laps: Optional[int], time_seconds: Optional[float]) -> Optional[float]:
    """
    Seconds per lap, or None if laps/time aren't a valid gradeable pair
    (covers DNF rows — no time — and AP rows — no time or laps).
    """
    if laps is None or laps <= 0:
        return None
    if time_seconds is None or time_seconds <= 0:
        return None
    return time_seconds / laps


def pace_grade(rider_laps: Optional[int], rider_time_seconds: Optional[float],
               leader_pace: Optional[float]) -> Optional[float]:
    """
    Returns the rider's Pace Grade for one round as a percentage
    (leader's own pace = 100%), or None if either side of the comparison
    isn't gradeable.
    """
    if leader_pace is None or leader_pace <= 0:
        return None
    rider_pace = compute_pace(rider_laps, rider_time_seconds)
    if rider_pace is None:
        return None
    return (leader_pace / rider_pace) * 100.0


def compute_round_leader_paces(conn: sqlite3.Connection, rounds: int, db_stem: str
                               ) -> Dict[Tuple[int, str], float]:
    """
    Returns {(round_number, pace_group): leader_pace_seconds_per_lap} —
    one leader per round PER PACE GROUP (see pace_group_for() and the
    module docstring's "WHO COUNTS AS THE LEADER" section — this is NOT
    simply race_category, a few DBs merge several categories that share
    a start). A (round, group) with no valid FIN result at all is simply
    absent from the dict (not an error — e.g. a round not yet raced, or
    nobody in that group finished).

    db_stem: the source db's filename stem (e.g. "Seniors" from
    "Seniors.db"), used to pick the right PACE_GROUP_OVERRIDES entry —
    same value export_rider_pages.py already computes as db_stem for
    picking award links, pass the same one through here.

    race_category comes from the RIDERS table (results doesn't store
    it), so this joins across — that's the one thing this function does
    that isn't a single flat read of `results`.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT res.round, r.race_category, res.laps_completed, res.finish_time_seconds
        FROM results res
        JOIN riders r ON r.id = res.rider_id
        WHERE res.status = 'FIN'
          AND res.round BETWEEN 1 AND ?
          AND res.laps_completed IS NOT NULL AND res.laps_completed > 0
          AND res.finish_time_seconds IS NOT NULL AND res.finish_time_seconds > 0
    """, (rounds,))

    leader_paces: Dict[Tuple[int, str], float] = {}
    for rnd, race_category, laps, time_sec in cur.fetchall():
        group = pace_group_for(db_stem, race_category)
        if group is None:
            # No category on file for this rider — nothing safe to group
            # them with (grouping blanks together would silently compare
            # unrelated riders), so they get no leader and no grade.
            continue
        pace = time_sec / laps
        key = (rnd, group)
        if key not in leader_paces or pace < leader_paces[key]:
            leader_paces[key] = pace

    return leader_paces


def compute_rider_pace_grades(history: List[Tuple], leader_paces: Dict[Tuple[int, str], float],
                              db_stem: str, race_category: Optional[str]
                              ) -> Dict[int, Optional[float]]:
    """
    history rows: (round, cat_position, overall_position, points, is_ap,
                    status, laps_completed, finish_time_seconds)
    — the same shape export_rider_pages.py already loads per rider.

    db_stem / race_category: this rider's source db and own current-
    season category — together they pick the right pace group (see
    pace_group_for()) to look up in leader_paces for each round.
    Category is treated as fixed for the season here, same as everywhere
    else on the rider page (only race_category_previous_year exists for
    comparison, there's no per-round category history).

    Returns {round_number: grade_percent_or_None} for every round in
    history, gradeable rounds only get a value (DNF/AP/no-category/no-
    leader-data rounds get None, same "no grade this round" treatment as
    everywhere else on the rider page).
    """
    group = pace_group_for(db_stem, race_category)
    grades: Dict[int, Optional[float]] = {}
    for rnd, cat_pos, overall_pos, points, is_ap, status, laps, time_sec in history:
        if status != "FIN" or is_ap or group is None:
            grades[rnd] = None
            continue
        grades[rnd] = pace_grade(laps, time_sec, leader_paces.get((rnd, group)))
    return grades


def pace_grade_trend(graded_rounds: List[Tuple[int, float]]) -> Optional[Dict[str, object]]:
    """
    A short, positive season-trend summary comparing a rider's earlier
    graded rounds to their more recent ones.

    graded_rounds: [(round_number, grade_percent), ...] for rounds that
    actually have a grade (already filtered — callers should drop the
    Nones before calling this), in any order; sorted here by round.

    Needs at least 4 graded rounds to say anything meaningful (fewer
    than that and an "early vs late" split is just noise) — returns
    None below that threshold.

    Returns {"early_avg": float, "late_avg": float, "delta": float,
             "direction": "up" | "down" | "steady"} where "steady" is
    anything within 1.5 percentage points either way (not meaningfully
    different given normal week-to-week course/conditions variation).
    """
    if len(graded_rounds) < 4:
        return None

    ordered = sorted(graded_rounds, key=lambda x: x[0])
    half = len(ordered) // 2
    early = [g for _, g in ordered[:half]]
    late = [g for _, g in ordered[half:]]

    early_avg = sum(early) / len(early)
    late_avg = sum(late) / len(late)
    delta = late_avg - early_avg

    if delta > 1.5:
        direction = "up"
    elif delta < -1.5:
        direction = "down"
    else:
        direction = "steady"

    return {
        "early_avg": early_avg,
        "late_avg": late_avg,
        "delta": delta,
        "direction": direction,
    }
