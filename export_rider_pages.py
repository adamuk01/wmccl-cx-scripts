#!/usr/bin/env python3
"""
export_rider_pages.py
----------------------
Generate static, privacy-conscious HTML "rider pages" from one or more
league SQLite DBs — one page per race_number, showing history, stats and
badges across all rounds in that DB.

WHY:
    The league currently publishes a flat PDF of category standings.
    This produces one static HTML file per rider, showing their own
    results history, so riders/parents can see progress over the season
    without needing a login system.

PRIVACY (deliberately limited fields):
    Public pages show ONLY:
        - race_number
        - firstname + surname initial   (e.g. "Jimmy S.")
        - club_name
        - gender
        - race_category (current season)
        - results history (round, position, points, status, medal)
    Public pages NEVER show:
        - full surname
        - DOB / YOB
        - BC_number / membership IDs
        - IBX (logistics-only field)

    Pages are also marked <meta name="robots" content="noindex,nofollow">
    and a robots.txt is generated disallowing the whole folder, so pages
    are only reachable by someone who already has the direct link
    (race number), not discoverable via search engines.

BADGES / STATS (all computed from data already in the DB):
    - Medals: gold/silver/bronze shown against any round where
      cat_position is 1/2/3 (category position is used for ALL
      categories, including Women's, per league convention — overall
      position is only used for gridding/league-table purposes
      elsewhere, not for medals).
    - Milestone badge: the highest of "5 Rounds" / "10 Rounds" /
      "Full House" (completed every round of the season) a rider has
      reached. Only one is shown, not all achieved thresholds.
    - Newbie badge: rider has no race_category_previous_year on record.
    - Season-best round: the round with the rider's highest points
      total is highlighted in their history table.
    - Season totals: laps completed and total recorded race time
      ("time in the saddle") across all real (non-AP) results.
    - Last year vs this year average points: shown side by side, with
      a caveat if the rider's category has changed between seasons
      (since a category change can shift the comparison group).

PACE GRADE (2026-09-19, leader scoping revised twice same day):
    A parkrun-age-grade-style "how did I do?" percentage, computed and
    documented in pace_grade_scoring.py (read that module's docstring
    for the full reasoning — the short version: it's a rider's pace
    (seconds per lap) compared to the fastest pace ridden in their PACE
    GROUP that round, so the fastest rider in that group is always 100%
    and everyone else's grade is fair regardless of how many laps they
    personally completed, which matters here because WMCCL rounds run to
    a time cutoff rather than a fixed distance).

    IMPORTANT: a "pace group" is USUALLY just the rider's own
    race_category, but not always — see PACE_GROUP_OVERRIDES in
    pace_grade_scoring.py. Seniors.db, Masters.db and Women.db each merge
    several race_category values into shared groups because Adam
    confirmed those riders genuinely share a start line, even though a
    couple of them (U8.db, Youth.db) that superficially look similar do
    NOT merge, because there Adam confirmed the opposite — separate
    starts entirely. Don't "simplify" this to either a single per-DB/
    round leader OR a strict one-group-per-race_category rule; both were
    tried, both produced wrong grades for some DB, and pace_grade_
    scoring.py's "WHO COUNTS AS THE LEADER" section documents both
    failures so a future change doesn't repeat either one.

    Shown three ways on a rider's page:
        - A "Pace grade" column in the results table (round-by-round),
          "—" for any round with no grade (DNF / AP / not yet raced).
        - A "Pace grade (season avg)" stat tile, mean of graded rounds.
        - A one-line season trend (early-season vs. recent-season
          average), only shown once a rider has at least 4 graded
          rounds — fewer than that and an early/late split is just
          noise. Framed positively in all three directions (up/down/
          steady): see render_pace_trend_block().
    The round with a rider's single best Pace Grade is tagged "⚡ Best
    pace" in the table — independent of, and possibly a different
    round from, the existing "★ Season best" points tag.

    Rider-facing explanation of all this lives in
    pace-grade-explainer.html (a WordPress-paste-in snippet, not linked
    from this page's HTML — the league site links to it from wherever
    makes sense alongside the results pages).

WHAT COUNTS AS A "REAL" RESULT:
    AP (average points) rows and DNF rows are excluded from points/
    position/medal/milestone calculations. DNF rows DO still count
    towards laps completed (they rode them) but not towards
    total race time (no reliable finish time exists for a DNF). Pace
    Grade follows the same rule (see above).

ROUND NAMES / VENUES:
    --rounds-file takes the SAME CSV used to populate each DB's `rounds`
    table (see set_round_names.py / rounds-template.csv), with headers:
        round,name,venue,date,conditions
    Only 'round' and at least one of name/venue/date are required — any
    of the four can be blank per-row, and 'conditions' is optional on
    top of that. Older CSVs with just round,name (or round,venue /
    round,location, with or without a conditions column) still work too.

    Each rider's results table shows two separate columns for this,
    kept apart from the "Round" column so nothing is repeated:
        - Location: the venue field (falls back to name if venue is
          blank, e.g. for an older round,name-only CSV)
        - Date: the date field
    The round's "name" (e.g. an event name distinct from its venue) is
    not shown on the rider page — it's only used on the league table
    pages (export_league_tables_html.py) round tooltips/legend.

CONDITIONS (bit of fun):
    Optional per-round ground/weather conditions, shown as a small icon
    against each round in a rider's results table (e.g. a rain cloud for
    a wet round). Read from the same --rounds-file CSV as name/venue/
    date, via an optional 'conditions' column — one value per round, not
    per rider, since everyone racing a given round rode the same ground.
    Recognised values are case-insensitive: dry, wet, muddy, icy, snowy
    (see CONDITION_ICONS). A blank or unrecognised value just shows no
    icon rather than erroring, so a typo in the CSV is silently invisible
    — worth a glance at the generated pages after editing the CSV. The
    hover tooltip on each icon is a fun nickname (e.g. "Torrent" for a
    wet round), not the plain CSV word — edit CONDITION_ICONS to change
    the wording.

TEAM & CLUB AWARDS LINKS (2026-09-19):
    Each rider page shows a short "Team standings" line linking out to
    whichever of the 3 award pages (built by export_team_awards_html.py
    into the SAME --outdir) apply to that rider: the U12 Team Competition
    or Team Competition depending on which source DB the rider came from
    (matching the same U8+U10+U12 / Women+Seniors+Masters+Youth groupings
    run-team-awards-results.sh and export_team_awards_html.py use), plus
    the Mick Ives Participation Award, which covers every rider. A rider
    with no club on file (blank, or the "No Club/Team" placeholder used
    to exclude unattached riders from the awards) gets no links, since
    they won't appear on those pages. These are plain relative links —
    this script doesn't check that the award pages actually exist yet,
    so run export_team_awards_html.py at some point into the same
    --outdir for them to resolve.

PAGE-LINK CACHE-BUSTING (2026-09-23):
    The "← Back" fallback link and the award links go through vurl(),
    which adds `?v=<BUILD_VERSION>` (a timestamp of this run, or the
    WMCCL_BUILD_VERSION environment variable if set) so each weekly
    update's pages get URLs the browser has never cached. Identical to
    BUILD_VERSION/vurl() in export_league_tables_html.py (see its
    BROWSER CACHING fix 3) — copied rather than imported because this
    script is deliberately standalone.

USAGE:
    python3 export_rider_pages.py --db U8.db U10.db U12.db Youth.db \
        Women.db Masters.db Seniors.db \
        --outdir rider_pages --rounds 11 --rounds-file rounds.csv

    Then upload the 'rider_pages' folder to the website (e.g. as a
    static subfolder alongside WordPress), and merge robots.txt.disallow.txt
    into the site's existing robots.txt.

OUTPUT:
    <outdir>/riders/<race_number>.html   one file per rider
    <outdir>/robots.txt.disallow.txt     snippet to merge into site robots.txt
    <outdir>/index_admin.csv             race_number -> name/club, for club
                                          admin use only (NOT for publishing)
"""

import argparse
import csv
import html
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pace_grade_scoring import (
    compute_round_leader_paces,
    compute_rider_pace_grades,
    pace_grade_trend,
)


AP_MARKER = 999
MILESTONE_THRESHOLDS = [5, 10]  # "Full House" is handled separately, at season_length

# Trend line only shown once a rider has at least this many graded
# rounds — fewer than that and an early/late split is just noise. Kept
# here (not in pace_grade_scoring.py) since it's a presentation choice
# for THIS page, not part of the grading formula itself.
PACE_TREND_MIN_ROUNDS = 4

# Page-link cache-busting (see PAGE-LINK CACHE-BUSTING in the docstring).
BUILD_VERSION = os.environ.get("WMCCL_BUILD_VERSION") or time.strftime("%Y%m%d%H%M%S")


def vurl(href: str) -> str:
    """Append ?v=<BUILD_VERSION> to a relative page link (keeps any #fragment at the end)."""
    base, hash_sign, fragment = href.partition("#")
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}v={BUILD_VERSION}{hash_sign}{fragment}"


# Optional per-round ground/weather conditions ("bit of fun") shown as a
# small icon against each round in a rider's results table — one value
# per round (not per rider), since everyone racing a given round rode
# the same ground. Keys are matched case-insensitively against the
# 'conditions' column in --rounds-file; anything else (including a typo,
# or a blank/missing value) shows no icon rather than erroring, so it's
# worth a glance at the generated pages after editing the CSV.
#
# The second item in each tuple is the hover title shown on the icon —
# a bit of fun rather than a plain restatement of the CSV value.
CONDITION_ICONS = {
    "dry": ("☀️", "Dust Bowl"),
    "wet": ("🌧️", "Torrent"),
    "muddy": ("🟤", "Slip & Slide"),
    "icy": ("🧊", "The Frostbite Classic"),
    "snowy": ("❄️", "The Whiteout"),
}

# Which source DB feeds which team award, mirroring the groupings used by
# run-team-awards-results.sh / export_team_awards_html.py:
#   youth_team (U12 Team Competition)  = U8 + U10 + U12
#   adult_team (Team Competition)      = Women + Seniors + Masters + Youth
# Matched case-insensitively against the DB's filename stem (no extension),
# so "U8.db", "u8.DB", etc. all match. Every rider, regardless of DB, also
# gets a link to the Mick Ives Participation Award (all categories).
U12_TEAM_DB_STEMS = {"u8", "u10", "u12"}
ADULT_TEAM_DB_STEMS = {"women", "seniors", "masters", "youth"}

AWARD_LINKS = {
    "u12_team": ("U12 Team Competition", "../awards/u12-team-competition.html"),
    "adult_team": ("Team Competition", "../awards/team-competition.html"),
    "participation": ("Mick Ives Participation Award", "../awards/participation.html"),
}

# Clubs that are excluded from the award pages (see export_team_awards_html.py
# / run-team-awards-results.sh's own --exclude-club default) — a rider with
# this club (or no club at all) won't appear there, so we don't show links.
NO_CLUB_PLACEHOLDER = "no club/team"


def team_award_links_for_db(db_stem: str) -> List[Tuple[str, str]]:
    """
    Returns the (label, href) pairs relevant to a rider from this source DB
    — always the Participation award, plus whichever team competition this
    DB feeds into (if any).
    """
    stem = db_stem.strip().lower()
    links = []
    if stem in U12_TEAM_DB_STEMS:
        links.append(AWARD_LINKS["u12_team"])
    elif stem in ADULT_TEAM_DB_STEMS:
        links.append(AWARD_LINKS["adult_team"])
    links.append(AWARD_LINKS["participation"])
    return links


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_float(x, default=None):
    if x is None:
        return default
    s = str(x).strip()
    if s == "":
        return default
    try:
        return float(s)
    except ValueError:
        return default


def surname_initial(surname: Optional[str]) -> str:
    s = (surname or "").strip()
    return f"{s[0].upper()}." if s else ""


def display_name(firstname: Optional[str], surname: Optional[str]) -> str:
    first = (firstname or "").strip()
    init = surname_initial(surname)
    return f"{first} {init}".strip()


def esc(x) -> str:
    return html.escape(str(x if x is not None else ""))


def format_duration(total_seconds: Optional[float]) -> str:
    """
    Formats a total number of seconds as e.g. '3h 24m' or '42m 10s'.
    Returns '—' for None/zero (nothing recorded yet).
    """
    if not total_seconds:
        return "—"
    total_seconds = int(round(total_seconds))
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def format_pct(value: Optional[float]) -> str:
    """Whole-number percentage for a Pace Grade value, '—' if ungraded."""
    if value is None:
        return "—"
    return f"{value:.0f}%"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def ensure_schema(conn: sqlite3.Connection, db_name: str):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='riders'")
    if not cur.fetchone():
        raise SystemExit(f"❌ {db_name}: missing 'riders' table")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results'")
    if not cur.fetchone():
        raise SystemExit(f"❌ {db_name}: missing 'results' table")

    cur.execute("PRAGMA table_info(results)")
    cols = {row[1] for row in cur.fetchall()}
    missing = {"laps_completed", "finish_time_seconds"} - cols
    if missing:
        raise SystemExit(
            f"❌ {db_name}: 'results' table is missing {sorted(missing)}.\n"
            f"   Run migrate_add_time_laps.py against it first."
        )


def load_riders(conn: sqlite3.Connection) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, race_number, firstname, surname, gender, club_name,
               race_category, race_category_previous_year, average_points_last_year
        FROM riders
        ORDER BY race_number
    """)
    return cur.fetchall()


def load_results_for_rider(conn: sqlite3.Connection, rider_id: int, rounds: int) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT round, cat_position, overall_position, points, is_ap, status,
               laps_completed, finish_time_seconds
        FROM results
        WHERE rider_id = ? AND round BETWEEN 1 AND ?
        ORDER BY round
    """, (rider_id, rounds))
    return cur.fetchall()


def load_round_names(path: Optional[str]) -> Dict[int, Dict[str, str]]:
    """
    Loads the shared rounds CSV (same file used for set_round_names.py /
    each DB's `rounds` table), with headers:
        round,name,venue,date,conditions

    Returns {round_number: {"name": ..., "venue": ..., "date": ...,
    "conditions": ...}}. Any of name/venue/date/conditions may be blank
    on a given row. Older CSVs that only have round,name (or round,venue
    / round,location, with or without a conditions column) still work —
    whichever of those columns is present is read as "name" and the
    others default to "".

    Column matching is case/whitespace-insensitive, so a header like
    'Round,Location' or ' Round , Name ' still works. 'conditions' /
    'condition' are both accepted for the conditions column.

    Returns {} if no file given.
    """
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        print(f"⚠️  WARNING: rounds file not found, continuing without race names: {p}")
        return {}

    with p.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        # Map normalised (lowercase, stripped) header -> actual header text,
        # so we can find "round" and "name"/"venue"/"date"/"location"
        # regardless of case or stray whitespace in the CSV.
        normalised = {(fn or "").strip().lower(): fn for fn in fieldnames}
        round_key = normalised.get("round")
        name_key = normalised.get("name") or normalised.get("location")
        venue_key = normalised.get("venue")
        date_key = normalised.get("date")
        conditions_key = normalised.get("conditions") or normalised.get("condition")

        if round_key is None or not any([name_key, venue_key, date_key]):
            print(f"⚠️  WARNING: rounds file '{p}' doesn't have a recognisable "
                  f"'round' column plus at least one of name/venue/date/location.")
            print(f"    Headers found: {fieldnames}")
            print(f"    Continuing without race names.")
            return {}

        info = {}
        for row in reader:
            rnd_raw = (row.get(round_key) or "").strip()
            if not rnd_raw:
                continue
            try:
                rnd = int(rnd_raw)
            except ValueError:
                continue

            entry = {
                "name": (row.get(name_key) or "").strip() if name_key else "",
                "venue": (row.get(venue_key) or "").strip() if venue_key else "",
                "date": (row.get(date_key) or "").strip() if date_key else "",
                "conditions": (row.get(conditions_key) or "").strip().lower() if conditions_key else "",
            }
            if any(entry.values()):
                info[rnd] = entry

    return info


def round_venue_label(rnd: int, round_names: Dict[int, Dict[str, str]]) -> str:
    """
    The "Location" cell for a round — just the venue field from the CSV.
    Falls back to the name field if venue is blank (covers an older CSV
    that only has round,name or round,location — no separate venue
    column). Returns "—" if nothing is on file for this round.
    """
    info = round_names.get(rnd)
    if not info:
        return "—"
    return info["venue"] or info["name"] or "—"


def round_date_label(rnd: int, round_names: Dict[int, Dict[str, str]]) -> str:
    """The "Date" cell for a round — just the date field from the CSV."""
    info = round_names.get(rnd)
    if not info:
        return "—"
    return info["date"] or "—"


def round_conditions_icon(rnd: int, round_names: Dict[int, Dict[str, str]]) -> Tuple[str, str]:
    """
    Returns (icon, label) for a round's ground/weather conditions, e.g.
    ("🌧️", "Wet"), looked up in CONDITION_ICONS. Returns ("", "") if no
    conditions are on file for this round, or the CSV value isn't one of
    the recognised keys (dry/wet/muddy/icy/snowy) — either way the cell
    is left blank rather than showing a placeholder or raising an error.
    """
    info = round_names.get(rnd)
    if not info:
        return "", ""
    return CONDITION_ICONS.get((info.get("conditions") or "").strip().lower(), ("", ""))


# ---------------------------------------------------------------------------
# Stats & badges
# ---------------------------------------------------------------------------

MEDAL_LABELS = {1: "1st", 2: "2nd", 3: "3rd"}
MEDAL_CLASSES = {1: "gold", 2: "silver", 3: "bronze"}


def compute_stats(history: List[Tuple], pace_grades: Dict[int, Optional[float]], *,
                  season_length: int,
                  race_category: Optional[str],
                  race_category_previous_year: Optional[str],
                  average_points_last_year: Optional[float]) -> Dict[str, object]:
    """
    history rows: (round, cat_position, overall_position, points, is_ap,
                    status, laps_completed, finish_time_seconds)

    pace_grades: {round_number: grade_percent_or_None}, from
    pace_grade_scoring.compute_rider_pace_grades() — passed in rather
    than recomputed here so the table cells and these stats can never
    disagree.

    Medals always key off cat_position (category position) for every
    category, including Women's — overall_position is used elsewhere
    for gridding/league tables, not for medals.
    """
    real_points = []
    races_completed = 0
    best_cat_position = None
    best_points = None
    best_round = None
    total_laps = 0
    total_time_seconds = 0.0
    medal_rounds = {}  # round -> 1/2/3

    best_pace_grade = None
    best_pace_round = None
    graded_rounds: List[Tuple[int, float]] = []

    for rnd, cat_pos, overall_pos, points, is_ap, status, laps, time_sec in history:
        is_real = (status == "FIN") and not is_ap and points != AP_MARKER

        # Laps count even on a DNF (they still rode them); time does not,
        # since a DNF has no reliable finish time.
        if laps is not None:
            total_laps += laps

        if is_real:
            races_completed += 1

            p = safe_float(points)
            if p is not None:
                real_points.append(p)
                if best_points is None or p > best_points:
                    best_points = p
                    best_round = rnd

            if cat_pos is not None:
                if best_cat_position is None or cat_pos < best_cat_position:
                    best_cat_position = cat_pos
                if cat_pos in MEDAL_LABELS:
                    medal_rounds[rnd] = cat_pos

            if time_sec is not None:
                total_time_seconds += time_sec

        grade = pace_grades.get(rnd)
        if grade is not None:
            graded_rounds.append((rnd, grade))
            if best_pace_grade is None or grade > best_pace_grade:
                best_pace_grade = grade
                best_pace_round = rnd

    avg_points = (sum(real_points) / len(real_points)) if real_points else None
    avg_pace_grade = (
        sum(g for _, g in graded_rounds) / len(graded_rounds) if graded_rounds else None
    )

    trend = None
    if len(graded_rounds) >= PACE_TREND_MIN_ROUNDS:
        trend = pace_grade_trend(graded_rounds)

    # Milestone badge: highest threshold reached, "Full House" beats
    # the numeric thresholds if the season is complete for this rider.
    milestone_badge = None
    for threshold in MILESTONE_THRESHOLDS:
        if races_completed >= threshold:
            milestone_badge = f"{threshold} Rounds"
    if season_length and races_completed >= season_length:
        milestone_badge = "Full House"

    is_newbie = not (race_category_previous_year or "").strip()

    category_changed = (
        not is_newbie
        and (race_category or "").strip() != (race_category_previous_year or "").strip()
    )

    return {
        "races_completed": races_completed,
        "best_cat_position": best_cat_position,
        "best_points": best_points,
        "best_round": best_round,
        "avg_points": avg_points,
        "total_laps": total_laps,
        "total_time_seconds": total_time_seconds,
        "medal_rounds": medal_rounds,
        "milestone_badge": milestone_badge,
        "is_newbie": is_newbie,
        "category_changed": category_changed,
        "avg_points_last_year": average_points_last_year,
        "avg_pace_grade": avg_pace_grade,
        "best_pace_grade": best_pace_grade,
        "best_pace_round": best_pace_round,
        "graded_rounds_count": len(graded_rounds),
        "pace_trend": trend,
    }


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

# Auto-resizes the iframe this page is embedded in (e.g. a WordPress page
# with an <iframe src="/rider_pages/riders/..."> so the page keeps the
# site's header/footer). Only does anything when the page is actually
# loaded inside a SAME-ORIGIN iframe (window.frameElement is only
# reachable same-origin) — visiting the page directly, or a cross-origin
# embed, leaves it a harmless no-op. Re-checks on load, on pageshow (this
# is the one that matters for browser back/forward — a back-navigation
# restores the page without firing 'load', so without a pageshow handler
# the iframe gets stuck at whatever height the page you navigated away
# from needed), on window resize, and shortly after load to catch
# late-loading fonts/images shifting the page height.
#
# Shrinking is a special case: document.documentElement.scrollHeight is
# defined as never smaller than the iframe's OWN current viewport height,
# so if we don't first collapse the iframe back towards zero before
# re-measuring, going from a tall page to a shorter one would read the
# still-inflated old height back as "content height" and the iframe would
# never shrink. Resetting to 0px immediately before measuring fixes that.
#
# That reset is itself a real height change, so it triggers this script's
# own 'resize' listener (the iframe's own viewport just changed) — the
# `busy` guard stops that from re-entering resize() and looping.
#
# SCROLL-INTO-VIEW ON NAVIGATION (added 2026-09-22, identical to the copy
# in export_league_tables_html.py — see that file's comment for the full
# story): clicking into a rider page from partway down a long league
# table left the browser scrolled to the bottom of the outer WordPress
# page, since the outer page's scroll position doesn't move on its own
# when the iframe navigates to a new (often shorter) page. On 'load'
# specifically (not 'pageshow', which also covers a back/forward restore
# where the outer scroll position is already correct; not 'resize'),
# scroll the outer page so the iframe's top is back in view — skipped on
# the very first page load in a tab (sessionStorage flag) so a normal
# arrival at the page doesn't jump unexpectedly.
IFRAME_RESIZE_SCRIPT = """<script>
(function () {
  try {
    if (window.frameElement) {
      var busy = false;
      var resize = function () {
        if (busy) return;
        busy = true;
        window.frameElement.style.height = '0px';
        window.frameElement.style.height = document.documentElement.scrollHeight + 'px';
        setTimeout(function () { busy = false; }, 50);
      };
      window.addEventListener('load', function () {
        resize();
        try {
          if (sessionStorage.getItem('wmccl_iframe_seen')) {
            window.frameElement.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }
          sessionStorage.setItem('wmccl_iframe_seen', '1');
        } catch (e) { /* storage blocked (private browsing etc.) - ignore */ }
      });
      window.addEventListener('pageshow', resize);
      window.addEventListener('resize', resize);
      setTimeout(resize, 300);
      resize();
    }
  } catch (e) { /* cross-origin or no iframe context - ignore */ }
})();
</script>"""

# Rider pages are linked from several different places — a specific league
# table row, an award page, potentially others later — so there's no single
# fixed "came from" page to hard-code a breadcrumb link to (unlike the
# league table pages' "← All categories", which always goes to the same
# index.html). Added 2026-09-22 at Adam's request: rider pages had no way
# back at all. Rather than guessing, the "← Back" link uses the browser's
# own history to return to WHICHEVER page actually linked here — a table,
# an award page, wherever — via history.back(), which (same as a real
# browser Back button) navigates just this iframe back to its previous
# page, consistent with the existing back/forward handling in
# IFRAME_RESIZE_SCRIPT above. Only wired up when there's actually
# something to go back to (history.length > 1); otherwise the link's
# plain href to ../index.html (the season hub) is left as a sensible
# fallback for a rider page opened directly (e.g. a bookmark or a shared
# link, not navigated to from within the site).
BACK_LINK_SCRIPT = """<script>
(function () {
  try {
    var link = document.getElementById('wmccl-back-link');
    if (link && window.history && history.length > 1) {
      link.addEventListener('click', function (e) {
        e.preventDefault();
        history.back();
      });
    }
  } catch (e) { /* ignore */ }
})();
</script>"""

PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Rider {race_number} — {name} — WMCCL Results History</title>
<style>
  .wmccl-rider {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; max-width: 680px; margin: 0 auto; padding: 1rem; }}
  .wmccl-rider h1 {{ font-size: 1.4rem; margin-bottom: 0.1rem; }}
  .wmccl-rider .meta {{ color: #555; margin-bottom: 0.75rem; }}
  .wmccl-rider table {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; }}
  .wmccl-rider th, .wmccl-rider td {{ text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid #ddd; }}
  .wmccl-rider th {{ background: #f5f5f5; }}
  .wmccl-rider .stats {{ display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 1rem 0; }}
  .wmccl-rider .stat {{ background: #f5f5f5; border-radius: 6px; padding: 0.5rem 0.8rem; }}
  .wmccl-rider .stat .label {{ display: block; font-size: 0.75rem; color: #666; }}
  .wmccl-rider .stat .value {{ font-size: 1.1rem; font-weight: 600; }}
  .wmccl-rider .ap {{ color: #888; font-style: italic; }}
  .wmccl-rider .badges {{ display: flex; flex-wrap: wrap; gap: 0.5rem; margin: 0.75rem 0 1.25rem; }}
  .wmccl-rider .badge {{ display: inline-block; padding: 0.3rem 0.7rem; border-radius: 999px; font-size: 0.8rem; font-weight: 600; }}
  .wmccl-rider .badge.milestone {{ background: #e6f0ff; color: #1f4e79; }}
  .wmccl-rider .badge.newbie {{ background: #eafbea; color: #1f7a1f; }}
  .wmccl-rider .medal {{ display: inline-block; padding: 0.1rem 0.5rem; border-radius: 999px; font-size: 0.8rem; font-weight: 600; margin-left: 0.4rem; }}
  .wmccl-rider .medal.gold {{ background: #f4c430; color: #4a3900; }}
  .wmccl-rider .medal.silver {{ background: #cfd4d8; color: #333; }}
  .wmccl-rider .medal.bronze {{ background: #d8935a; color: #3a2200; }}
  .wmccl-rider .cond {{ font-size: 1.1rem; cursor: default; }}
  .wmccl-rider tr.best-round {{ background: #fff8e1; }}
  .wmccl-rider .best-tag {{ margin-left: 0.4rem; font-size: 0.75rem; color: #8a6d00; font-weight: 600; }}
  .wmccl-rider .pace-best-tag {{ margin-left: 0.4rem; font-size: 0.75rem; color: #1f7a1f; font-weight: 600; }}
  .wmccl-rider .compare {{ margin: 0.5rem 0 1.25rem; font-size: 0.9rem; color: #444; }}
  .wmccl-rider .compare .note {{ display: block; font-size: 0.8rem; color: #888; margin-top: 0.2rem; }}
  .wmccl-rider .pace-trend {{ margin: 0.5rem 0 1.25rem; font-size: 0.9rem; padding: 0.5rem 0.8rem; border-radius: 6px; }}
  .wmccl-rider .pace-trend.up {{ background: #eafbea; color: #1f7a1f; }}
  .wmccl-rider .pace-trend.steady {{ background: #f0f4ff; color: #1f4e79; }}
  .wmccl-rider .pace-trend.down {{ background: #f5f5f5; color: #555; }}
  .wmccl-rider .team-links {{ margin: 0 0 1.25rem; font-size: 0.9rem; color: #444; }}
  .wmccl-rider .team-links a {{ margin-right: 0.75rem; }}
  .wmccl-rider .breadcrumb {{ margin: 0 0 0.75rem; font-size: 0.9rem; }}
</style>
</head>
<body>
<div class="wmccl-rider">
  <p class="breadcrumb"><a href="{back_href}" id="wmccl-back-link">← Back</a></p>
  <h1>#{race_number} — {name}</h1>
  <div class="meta">{club} &middot; {gender_label} &middot; {category}</div>

  <div class="badges">
{badges}
  </div>

  <div class="stats">
    <div class="stat"><span class="label">Races completed</span><span class="value">{races_completed}</span></div>
    <div class="stat"><span class="label">Best cat. finish</span><span class="value">{best_cat_position}</span></div>
    <div class="stat"><span class="label">Best points</span><span class="value">{best_points}</span></div>
    <div class="stat"><span class="label">Season average</span><span class="value">{avg_points}</span></div>
    <div class="stat"><span class="label">Pace grade (season avg)</span><span class="value">{avg_pace_grade}</span></div>
    <div class="stat"><span class="label">Laps completed</span><span class="value">{total_laps}</span></div>
    <div class="stat"><span class="label">Time in the saddle</span><span class="value">{total_time}</span></div>
  </div>

{compare_block}

{pace_trend_block}

{team_links_block}

  <table>
    <thead>
      <tr><th>Round</th><th>Location</th><th>Date</th><th>Conds.</th><th>Cat. position</th><th>Points</th><th>Pace grade</th><th>Status</th></tr>
    </thead>
    <tbody>
{rows}
    </tbody>
  </table>
</div>
{iframe_resize_script}
{back_link_script}
</body>
</html>
"""

ROW_TEMPLATE = ('      <tr{row_class}><td>{round}{best_tag}</td><td>{location}</td><td>{date}</td>'
                 '<td>{conditions}</td><td>{cat_pos}{medal}</td><td>{points}</td>'
                 '<td>{pace_grade}{pace_best_tag}</td><td>{status}</td></tr>\n')


def render_team_links_block(club: Optional[str], db_stem: str) -> str:
    """
    A short "Club: X — see Team standings / Participation award" line
    linking to the relevant award page(s) for this rider's source DB.
    Returns "" (nothing rendered) for a rider with no club, or with the
    "No Club/Team" placeholder, since those riders don't appear on the
    award pages anyway.
    """
    club_clean = (club or "").strip()
    if not club_clean or club_clean.strip().lower() == NO_CLUB_PLACEHOLDER:
        return ""

    links = team_award_links_for_db(db_stem)
    if not links:
        return ""

    link_html = " ".join(f'<a href="{esc(vurl(href))}">{esc(label)}</a>' for label, href in links)
    return f'  <div class="team-links">{esc(club_clean)} — {link_html}</div>'


def render_pace_trend_block(stats: Dict) -> str:
    """
    A short, positively-framed season trend line comparing a rider's
    earlier graded rounds to their more recent ones. "" if there isn't
    enough graded history yet (see PACE_TREND_MIN_ROUNDS / pace_grade_
    scoring.pace_grade_trend for the threshold and the split itself).

    All three directions are framed supportively — a "down" trend is
    real information a rider might want, but courses and conditions
    vary week to week, so it's shown as a plain, calm note rather than
    a red flag, and never as the only thing said about their pace.
    """
    trend = stats.get("pace_trend")
    if not trend:
        return ""

    early = trend["early_avg"]
    late = trend["late_avg"]
    direction = trend["direction"]

    if direction == "up":
        text = (
            f'📈 Your pace grade is trending up this season — averaging '
            f'<strong>{late:.0f}%</strong> in recent rounds vs '
            f'<strong>{early:.0f}%</strong> earlier on. Nice work.'
        )
    elif direction == "steady":
        text = (
            f'👍 Your pace grade has been rock steady this season — around '
            f'<strong>{late:.0f}%</strong> recently, much the same as the '
            f'<strong>{early:.0f}%</strong> you were riding at earlier on.'
        )
    else:  # "down"
        text = (
            f'Your pace grade has eased a little this season — '
            f'<strong>{late:.0f}%</strong> in recent rounds vs '
            f'<strong>{early:.0f}%</strong> earlier on. Courses and conditions '
            f'vary a lot week to week, so this is just one signal among many — '
            f'worth a look alongside your best rounds, not a verdict on its own.'
        )

    return f'  <div class="pace-trend {esc(direction)}">{text}</div>'


def render_page(race_number: int, firstname: str, surname: str, gender: str,
                club: str, category: str, history: List[Tuple], stats: Dict,
                pace_grades: Dict[int, Optional[float]],
                round_names: Dict[int, Dict[str, str]], db_stem: str) -> str:
    name = display_name(firstname, surname)

    gender_label = {"M": "Male", "F": "Female"}.get((gender or "").strip().upper(), esc(gender) or "—")

    def round_label(rnd) -> str:
        return f"Round {esc(rnd)}"

    def location_label(rnd) -> str:
        return esc(round_venue_label(rnd, round_names))

    def date_label(rnd) -> str:
        return esc(round_date_label(rnd, round_names))

    def conditions_cell(rnd) -> str:
        icon, label = round_conditions_icon(rnd, round_names)
        if not icon:
            return ""
        return f'<span class="cond" title="{esc(label)}">{icon}</span>'

    medal_rounds = stats["medal_rounds"]
    best_round = stats["best_round"]
    best_pace_round = stats["best_pace_round"]

    rows_html = ""
    if not history:
        rows_html = '      <tr><td colspan="8">No results recorded yet this season.</td></tr>\n'
    else:
        for rnd, cat_pos, overall_pos, points, is_ap, status, laps, time_sec in history:
            if is_ap or points == AP_MARKER:
                status_disp = "AP"
                points_disp = '<span class="ap">avg</span>'
            else:
                status_disp = esc(status or "")
                points_disp = esc(points if points is not None else "")

            medal_html = ""
            if rnd in medal_rounds:
                tier = medal_rounds[rnd]
                medal_html = f'<span class="medal {MEDAL_CLASSES[tier]}">{MEDAL_LABELS[tier]}</span>'

            is_best = (rnd == best_round) and best_round is not None
            row_class = ' class="best-round"' if is_best else ""
            best_tag = '<span class="best-tag">★ Season best</span>' if is_best else ""

            grade = pace_grades.get(rnd)
            pace_grade_disp = format_pct(grade) if (status == "FIN" and not is_ap) else "—"
            is_best_pace = (rnd == best_pace_round) and best_pace_round is not None
            pace_best_tag = (
                '<span class="pace-best-tag">⚡ Best pace</span>' if is_best_pace else ""
            )

            rows_html += ROW_TEMPLATE.format(
                row_class=row_class,
                round=round_label(rnd),
                best_tag=best_tag,
                location=location_label(rnd),
                date=date_label(rnd),
                conditions=conditions_cell(rnd),
                cat_pos=esc(cat_pos if cat_pos is not None else ""),
                medal=medal_html,
                points=points_disp,
                pace_grade=pace_grade_disp,
                pace_best_tag=pace_best_tag,
                status=status_disp,
            )

    # Badges
    badge_parts = []
    if stats["is_newbie"]:
        badge_parts.append('<span class="badge newbie">New rider this season</span>')
    if stats["milestone_badge"]:
        badge_parts.append(f'<span class="badge milestone">{esc(stats["milestone_badge"])}</span>')
    badges_html = "\n".join(f"    {b}" for b in badge_parts) if badge_parts else ""

    # Last-year vs this-year average comparison
    compare_block = ""
    if stats["is_newbie"]:
        compare_block = '  <div class="compare">First season with the league — no prior-year average to compare.</div>'
    elif stats["avg_points_last_year"] is not None and stats["avg_points"] is not None:
        compare_block = (
            '  <div class="compare">'
            f'Season average: <strong>{stats["avg_points"]:.1f}</strong> this year '
            f'vs <strong>{stats["avg_points_last_year"]:.1f}</strong> last year.'
        )
        if stats["category_changed"]:
            compare_block += (
                '<span class="note">Category changed since last year — '
                "this comparison may not be like-for-like.</span>"
            )
        compare_block += "</div>"

    pace_trend_block = render_pace_trend_block(stats)
    team_links_block = render_team_links_block(club, db_stem)

    def fmt(v, decimals=1):
        if v is None:
            return "—"
        if isinstance(v, float):
            return f"{v:.{decimals}f}"
        return str(v)

    return PAGE_TEMPLATE.format(
        race_number=esc(race_number),
        name=esc(name),
        club=esc(club or "—"),
        gender_label=gender_label,
        category=esc(category or "—"),
        badges=badges_html,
        races_completed=stats["races_completed"],
        best_cat_position=fmt(stats["best_cat_position"], 0),
        best_points=fmt(stats["best_points"], 0),
        avg_points=fmt(stats["avg_points"], 1),
        avg_pace_grade=format_pct(stats["avg_pace_grade"]),
        total_laps=stats["total_laps"] if stats["total_laps"] else "—",
        total_time=format_duration(stats["total_time_seconds"]),
        compare_block=compare_block,
        pace_trend_block=pace_trend_block,
        team_links_block=team_links_block,
        rows=rows_html,
        back_href=esc(vurl("../index.html")),
        iframe_resize_script=IFRAME_RESIZE_SCRIPT,
        back_link_script=BACK_LINK_SCRIPT,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Generate static, noindex'd HTML rider pages keyed by race number."
    )
    ap.add_argument("--db", nargs="+", required=True,
                    help="One or more SQLite race DBs (e.g. U8.db U10.db Youth.db ...)")
    ap.add_argument("--outdir", default="rider_pages", help="Output directory (default: rider_pages)")
    ap.add_argument("--rounds", type=int, default=11,
                    help="Number of rounds in the season (default: 11). Also used as the "
                         "'Full House' milestone threshold.")
    ap.add_argument("--rounds-file", default=None,
                    help="Optional CSV mapping round number to name/venue/date/conditions "
                         "(columns: round,name,venue,date,conditions — the same file used for "
                         "set_round_names.py / rounds-template.csv). Shared across "
                         "all category DBs since a round is the same event for everyone.")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    riders_dir = outdir / "riders"
    riders_dir.mkdir(parents=True, exist_ok=True)

    print(f"Build version (page-link cache-busting): {BUILD_VERSION}")

    round_names = load_round_names(args.rounds_file)
    if args.rounds_file:
        if round_names:
            summary = ", ".join(
                f"R{r}={round_venue_label(r, round_names)}"
                + (f" ({round_date_label(r, round_names)})" if round_names[r]["date"] else "")
                + (f" [{round_names[r]['conditions']}]" if round_names[r].get("conditions") else "")
                for r in sorted(round_names.keys())
            )
            print(f"Loaded {len(round_names)} round(s) from {args.rounds_file}: {summary}")
        else:
            print(f"⚠️  No round info loaded from {args.rounds_file} — pages will show plain 'Round N'.")

    admin_rows = []  # race_number, name, club, category, source_db  -- ADMIN ONLY, do not publish
    written = 0
    skipped_no_number = 0

    for db_file in args.db:
        db_path = Path(db_file)
        if not db_path.exists():
            print(f"⚠️  WARNING: DB not found, skipping: {db_path}")
            continue

        db_stem = db_path.stem  # e.g. "U8" from "U8.db" -- used to pick award links

        conn = sqlite3.connect(str(db_path))
        ensure_schema(conn, db_path.name)

        # One query per DB, shared across every rider in it — each
        # (round, pace group) leader pace never changes between riders in
        # that group, so there's no reason to recompute it per rider (and
        # no risk of it drifting either). Keyed by (round, pace group),
        # NOT just round and NOT just race_category — see pace_group_for()
        # and compute_round_leader_paces' docstring: a plain per-round
        # leader was wrong for a DB that bundles more than one age group
        # (U8.db, Youth.db), and a plain per-category leader was ALSO
        # wrong for DBs where several categories share one actual start
        # (Seniors.db, Masters.db, Women.db).
        leader_paces = compute_round_leader_paces(conn, args.rounds, db_stem)

        riders = load_riders(conn)
        print(f"\n{db_path.name}: {len(riders)} riders")

        for (rider_id, race_number, firstname, surname, gender, club_name,
             race_category, race_category_prev, avg_last_year) in riders:

            if race_number is None:
                skipped_no_number += 1
                continue

            history = load_results_for_rider(conn, rider_id, args.rounds)
            pace_grades = compute_rider_pace_grades(history, leader_paces, db_stem, race_category)
            stats = compute_stats(
                history, pace_grades,
                season_length=args.rounds,
                race_category=race_category,
                race_category_previous_year=race_category_prev,
                average_points_last_year=avg_last_year,
            )

            page_html = render_page(
                race_number, firstname, surname, gender, club_name,
                race_category, history, stats, pace_grades, round_names, db_stem,
            )

            out_file = riders_dir / f"{race_number}.html"
            out_file.write_text(page_html, encoding="utf-8")
            written += 1

            admin_rows.append([
                race_number,
                f"{(firstname or '').strip()} {(surname or '').strip()}".strip(),
                club_name or "",
                race_category or "",
                db_path.name,
            ])

        conn.close()

    # robots.txt snippet — merge into the site's existing robots.txt
    robots_path = outdir / "robots.txt.disallow.txt"
    robots_path.write_text(
        "# Merge these lines into the website's existing robots.txt\n"
        "# so search engines never crawl or index individual rider pages.\n"
        "User-agent: *\n"
        "Disallow: /rider_pages/riders/\n",
        encoding="utf-8",
    )

    # Admin-only lookup CSV — internal use, NOT for publishing on the website
    admin_csv_path = outdir / "index_admin.csv"
    with admin_csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["race_number", "full_name", "club_name", "race_category", "source_db"])
        w.writerows(admin_rows)

    print(f"\nDone.")
    print(f"  Rider pages written : {written}  -> {riders_dir}/<race_number>.html")
    print(f"  Skipped (no number) : {skipped_no_number}")
    print(f"  robots.txt snippet  : {robots_path}")
    print(f"  Admin lookup CSV    : {admin_csv_path}  (internal use only — do NOT publish this file)")


if __name__ == "__main__":
    main()
