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

WHAT COUNTS AS A "REAL" RESULT:
    AP (average points) rows and DNF rows are excluded from points/
    position/medal/milestone calculations. DNF rows DO still count
    towards laps completed (they rode those laps) but not towards
    total race time (no reliable finish time exists for a DNF).

ROUND NAMES / VENUES:
    --rounds-file takes the SAME CSV used to populate each DB's `rounds`
    table (see set_round_names.py / rounds-template.csv), with headers:
        round,name,venue,date
    Only 'round' and at least one of name/venue/date are required — any
    of the four can be blank per-row. Older CSVs with just round,name
    (or round,venue / round,location) still work too.

    Each rider's results table shows two separate columns for this,
    kept apart from the "Round" column so nothing is repeated:
        - Location: the venue field (falls back to name if venue is
          blank, e.g. for an older round,name-only CSV)
        - Date: the date field
    The round's "name" (e.g. an event name distinct from its venue) is
    not shown on the rider page — it's only used on the league table
    pages (export_league_tables_html.py) round tooltips/legend.

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
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple


AP_MARKER = 999
MILESTONE_THRESHOLDS = [5, 10]  # "Full House" is handled separately, at season_length


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
        round,name,venue,date

    Returns {round_number: {"name": ..., "venue": ..., "date": ...}}.
    Any of name/venue/date may be blank on a given row. Older CSVs that
    only have round,name (or round,venue / round,location) still work —
    whichever of those columns is present is read as "name" and the
    others default to "".

    Column matching is case/whitespace-insensitive, so a header like
    'Round,Location' or ' Round , Name ' still works.

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


# ---------------------------------------------------------------------------
# Stats & badges
# ---------------------------------------------------------------------------

MEDAL_LABELS = {1: "1st", 2: "2nd", 3: "3rd"}
MEDAL_CLASSES = {1: "gold", 2: "silver", 3: "bronze"}


def compute_stats(history: List[Tuple], *, season_length: int,
                  race_category: Optional[str],
                  race_category_previous_year: Optional[str],
                  average_points_last_year: Optional[float]) -> Dict[str, object]:
    """
    history rows: (round, cat_position, overall_position, points, is_ap,
                    status, laps_completed, finish_time_seconds)

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

    avg_points = (sum(real_points) / len(real_points)) if real_points else None

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
    }


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

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
  .wmccl-rider tr.best-round {{ background: #fff8e1; }}
  .wmccl-rider .best-tag {{ margin-left: 0.4rem; font-size: 0.75rem; color: #8a6d00; font-weight: 600; }}
  .wmccl-rider .compare {{ margin: 0.5rem 0 1.25rem; font-size: 0.9rem; color: #444; }}
  .wmccl-rider .compare .note {{ display: block; font-size: 0.8rem; color: #888; margin-top: 0.2rem; }}
</style>
</head>
<body>
<div class="wmccl-rider">
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
    <div class="stat"><span class="label">Laps completed</span><span class="value">{total_laps}</span></div>
    <div class="stat"><span class="label">Time in the saddle</span><span class="value">{total_time}</span></div>
  </div>

{compare_block}

  <table>
    <thead>
      <tr><th>Round</th><th>Location</th><th>Date</th><th>Cat. position</th><th>Points</th><th>Status</th></tr>
    </thead>
    <tbody>
{rows}
    </tbody>
  </table>
</div>
</body>
</html>
"""

ROW_TEMPLATE = '      <tr{row_class}><td>{round}{best_tag}</td><td>{location}</td><td>{date}</td><td>{cat_pos}{medal}</td><td>{points}</td><td>{status}</td></tr>\n'


def render_page(race_number: int, firstname: str, surname: str, gender: str,
                club: str, category: str, history: List[Tuple], stats: Dict,
                round_names: Dict[int, Dict[str, str]]) -> str:
    name = display_name(firstname, surname)

    gender_label = {"M": "Male", "F": "Female"}.get((gender or "").strip().upper(), esc(gender) or "—")

    def round_label(rnd) -> str:
        return f"Round {esc(rnd)}"

    def location_label(rnd) -> str:
        return esc(round_venue_label(rnd, round_names))

    def date_label(rnd) -> str:
        return esc(round_date_label(rnd, round_names))

    medal_rounds = stats["medal_rounds"]
    best_round = stats["best_round"]

    rows_html = ""
    if not history:
        rows_html = '      <tr><td colspan="6">No results recorded yet this season.</td></tr>\n'
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

            rows_html += ROW_TEMPLATE.format(
                row_class=row_class,
                round=round_label(rnd),
                best_tag=best_tag,
                location=location_label(rnd),
                date=date_label(rnd),
                cat_pos=esc(cat_pos if cat_pos is not None else ""),
                medal=medal_html,
                points=points_disp,
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
        total_laps=stats["total_laps"] if stats["total_laps"] else "—",
        total_time=format_duration(stats["total_time_seconds"]),
        compare_block=compare_block,
        rows=rows_html,
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
                    help="Optional CSV mapping round number to name/venue/date "
                         "(columns: round,name,venue,date — the same file used for "
                         "set_round_names.py / rounds-template.csv). Shared across "
                         "all category DBs since a round is the same event for everyone.")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    riders_dir = outdir / "riders"
    riders_dir.mkdir(parents=True, exist_ok=True)

    round_names = load_round_names(args.rounds_file)
    if args.rounds_file:
        if round_names:
            summary = ", ".join(
                f"R{r}={round_venue_label(r, round_names)}"
                + (f" ({round_date_label(r, round_names)})" if round_names[r]["date"] else "")
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

        conn = sqlite3.connect(str(db_path))
        ensure_schema(conn, db_path.name)

        riders = load_riders(conn)
        print(f"\n{db_path.name}: {len(riders)} riders")

        for (rider_id, race_number, firstname, surname, gender, club_name,
             race_category, race_category_prev, avg_last_year) in riders:

            if race_number is None:
                skipped_no_number += 1
                continue

            history = load_results_for_rider(conn, rider_id, args.rounds)
            stats = compute_stats(
                history,
                season_length=args.rounds,
                race_category=race_category,
                race_category_previous_year=race_category_prev,
                average_points_last_year=avg_last_year,
            )

            page_html = render_page(
                race_number, firstname, surname, gender, club_name,
                race_category, history, stats, round_names,
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
