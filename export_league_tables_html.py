#!/usr/bin/env python3
"""
export_league_tables_html.py
-----------------------------
Generate a static HTML league-tables site from a single SQLite DB — the HTML
counterpart to export_league_tables.py's CSV output. Uses the SAME scoring
logic (league_scoring.py) so the two outputs can never disagree on a rider's
points, best-N total or average.

Run once per DB/profile, same pattern as export_league_tables.py in
produce-lt.sh — e.g. 7 invocations for a full season update:

  python3 export_league_tables_html.py --db U8.db      --profile u8      --outdir league_html
  python3 export_league_tables_html.py --db U10.db     --profile u10     --outdir league_html
  python3 export_league_tables_html.py --db U12.db     --profile u12     --outdir league_html
  python3 export_league_tables_html.py --db Youth.db   --profile youth   --outdir league_html
  python3 export_league_tables_html.py --db Women.db   --profile women   --outdir league_html
  python3 export_league_tables_html.py --db Masters.db --profile masters --outdir league_html
  python3 export_league_tables_html.py --db Seniors.db --profile seniors --outdir league_html \
      --sponsor-logo logo1.png --sponsor-logo logo2.png --sponsor-logo logo3.png

Each invocation writes that profile's table pages and then REBUILDS
index.html from a small on-disk manifest (.manifest.json) that accumulates
across invocations, so index.html always reflects every table written so
far, however many separate runs it took.

OUTPUT (into --outdir, "league_html" in the examples above):
  index.html            season hub: sponsor strip, links to every table
  css/site.css          one shared stylesheet
  assets/sponsors/*     sponsor logo files (copied in via --sponsor-logo)
  tables/<table>.html   one page per league table (sponsor strip at the top,
                         then the table itself); each rider name links to
                         ../riders/<race_number>.html

IMPORTANT: rider pages themselves are NOT built by this script — that's
export_rider_pages.py's job (kept deliberately separate). For the links to
resolve, run both scripts with the SAME --outdir, e.g.:

  python3 export_rider_pages.py --db U8.db ... Seniors.db \
      --outdir league_html --rounds 12 --rounds-file rounds.csv

ROUND NAMES: read from the DB's `rounds` table (round_number, name, venue,
date) if present and populated — see add_rounds_table.py / set_round_names.py.
If that table is missing/empty, columns just show "Round N" with no name.
The --rounds flag still controls how many round columns are scored/shown
(same meaning as in export_league_tables.py) — a season with fewer or more
rounds than 12 just needs --rounds set accordingly and the rounds table
populated to match.

TEAM & CLUB AWARDS: index.html also shows an "Awards" section whenever an
`.awards.json` cache is present in --outdir (written by
export_team_awards_html.py — see that script's docstring). This script
never writes that cache itself; it only reads it back (if present) each
time it rebuilds index.html, so running the two generators in either order
always leaves index.html showing whatever's actually been built so far.
"""

import argparse
import html
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from league_scoring import (
    best_n_sum,
    effective_points_for_round,
    profile_tables,
    rider_avg_this_season,
    safe_float,
    safe_int,
)


AP_MARKER = 999
MEDAL_CLASSES = {1: "gold", 2: "silver", 3: "bronze"}

# Display order for grouping tables on the index page. Only profiles that
# actually appear in the manifest are shown.
PROFILE_ORDER = ["u8", "u10", "u12", "youth", "women", "seniors", "masters"]
PROFILE_LABELS = {
    "u8": "Under 8",
    "u10": "Under 10",
    "u12": "Under 12",
    "youth": "Youth (U14/U16)",
    "women": "Women",
    "seniors": "Seniors",
    "masters": "Masters",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def esc(x) -> str:
    return html.escape(str(x if x is not None else ""))


def surname_initial(surname: Optional[str]) -> str:
    s = (surname or "").strip()
    return f"{s[0].upper()}." if s else ""


def display_name(firstname: Optional[str], surname: Optional[str]) -> str:
    """
    First name + surname initial (e.g. "Jimmy S.") — same privacy-conscious
    convention export_rider_pages.py already uses on the public rider pages,
    so the league table doesn't show a fuller name than the rider's own page
    does. Also handy for keeping the table narrower on a small screen.
    """
    first = (firstname or "").strip()
    init = surname_initial(surname)
    return f"{first} {init}".strip()


def truncate(text: Optional[str], max_len: int = 25) -> str:
    """
    Shortens a long display string (e.g. a club/team name like "Team Mi
    Racing Townsend Vehicle Hire") to at most max_len characters, appending
    an ellipsis when it's cut. Strings already within the limit are
    returned unchanged. The full, untruncated text is still shown as a
    hover tooltip wherever this is used, so nothing is actually lost.
    """
    s = (text or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


# ---------------------------------------------------------------------------
# DB loading
# ---------------------------------------------------------------------------

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='riders'")
    if not cur.fetchone():
        raise SystemExit("❌ DB missing 'riders' table")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results'")
    if not cur.fetchone():
        raise SystemExit("❌ DB missing 'results' table")


def load_riders(conn: sqlite3.Connection) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, race_number, firstname, surname, club_name, race_category, IBX
        FROM riders
        ORDER BY race_number
    """)
    return cur.fetchall()


def load_results(conn: sqlite3.Connection, rounds: int) -> Dict[Tuple[int, int], Tuple[Optional[float], int]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT rider_id, round, points, is_ap
        FROM results
        WHERE round BETWEEN 1 AND ?
    """, (rounds,))
    out = {}
    for rider_id, rnd, pts, is_ap in cur.fetchall():
        out[(rider_id, rnd)] = (safe_float(pts), safe_int(is_ap, 0) or 0)
    return out


def load_rounds(conn: sqlite3.Connection) -> Dict[int, Dict[str, str]]:
    """
    Returns {round_number: {"name": ..., "venue": ..., "date": ...}} from the
    DB's `rounds` table, or {} if the table doesn't exist / is empty.
    """
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rounds'")
    if not cur.fetchone():
        return {}
    cur.execute("SELECT round_number, name, venue, date FROM rounds ORDER BY round_number")
    out = {}
    for round_number, name, venue, date in cur.fetchall():
        out[round_number] = {
            "name": (name or "").strip(),
            "venue": (venue or "").strip(),
            "date": (date or "").strip(),
        }
    return out


def round_display_label(rnd: int, rounds_info: Dict[int, Dict[str, str]]) -> Tuple[str, str]:
    """
    Returns (short_header, title_tooltip) for a round column, e.g.
    ("R1", "Round 1 — Sutton Park (2026-10-04)").
    """
    info = rounds_info.get(rnd)
    short = f"R{rnd}"
    if not info:
        return short, f"Round {rnd}"
    parts = [info["name"] or f"Round {rnd}"]
    if info["venue"]:
        parts.append(f"— {info['venue']}")
    if info["date"]:
        parts.append(f"({info['date']})")
    return short, " ".join(parts)


# ---------------------------------------------------------------------------
# Scoring -> row data (mirrors export_league_tables.py's export_table, but
# builds row dicts for HTML rendering instead of writing CSV rows)
# ---------------------------------------------------------------------------

def compute_table_rows(riders_subset: List[Tuple],
                       results: Dict[Tuple[int, int], Tuple[Optional[float], int]],
                       rounds: int,
                       best_n: int,
                       upto_round: int,
                       avg_decimals: int) -> List[Dict]:
    avgs = {rider_id: rider_avg_this_season(rider_id, results, upto_round)
            for (rider_id, *_rest) in riders_subset}

    rows = []
    for rider_id, race_no, first, last, club, cat, one_bx in riders_subset:
        avg = avgs.get(rider_id)

        eff_points_for_totals: List[float] = []
        total_points = 0.0
        per_round = []  # list of (display_value, is_ap)
        ap_rounds_list: List[int] = []

        for rnd in range(1, rounds + 1):
            eff, ap_flag = effective_points_for_round(rider_id, rnd, results, avg)
            if ap_flag:
                ap_rounds_list.append(rnd)

            if eff is None:
                per_round.append((None, False))
                continue

            eff_f = float(eff)
            eff_points_for_totals.append(eff_f)
            total_points += eff_f

            if ap_flag:
                per_round.append((round(eff_f, avg_decimals), True))
            else:
                per_round.append((int(round(eff_f, 0)), False))

        best_total = best_n_sum(eff_points_for_totals, best_n) if eff_points_for_totals else 0.0

        rows.append({
            "race_number": race_no,
            "name": display_name(first, last),
            "category": cat,
            "club": club or "",
            "one_bx": (one_bx or "").strip(),
            "best_total": int(round(best_total, 0)),
            "avg": None if avg is None else round(avg, avg_decimals),
            "per_round": per_round,
            "ap_rounds": ap_rounds_list,
            "_sort_best": best_total,
            "_sort_total": total_points,
            "_sort_last": last.lower(),
            "_sort_first": first.lower(),
        })

    rows.sort(key=lambda r: (-r["_sort_best"], -r["_sort_total"], r["_sort_last"], r["_sort_first"]))
    for i, r in enumerate(rows, start=1):
        r["position"] = i
    return rows


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

SITE_CSS = """
:root {
  --wmccl-bg: #ffffff;
  --wmccl-fg: #222;
  --wmccl-muted: #666;
  --wmccl-border: #ddd;
  --wmccl-stripe: #f7f7f7;
  --wmccl-header-bg: #f5f5f5;
  --wmccl-link: #1f4e79;
  --wmccl-gold-bg: #f4c430; --wmccl-gold-fg: #4a3900;
  --wmccl-silver-bg: #cfd4d8; --wmccl-silver-fg: #333;
  --wmccl-bronze-bg: #d8935a; --wmccl-bronze-fg: #3a2200;
}
body.wmccl-league {
  font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
  color: var(--wmccl-fg);
  background: var(--wmccl-bg);
  max-width: 980px;
  margin: 0 auto;
  padding: 1rem;
}
.wmccl-league h1 { font-size: 1.5rem; margin-bottom: 0.2rem; }
.wmccl-league h2 { font-size: 1.1rem; margin-top: 2rem; }
.wmccl-league a { color: var(--wmccl-link); }
.wmccl-league .breadcrumb { margin-bottom: 1rem; font-size: 0.9rem; }
.wmccl-league .sponsors { display: flex; flex-wrap: wrap; gap: 1.5rem; align-items: center; margin: 1rem 0 1.5rem; }
.wmccl-league .sponsors img { max-height: 60px; max-width: 160px; }
.wmccl-league .index-row { display: flex; align-items: baseline; flex-wrap: wrap; gap: 0.4rem 0.75rem; margin: 0.5rem 0; }
.wmccl-league .index-row + .index-row { border-top: 1px solid var(--wmccl-border); padding-top: 0.5rem; }
.wmccl-league .index-row h2 { margin: 0; font-size: 1rem; white-space: nowrap; flex-shrink: 0; }
.wmccl-league .table-links { list-style: none; padding: 0; display: flex; flex-wrap: wrap; gap: 0.5rem; margin: 0; }
.wmccl-league .table-links li a { display: inline-block; background: var(--wmccl-header-bg); border-radius: 6px; padding: 0.3rem 0.7rem; text-decoration: none; font-size: 0.85rem; }
.wmccl-league .table-links li a:hover { background: #eef3f9; }
.wmccl-league table { width: 100%; border-collapse: collapse; margin: 1rem 0; font-size: 0.9rem; }
.wmccl-league th, .wmccl-league td { text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid var(--wmccl-border); white-space: nowrap; }
.wmccl-league th { background: var(--wmccl-header-bg); position: sticky; top: 0; }
.wmccl-league tbody tr:nth-child(even) { background: var(--wmccl-stripe); }
.wmccl-league .pos { font-weight: 600; }
.wmccl-league .medal { display: inline-block; min-width: 1.5rem; text-align: center; border-radius: 999px; padding: 0.05rem 0.4rem; font-weight: 700; }
.wmccl-league .medal.gold { background: var(--wmccl-gold-bg); color: var(--wmccl-gold-fg); }
.wmccl-league .medal.silver { background: var(--wmccl-silver-bg); color: var(--wmccl-silver-fg); }
.wmccl-league .medal.bronze { background: var(--wmccl-bronze-bg); color: var(--wmccl-bronze-fg); }
.wmccl-league .best-total { font-weight: 700; }
.wmccl-league .ap-cell { color: #888; font-style: italic; }
.wmccl-league .empty-cell { color: #ccc; }
.wmccl-league .ap-note { color: #888; font-size: 0.85rem; }
.wmccl-league footer { margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--wmccl-border); font-size: 0.8rem; color: var(--wmccl-muted); }
"""

# Auto-resizes the iframe this page is embedded in (e.g. a WordPress page
# with an <iframe src="/league_html/..."> so the page keeps the site's
# header/footer). Only does anything when the page is actually loaded
# inside a SAME-ORIGIN iframe (window.frameElement is only reachable
# same-origin) — visiting the page directly, or a cross-origin embed,
# leaves it a harmless no-op. Re-checks on load, on pageshow (this is the
# one that matters for browser back/forward — a back-navigation restores
# the page without firing 'load', so without a pageshow handler the iframe
# gets stuck at whatever height the page you navigated away from needed),
# on window resize, and shortly after load to catch late-loading
# fonts/images shifting the page height.
#
# Shrinking is a special case: document.documentElement.scrollHeight is
# defined as never smaller than the iframe's OWN current viewport height,
# so if we don't first collapse the iframe back towards zero before
# re-measuring, going from a tall page (a big table) to a shorter one (the
# index, or a smaller table) would read the still-inflated old height back
# as "content height" and the iframe would never shrink — you'd click back
# from a big table and see a large empty gap where that table used to be.
# Resetting to 0px immediately before measuring fixes that.
#
# That reset is itself a real height change, so it triggers this script's
# own 'resize' listener (the iframe's own viewport just changed) — the
# `busy` guard stops that from re-entering resize() and looping.
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
      window.addEventListener('load', resize);
      window.addEventListener('pageshow', resize);
      window.addEventListener('resize', resize);
      setTimeout(resize, 300);
      resize();
    }
  } catch (e) { /* cross-origin or no iframe context - ignore */ }
})();
</script>"""


def render_sponsor_strip(sponsors: List[Dict[str, str]], assets_prefix: str) -> str:
    """
    assets_prefix: relative path from the page to the outdir, e.g. "" for
    index.html (assets live at assets/sponsors/) or "../" for a page inside
    tables/ or awards/ (assets live at ../assets/sponsors/).
    """
    if not sponsors:
        return ""
    items = []
    for s in sponsors:
        img = f'<img src="{assets_prefix}assets/sponsors/{esc(s["filename"])}" alt="Sponsor logo">'
        if s.get("link"):
            img = f'<a href="{esc(s["link"])}" target="_blank" rel="noopener">{img}</a>'
        items.append(img)
    return f'<div class="sponsors">{"".join(items)}</div>'


def render_table_page(table_name: str, profile: str, rows: List[Dict],
                      rounds: int, best_n: int, rounds_info: Dict[int, Dict[str, str]],
                      sponsors: List[Dict[str, str]]) -> str:
    sponsor_html = render_sponsor_strip(sponsors, "../")
    round_headers = []
    for rnd in range(1, rounds + 1):
        short, title = round_display_label(rnd, rounds_info)
        round_headers.append(
            f'<th title="{esc(title)}">{esc(short)}</th>'
        )

    body_rows = []
    for r in rows:
        pos = r["position"]
        if pos in MEDAL_CLASSES:
            pos_html = f'<span class="medal {MEDAL_CLASSES[pos]}">{pos}</span>'
        else:
            pos_html = f'<span class="pos">{pos}</span>'

        name_link = f'<a href="../riders/{esc(r["race_number"])}.html">{esc(r["name"])}</a>'

        round_cells = []
        for value, is_ap in r["per_round"]:
            if value is None:
                round_cells.append('<td class="empty-cell">—</td>')
            elif is_ap:
                round_cells.append(f'<td class="ap-cell">{esc(value)}</td>')
            else:
                round_cells.append(f"<td>{esc(value)}</td>")

        ap_note = ""
        if r["ap_rounds"]:
            ap_note = '<span class="ap-note">AP: ' + ", ".join(f"R{n}" for n in r["ap_rounds"]) + "</span>"

        avg_disp = "—" if r["avg"] is None else esc(r["avg"])

        club_full = (r["club"] or "").strip()
        club_disp = truncate(club_full, 25)
        if club_disp != club_full:
            club_cell = f'<td title="{esc(club_full)}">{esc(club_disp)}</td>'
        else:
            club_cell = f"<td>{esc(club_disp)}</td>"

        body_rows.append(
            "<tr>"
            f"<td>{pos_html}</td>"
            f"<td>{name_link}</td>"
            f"<td>{esc(r['race_number'])}</td>"
            f"<td>{esc(r['category'])}</td>"
            f"{club_cell}"
            f"<td>{esc(r['one_bx'])}</td>"
            f"<td class=\"best-total\">{esc(r['best_total'])}</td>"
            f"<td>{avg_disp}</td>"
            + "".join(round_cells)
            + f"<td>{ap_note}</td>"
            "</tr>"
        )

    rounds_legend = ", ".join(
        f"R{rnd} = {esc(round_display_label(rnd, rounds_info)[1])}"
        for rnd in range(1, rounds + 1) if rnd in rounds_info
    )
    rounds_legend_html = f'<p class="ap-note">{rounds_legend}</p>' if rounds_legend else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(table_name)} — WMCCL League Table</title>
<link rel="stylesheet" href="../css/site.css">
</head>
<body class="wmccl-league">
  <p class="breadcrumb"><a href="../index.html">← All categories</a></p>
  {sponsor_html}
  <h1>{esc(table_name)}</h1>
  <p class="ap-note">Best {best_n} of {rounds} rounds count. Points shown in <em>italics</em> are AP (average points).</p>
  {rounds_legend_html}
  <table>
    <thead>
      <tr>
        <th>Pos</th><th>Name</th><th>No.</th><th>Category</th><th>Club</th><th>1BX</th>
        <th>Best {best_n}</th><th>Avg</th>
        {''.join(round_headers)}
        <th>AP rounds</th>
      </tr>
    </thead>
    <tbody>
      {''.join(body_rows) if body_rows else '<tr><td colspan="99">No riders in this table.</td></tr>'}
    </tbody>
  </table>
  <footer>WMCCL League Tables — generated by export_league_tables_html.py</footer>
  {IFRAME_RESIZE_SCRIPT}
</body>
</html>
"""


def render_index_page(manifest: Dict[str, Dict],
                      sponsors: List[Dict[str, str]], site_title: str,
                      awards: Optional[List[Dict[str, str]]] = None) -> str:
    sponsor_html = render_sponsor_strip(sponsors, "")

    # Each category is one compact row: heading and its table links share a
    # single line (flex row), rather than the heading sitting on its own
    # line above a link list below it — cuts a lot of vertical space when
    # there are 7+ profiles. The redundant standalone "League tables"
    # heading that used to sit above all of these was dropped too (2026-09-19,
    # Adam's request) — the page's own <h1> already says what this is.
    tables_html = ""
    for profile in PROFILE_ORDER:
        entries = {name: meta for name, meta in manifest.items() if meta.get("profile") == profile}
        if not entries:
            continue
        label = PROFILE_LABELS.get(profile, profile)
        links = "".join(
            f'<li><a href="tables/{esc(name)}.html">{esc(name)} ({meta.get("riders", 0)})</a></li>'
            for name, meta in sorted(entries.items())
        )
        tables_html += f'<div class="index-row"><h2>{esc(label)}</h2><ul class="table-links">{links}</ul></div>'

    # Any tables whose profile isn't in PROFILE_ORDER (shouldn't normally happen)
    leftover = {name: meta for name, meta in manifest.items() if meta.get("profile") not in PROFILE_ORDER}
    if leftover:
        links = "".join(
            f'<li><a href="tables/{esc(name)}.html">{esc(name)} ({meta.get("riders", 0)})</a></li>'
            for name, meta in sorted(leftover.items())
        )
        tables_html += f'<div class="index-row"><h2>Other</h2><ul class="table-links">{links}</ul></div>'

    # Team & club awards — same one-line-per-heading treatment, populated
    # from the .awards.json cache written by export_team_awards_html.py (if
    # that script has been run at all; otherwise this row is simply omitted).
    awards_html = ""
    if awards:
        award_links = "".join(
            f'<li><a href="{esc(a["href"])}">{esc(a["label"])}</a></li>'
            for a in awards
        )
        awards_html = f'<div class="index-row"><h2>Team &amp; Club Awards</h2><ul class="table-links">{award_links}</ul></div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(site_title)}</title>
<link rel="stylesheet" href="css/site.css">
</head>
<body class="wmccl-league">
  <h1>{esc(site_title)}</h1>
  {sponsor_html}
  {tables_html if tables_html else '<p>No tables generated yet.</p>'}
  {awards_html}
  <footer>WMCCL League Tables — generated by export_league_tables_html.py</footer>
  {IFRAME_RESIZE_SCRIPT}
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Manifest / caches
# ---------------------------------------------------------------------------

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Export league tables as a static HTML site (see module docstring).")
    ap.add_argument("--db", required=True, help="SQLite DB (e.g. U8.db, Youth.db, Women.db, Seniors.db)")
    ap.add_argument("--profile", required=True,
                    choices=["u8", "u10", "u12", "youth", "women", "seniors", "masters"],
                    help="Grouping rules matching your DB/race")
    ap.add_argument("--outdir", default="league_html", help="Site output directory (default: league_html)")
    ap.add_argument("--rounds", type=int, default=12, help="Number of rounds columns to score/show (default: 12)")
    ap.add_argument("--best", type=int, default=10, help="Best N results (default: 10)")
    ap.add_argument("--upto-round", type=int, default=None,
                    help="Compute averages using results up to this round (inclusive). "
                         "Default: max round present in DB (capped by --rounds).")
    ap.add_argument("--avg-decimals", type=int, default=1, help="Decimals for displayed average (default 1)")
    ap.add_argument("--skip-empty", action="store_true", default=True,
                    help="Skip writing pages for tables with 0 riders (default: on).")
    ap.add_argument("--site-title", default="WMCCL League Tables", help="Heading on index.html")
    ap.add_argument("--sponsor-logo", action="append", default=[],
                    help="Path to a sponsor logo image to copy into assets/sponsors/ (repeatable). "
                         "Overwrites the sponsor list cache if given; omit on later runs to keep it.")
    ap.add_argument("--sponsor-link", action="append", default=[],
                    help="URL for the sponsor logo at the same position (repeatable, optional, paired by order).")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")

    outdir = Path(args.outdir)
    tables_dir = outdir / "tables"
    css_dir = outdir / "css"
    sponsors_dir = outdir / "assets" / "sponsors"
    manifest_path = outdir / ".manifest.json"
    rounds_cache_path = outdir / ".rounds.json"
    sponsors_cache_path = outdir / ".sponsors.json"
    awards_cache_path = outdir / ".awards.json"

    tables_dir.mkdir(parents=True, exist_ok=True)
    css_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)

    cur = conn.cursor()
    cur.execute("SELECT MAX(round) FROM results")
    max_round = cur.fetchone()[0] or 0
    upto_round = args.upto_round if args.upto_round is not None else max_round
    if upto_round > args.rounds:
        upto_round = args.rounds

    riders = load_riders(conn)
    results = load_results(conn, args.rounds)
    rounds_info = load_rounds(conn)
    conn.close()

    # Rounds cache: only overwrite if this DB actually had rounds data,
    # so a DB without a populated rounds table doesn't blank out the index.
    rounds_cache = load_json(rounds_cache_path, {})
    if rounds_info:
        rounds_cache = {str(k): v for k, v in rounds_info.items()}
        save_json(rounds_cache_path, rounds_cache)
    rounds_info_for_index = {int(k): v for k, v in rounds_cache.items()}

    # Sponsors: only touch the cache if --sponsor-logo was given this run.
    sponsors_cache = load_json(sponsors_cache_path, [])
    if args.sponsor_logo:
        sponsors_dir.mkdir(parents=True, exist_ok=True)
        sponsors_cache = []
        for i, logo_path_str in enumerate(args.sponsor_logo):
            logo_path = Path(logo_path_str)
            if not logo_path.exists():
                print(f"⚠️  WARNING: sponsor logo not found, skipping: {logo_path}")
                continue
            dest = sponsors_dir / logo_path.name
            shutil.copy(logo_path, dest)
            link = args.sponsor_link[i] if i < len(args.sponsor_link) else None
            sponsors_cache.append({"filename": logo_path.name, "link": link})
        save_json(sponsors_cache_path, sponsors_cache)

    # Shared CSS (always rewritten — cheap, keeps it current if the script changes)
    (css_dir / "site.css").write_text(SITE_CSS, encoding="utf-8")

    tables = profile_tables(args.profile)
    manifest = load_json(manifest_path, {})

    print(f"\nDB: {db_path.name}")
    print(f"Profile: {args.profile}")
    print(f"Rounds shown: 1..{args.rounds}")
    print(f"Averages computed up to round: {upto_round} (max in DB was {max_round})")
    print(f"Best {args.best} scoring")
    print(f"Output dir: {outdir}\n")

    for table_name, cat_list in tables.items():
        if cat_list == ["*"]:
            subset = riders
        else:
            cat_set = set(cat_list)
            subset = [r for r in riders if (r[5] or "") in cat_set]

        if args.skip_empty and not subset:
            print(f"  Skipped {table_name}.html  (0 riders)")
            manifest.pop(table_name, None)
            continue

        rows = compute_table_rows(subset, results, args.rounds, args.best, upto_round, args.avg_decimals)
        page_html = render_table_page(table_name, args.profile, rows, args.rounds, args.best, rounds_info_for_index, sponsors_cache)
        (tables_dir / f"{table_name}.html").write_text(page_html, encoding="utf-8")

        manifest[table_name] = {
            "profile": args.profile,
            "db": db_path.name,
            "riders": len(subset),
        }
        print(f"  Wrote {table_name}.html  ({len(subset)} riders)")

    save_json(manifest_path, manifest)

    awards_cache = load_json(awards_cache_path, [])
    index_html = render_index_page(manifest, sponsors_cache, args.site_title, awards=awards_cache)
    (outdir / "index.html").write_text(index_html, encoding="utf-8")
    print(f"\n  Wrote index.html  ({len(manifest)} table(s) total across all runs so far)")

    print("\nDone.")


if __name__ == "__main__":
    main()
