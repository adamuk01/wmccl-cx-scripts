#!/usr/bin/env python3
"""
export_team_awards_html.py
----------------------------
Generate the three WMCCL team/club award pages as static HTML, alongside
the category league tables and rider pages in the same site (--outdir).
Uses the SAME club-ranking logic (team_awards_scoring.py) as the existing
CSV scripts (team_points_multi.py, club_completed_rides_multi.py) so none
of the three outputs can ever disagree on a club's standing.

Unlike export_league_tables_html.py, this is a single-pass, single-
invocation script: every award is inherently cross-category (all the
relevant DBs combined at once), so there's no per-profile manifest/cache
system needed here — just one run each time results change.

  python3 export_team_awards_html.py --outdir league_html

By default this looks for the 7 usual DB filenames (U8.db, U10.db, U12.db,
Youth.db, Women.db, Masters.db, Seniors.db) in the current directory,
matching run-team-awards-results.sh's own assumption. Override any of them
with --u8-db / --u10-db / etc. if your filenames differ.

THE THREE AWARDS (names confirmed with Adam 2026-09-19):
  - "Team competition"                 — adult/senior teams, U14 and above
                                          (Women + Seniors + Masters + Youth)
  - "U12 team competition"             — youth teams, U12s and below
                                          (U8 + U10 + U12)
  - "Mick Ives Participation Award"    — completed rides, every category

OUTPUT (into --outdir, same site as export_league_tables_html.py /
export_rider_pages.py):
  awards/team-competition.html
  awards/u12-team-competition.html
  awards/participation.html
  .awards.json           cache read back by export_league_tables_html.py
                          (and by this script itself) so index.html always
                          shows an "Awards" section once this has been run,
                          regardless of which generator runs last.

This script also rewrites index.html itself (using the SAME
render_index_page() as export_league_tables_html.py, imported from it) so
the Awards section appears immediately, without needing a table script
re-run. It reads back whatever .manifest.json / .sponsors.json already
exist in --outdir so it doesn't blank out tables or sponsors that were
already there.
"""

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

from team_awards_scoring import compute_completed_rides_multi, compute_team_points_multi
from export_league_tables_html import (
    MEDAL_CLASSES,
    SITE_CSS,
    IFRAME_RESIZE_SCRIPT,
    esc,
    load_json,
    render_index_page,
    render_sponsor_strip,
    save_json,
    truncate,
)


# ---------------------------------------------------------------------------
# Award definitions — key, output filename, display title/description, and
# which value column to show.
# ---------------------------------------------------------------------------

AWARD_ORDER = ["team", "u12_team", "participation"]

AWARD_META = {
    "team": {
        "filename": "team-competition.html",
        "title": "Team Competition",
        "value_label": "Points",
        "description": f"Top 6 scoring riders from each club, combined per round, summed across the season. "
                        f"Women, Seniors, Masters and Youth (U14/U16) categories combined.",
    },
    "u12_team": {
        "filename": "u12-team-competition.html",
        "title": "U12 Team Competition",
        "value_label": "Points",
        "description": "Top 6 scoring riders from each club, combined per round, summed across the season. "
                        "Under 8, Under 10 and Under 12 categories combined.",
    },
    "participation": {
        "filename": "participation.html",
        "title": "Mick Ives Participation Award",
        "value_label": "Completed Rides",
        "description": "Total completed (finished, non-average-points) rides per club, across every category.",
    },
}


def render_award_page(award_key: str, rows: List[Tuple[str, float]],
                      top_n: int, sponsors: List[Dict[str, str]]) -> str:
    meta = AWARD_META[award_key]
    sponsor_html = render_sponsor_strip(sponsors, "../")

    body_rows = []
    for i, (club, value) in enumerate(rows, start=1):
        if i in MEDAL_CLASSES:
            pos_html = f'<span class="medal {MEDAL_CLASSES[i]}">{i}</span>'
        else:
            pos_html = f'<span class="pos">{i}</span>'

        club_full = (club or "").strip()
        club_disp = truncate(club_full, 25)
        if club_disp != club_full:
            club_cell = f'<td title="{esc(club_full)}">{esc(club_disp)}</td>'
        else:
            club_cell = f"<td>{esc(club_disp)}</td>"

        display_value = int(value) if float(value).is_integer() else value
        body_rows.append(
            "<tr>"
            f"<td>{pos_html}</td>"
            f"{club_cell}"
            f'<td class="best-total">{esc(display_value)}</td>'
            "</tr>"
        )

    top_n_note = (
        f'<p class="ap-note">Top {top_n} scoring riders per club per round count.</p>'
        if award_key in ("team", "u12_team") else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(meta['title'])} — WMCCL Team Awards</title>
<link rel="stylesheet" href="../css/site.css">
</head>
<body class="wmccl-league">
  <p class="breadcrumb"><a href="../index.html">← All categories</a></p>
  {sponsor_html}
  <h1>{esc(meta['title'])}</h1>
  <p class="ap-note">{esc(meta['description'])}</p>
  {top_n_note}
  <table>
    <thead>
      <tr><th>Pos</th><th>Club</th><th>{esc(meta['value_label'])}</th></tr>
    </thead>
    <tbody>
      {''.join(body_rows) if body_rows else '<tr><td colspan="3">No results yet.</td></tr>'}
    </tbody>
  </table>
  <footer>WMCCL Team Awards — generated by export_team_awards_html.py</footer>
  {IFRAME_RESIZE_SCRIPT}
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser(description="Export the 3 WMCCL team/club award pages as static HTML.")
    ap.add_argument("--u8-db", default="U8.db")
    ap.add_argument("--u10-db", default="U10.db")
    ap.add_argument("--u12-db", default="U12.db")
    ap.add_argument("--youth-db", default="Youth.db")
    ap.add_argument("--women-db", default="Women.db")
    ap.add_argument("--seniors-db", default="Seniors.db")
    ap.add_argument("--masters-db", default="Masters.db")
    ap.add_argument("--outdir", default="league_html", help="Site output directory (default: league_html)")
    ap.add_argument("--top-n", type=int, default=6,
                    help="Top N scoring riders per club per round for the two team competitions (default: 6)")
    ap.add_argument("--exclude-club", action="append", default=["No Club/Team"], help="Exclude club (repeatable)")
    ap.add_argument("--site-title", default="WMCCL League Tables",
                    help="Heading on index.html (keep in sync with export_league_tables_html.py's --site-title)")
    args = ap.parse_args()

    youth_dbs = [args.u8_db, args.u10_db, args.u12_db]
    adult_dbs = [args.women_db, args.seniors_db, args.masters_db, args.youth_db]
    all_dbs = [args.u8_db, args.u10_db, args.u12_db, args.youth_db, args.women_db, args.seniors_db, args.masters_db]

    for db in all_dbs:
        if not Path(db).exists():
            raise SystemExit(f"❌ DB not found: {db}")

    outdir = Path(args.outdir)
    awards_dir = outdir / "awards"
    css_dir = outdir / "css"
    awards_dir.mkdir(parents=True, exist_ok=True)
    css_dir.mkdir(parents=True, exist_ok=True)

    # Shared CSS — always rewritten (cheap), same as export_league_tables_html.py,
    # so this script also works standalone before any table pages exist yet.
    (css_dir / "site.css").write_text(SITE_CSS, encoding="utf-8")

    sponsors_cache = load_json(outdir / ".sponsors.json", [])
    manifest = load_json(outdir / ".manifest.json", {})

    print("Computing Team Competition (adult/senior, U14 and above)...")
    team_rows = compute_team_points_multi(adult_dbs, top_n=args.top_n, exclude_clubs=args.exclude_club)

    print("Computing U12 Team Competition (U12 and below)...")
    u12_team_rows = compute_team_points_multi(youth_dbs, top_n=args.top_n, exclude_clubs=args.exclude_club)

    print("Computing Mick Ives Participation Award (all categories)...")
    participation_rows = compute_completed_rides_multi(all_dbs, exclude_clubs=args.exclude_club)

    results_by_key = {
        "team": team_rows,
        "u12_team": u12_team_rows,
        "participation": participation_rows,
    }

    awards_cache = []
    for key in AWARD_ORDER:
        meta = AWARD_META[key]
        rows = results_by_key[key]
        page_html = render_award_page(key, rows, args.top_n, sponsors_cache)
        (awards_dir / meta["filename"]).write_text(page_html, encoding="utf-8")
        awards_cache.append({"label": meta["title"], "href": f"awards/{meta['filename']}"})
        print(f"  Wrote awards/{meta['filename']}  ({len(rows)} clubs)")

    save_json(outdir / ".awards.json", awards_cache)

    index_html = render_index_page(manifest, sponsors_cache, args.site_title, awards=awards_cache)
    (outdir / "index.html").write_text(index_html, encoding="utf-8")
    print("\n  Wrote index.html (with Awards section)")

    print("\nDone.")


if __name__ == "__main__":
    main()
