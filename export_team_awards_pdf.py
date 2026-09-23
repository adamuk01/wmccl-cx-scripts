#!/usr/bin/env python3
"""
export_team_awards_pdf.py
-------------------------
Printable PDF versions of the three WMCCL team/club awards — the PDF
counterpart to export_team_awards_html.py. Uses the SAME club-ranking code
(team_awards_scoring.py) and the same award names/descriptions
(AWARD_META, imported from export_team_awards_html.py), so the PDF, HTML
and CSV awards can never disagree on a club's standing.

Styling matches the league-table PDFs (export_league_tables_pdf.py): navy
title with the season, grey header row. Only the WMCCL logo is shown — no
sponsor logos on the award PDFs (Adam, 2026-09-23). The award tables only
have three columns (Pos, Club, Points / Rides Counted) and can be long, so
these are A4 PORTRAIT rather than landscape; long tables run onto further
pages with the header row repeated. The top
three clubs get a gold/silver/bronze tint on their position, like the
medals on the HTML award pages.

Single invocation, same as the HTML awards script — every award combines
several DBs at once:

  export_team_awards_pdf.py --outdir league_pdf --top-n 6 \\
      --exclude-club "No Club/Team" \\
      --league-logo ../sponsors/WMCCL.png

By default it looks for U8.db, U10.db, U12.db, Youth.db, Women.db,
Masters.db and Seniors.db in the current directory (override with --u8-db
etc.), exactly like export_team_awards_html.py.

OUTPUT (into --outdir, same folder as the league-table PDFs):
  team-competition.pdf
  u12-team-competition.pdf
  participation.pdf          (Mick Ives Participation Award)

Clubs only — no rider names appear on these PDFs.
"""

import argparse
import datetime as dt
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from team_awards_scoring import compute_completed_rides_multi, compute_team_points_multi
from export_team_awards_html import AWARD_META, AWARD_ORDER
from export_league_tables_pdf import (FONT, FONT_BOLD, GRID, HEADER_FILL, STRIPE_FILL, WMCCL_NAVY,
                                      current_season, fit_text, scaled_image)

PDF_FILENAMES = {
    "team": "team-competition.pdf",
    "u12_team": "u12-team-competition.pdf",
    "participation": "participation.pdf",
}

MEDAL_FILLS = {
    1: colors.HexColor("#F4C430"),   # gold   (same colours as the HTML medals)
    2: colors.HexColor("#CFD4D8"),   # silver
    3: colors.HexColor("#D8935A"),   # bronze
}

BODY_SIZE = 9.5


def max_round(db_paths: List[str]) -> int:
    best = 0
    for p in db_paths:
        conn = sqlite3.connect(p)
        try:
            best = max(best, conn.execute("SELECT MAX(round) FROM results").fetchone()[0] or 0)
        finally:
            conn.close()
    return best


def build_award_table(rows: List[Tuple[str, float]], value_label: str, avail_w: float) -> Table:
    head = ParagraphStyle("head", fontName=FONT_BOLD, fontSize=9, leading=11, alignment=TA_CENTER)
    head_left = ParagraphStyle("headl", parent=head, alignment=TA_LEFT)

    table_w = min(avail_w, 150 * mm)
    col_widths = [20 * mm, table_w - 20 * mm - 32 * mm, 32 * mm]
    club_w = col_widths[1] - 8

    data = [[Paragraph("Position", head), Paragraph("Club", head_left), Paragraph(value_label, head)]]
    style = [
        ("FONT", (0, 1), (-1, -1), FONT, BODY_SIZE),
        ("FONT", (2, 1), (2, -1), FONT_BOLD, BODY_SIZE),
        ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("ALIGN", (2, 1), (2, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_FILL),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("BOX", (0, 0), (-1, -1), 0.9, colors.black),
        ("LINEBELOW", (0, 0), (-1, 0), 0.9, colors.black),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
    ]
    for i, (club, value) in enumerate(rows, start=1):
        value = int(value) if float(value).is_integer() else round(float(value), 1)
        data.append([i, fit_text(club or "", FONT, BODY_SIZE, club_w), value])
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), STRIPE_FILL))
        if i in MEDAL_FILLS:
            style.append(("BACKGROUND", (0, i), (0, i), MEDAL_FILLS[i]))
            style.append(("FONT", (0, i), (0, i), FONT_BOLD, BODY_SIZE))
    if not rows:
        data.append(["No results yet.", "", ""])
        style.append(("SPAN", (0, 1), (-1, 1)))

    t = Table(data, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
    t.setStyle(TableStyle(style))
    return t


def write_award_pdf(out_path: Path, title: str, description: str, note: str,
                    rows: List[Tuple[str, float]], value_label: str,
                    league_logo: Optional[Path]):
    page_w, page_h = portrait(A4)
    margin = 14 * mm
    avail_w = page_w - 2 * margin
    generated = dt.date.today().strftime("%d %b %Y")

    def on_page(canv, doc):
        canv.saveState()
        canv.setFont(FONT, 7.5)
        canv.setFillColor(colors.HexColor("#555555"))
        canv.drawString(margin, 8 * mm, f"{title} — generated {generated}")
        canv.drawRightString(page_w - margin, 8 * mm, f"Page {doc.page}")
        if doc.page > 1:
            canv.setFont(FONT_BOLD, 9)
            canv.setFillColor(colors.black)
            canv.drawString(margin, page_h - 9 * mm, f"{title} (continued)")
        canv.restoreState()

    doc = SimpleDocTemplate(str(out_path), pagesize=(page_w, page_h),
                            leftMargin=margin, rightMargin=margin,
                            topMargin=12 * mm, bottomMargin=15 * mm,
                            title=title, author="West Midlands Cyclo-Cross League")

    title_style = ParagraphStyle("title", fontName=FONT_BOLD, fontSize=18, leading=22, textColor=WMCCL_NAVY)
    text_style = ParagraphStyle("text", fontName=FONT, fontSize=9, leading=12,
                                textColor=colors.HexColor("#444444"))

    story = []
    if league_logo:
        logo = scaled_image(league_logo, 70 * mm, 20 * mm)
        if logo:
            logo.hAlign = "LEFT"
            story += [logo, Spacer(1, 6 * mm)]
    story += [Paragraph(title, title_style), Spacer(1, 1.5 * mm), Paragraph(description, text_style)]
    if note:
        story.append(Paragraph(note, text_style))
    story += [Spacer(1, 4 * mm), build_award_table(rows, value_label, avail_w)]
    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)


def main():
    ap = argparse.ArgumentParser(description="Export the 3 WMCCL team/club awards as printable PDFs.")
    ap.add_argument("--u8-db", default="U8.db")
    ap.add_argument("--u10-db", default="U10.db")
    ap.add_argument("--u12-db", default="U12.db")
    ap.add_argument("--youth-db", default="Youth.db")
    ap.add_argument("--women-db", default="Women.db")
    ap.add_argument("--seniors-db", default="Seniors.db")
    ap.add_argument("--masters-db", default="Masters.db")
    ap.add_argument("--outdir", default="league_pdf", help="Output directory (default: league_pdf)")
    ap.add_argument("--top-n", type=int, default=6,
                    help="Top N scoring riders per club per round for the two team competitions (default: 6)")
    ap.add_argument("--exclude-club", action="append", default=["No Club/Team"], help="Exclude club (repeatable)")
    ap.add_argument("--season", default=current_season(), help="Season shown in the title (default: %(default)s)")
    ap.add_argument("--league-logo", default=None, help="WMCCL logo image, shown top-left")
    args = ap.parse_args()

    youth_dbs = [args.u8_db, args.u10_db, args.u12_db]
    adult_dbs = [args.women_db, args.seniors_db, args.masters_db, args.youth_db]
    all_dbs = [args.u8_db, args.u10_db, args.u12_db, args.youth_db, args.women_db, args.seniors_db, args.masters_db]
    for db in all_dbs:
        if not Path(db).exists():
            raise SystemExit(f"❌ DB not found: {db}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    league_logo = Path(args.league_logo) if args.league_logo else None
    after_round = max_round(all_dbs)

    print("Computing Team Competition (adult/senior, U14 and above)...")
    print("Computing U12 Team Competition (U12 and below)...")
    print("Computing Mick Ives Participation Award (all categories, finishes + AP rounds)...")
    results_by_key = {
        "team": compute_team_points_multi(adult_dbs, top_n=args.top_n, exclude_clubs=args.exclude_club),
        "u12_team": compute_team_points_multi(youth_dbs, top_n=args.top_n, exclude_clubs=args.exclude_club),
        "participation": compute_completed_rides_multi(all_dbs, exclude_clubs=args.exclude_club),
    }

    for key in AWARD_ORDER:
        meta = AWARD_META[key]
        rows = results_by_key[key]
        title = f"{meta['title']} {args.season}"
        description = meta["description"]
        if key in ("team", "u12_team"):
            # AWARD_META's text says "Top 6" literally; keep it honest if --top-n differs.
            description = description.replace("Top 6", f"Top {args.top_n}")
        note = f"Standings after round {after_round}."
        out_path = outdir / PDF_FILENAMES[key]
        write_award_pdf(out_path, title, description, note, rows, meta["value_label"],
                        league_logo)
        print(f"  Wrote {out_path}  ({len(rows)} clubs)")

    print("\nDone.")


if __name__ == "__main__":
    main()
