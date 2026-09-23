#!/usr/bin/env python3
"""
export_league_tables_pdf.py
---------------------------
Generate printable PDF league tables from a single SQLite DB — the PDF
counterpart to export_league_tables_html.py (and export_league_tables.py's
CSV output). Uses the SAME scoring + row-building code, imported directly
from export_league_tables_html.py (which in turn uses league_scoring.py), so
the PDF, HTML and CSV outputs can never disagree on a rider's points, best-N
total, average or position.

Layout mirrors the Numbers-produced PDF the league has been publishing
(e.g. "Junior Open League 2026-27"): landscape page, league logo + sponsor
logos across the top, a title, then one row per rider with Position, Name,
No, Category, Club, One bike, Best N, Av. Points and one column per round
(round number + venue as the header). AP (average points) cells get a
yellow fill, explained in a footnote at the bottom of every page. Long
tables flow onto extra pages with the column header repeated.

Run once per DB/profile, same pattern as the HTML exporter — see
export_league_tables_pdf.sh:

  export_league_tables_pdf.py --db Seniors.db --profile seniors --outdir league_pdf \\
      --rounds 11 --best 9 \\
      --league-logo ../sponsors/WMCCL.png \\
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.jpg \\
      --sponsor-logo ../sponsors/Shimano.jpg

OUTPUT: <outdir>/<table_name>.pdf, one file per non-empty table in the
profile (e.g. league_pdf/JunM.pdf, league_pdf/Sen_U23_M.pdf, ...).

MERGED PDFs (added 2026-09-23): the boys' and girls' tables for U6, U8, U10
and U12 are printed together in one PDF — league_pdf/U6.pdf, U8.pdf,
U10.pdf, U12.pdf — with the logos/title once at the top and each category
as its own headed table underneath ("Under 8 Boys", then "Under 8 Girls").
Each table keeps its own positions; riders are not re-ranked together.
Groups are in MERGE_GROUPS; if only one table of a pair has riders it just
gets its own PDF as normal. --no-merge restores one PDF per table.

DIFFERENCES FROM THE HTML PAGES (deliberate, to match the existing PDF):
  (Rider names are first name + surname initial, e.g. "Tom O." - the same
  privacy convention as the HTML league tables and rider pages. --full-names
  exists for internal/admin copies only; don't publish those.)
  * A round with no result shows 0 (the HTML shows a dash).
  * No links / medals — it's a print document.

TABLE TITLES: "<label> League <season>", where <label> comes from
TABLE_TITLES below (e.g. JunM -> "Junior Open") and <season> from --season
(defaults to the current season, e.g. 2026-27). Override any label at run
time with --title JunM="Junior Men" (repeatable).
"""

import argparse
import datetime as dt
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A3, A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import (CondPageBreak, Image, Paragraph, SimpleDocTemplate, Spacer,
                                Table, TableStyle)

from export_league_tables_html import (compute_table_rows, ensure_schema,
                                       load_results, load_riders, load_rounds)
from league_scoring import profile_tables


# Human-readable labels for the PDF title. Anything not listed falls back to
# the raw table name. Override per run with --title NAME="Label".
TABLE_TITLES = {
    "U6M": "Under 6 Boys", "U6F": "Under 6 Girls",
    "U8M": "Under 8 Boys", "U8F": "Under 8 Girls",
    "U10M": "Under 10 Boys", "U10F": "Under 10 Girls",
    "U12M": "Under 12 Boys", "U12F": "Under 12 Girls",
    "U14M": "Under 14 Boys", "U14F": "Under 14 Girls",
    "U16M": "Under 16 Boys", "U16F": "Under 16 Girls",
    "Women_All": "Women",
    "JunM": "Junior Open",
    "Sen_U23_M": "Senior / U23 Men",
    "M40M": "Masters 40-49",
    "M50M": "Masters 50-59",
    "M60M": "Masters 60-69",
    "M70M": "Masters 70+",
}

# Tables printed together in ONE PDF (one after the other, each keeping its
# own positions/standings - riders are NOT re-ranked against each other).
# Output file is <group>.pdf, e.g. league_pdf/U8.pdf holding U8M then U8F.
# Groups only apply to the profile whose tables they name; any table not in
# a group still gets its own PDF. Turn off with --no-merge.
MERGE_GROUPS = {
    "U6": ["U6M", "U6F"],
    "U8": ["U8M", "U8F"],
    "U10": ["U10M", "U10F"],
    "U12": ["U12M", "U12F"],
}
GROUP_TITLES = {"U6": "Under 6", "U8": "Under 8", "U10": "Under 10", "U12": "Under 12"}

DEFAULT_FOOTNOTE = "Yellow fill - Average points for helpers and Regional Champs."

PAGE_SIZES = {"a4": A4, "a3": A3, "tabloid": (792, 1224)}

FONT = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
BODY_SIZE = 7.5
HEAD_SIZE = 7

AP_FILL = colors.HexColor("#FFF200")
HEADER_FILL = colors.HexColor("#E6E6E6")
STRIPE_FILL = colors.HexColor("#F7F7F7")
GRID = colors.HexColor("#9A9A9A")
WMCCL_NAVY = colors.HexColor("#2F3B52")


def current_season(today: Optional[dt.date] = None) -> str:
    """Cyclo-cross seasons straddle new year: Jul 2026 - Jun 2027 -> '2026-27'."""
    today = today or dt.date.today()
    start = today.year if today.month >= 7 else today.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def fit_text(text: str, font: str, size: float, max_w: float) -> str:
    """Trim text with an ellipsis so it fits in max_w points."""
    text = (text or "").strip()
    if stringWidth(text, font, size) <= max_w:
        return text
    while text and stringWidth(text + "…", font, size) > max_w:
        text = text[:-1]
    return text.rstrip() + "…"


def scaled_image(path: Path, max_w: float, max_h: float) -> Optional[Image]:
    if not path.exists():
        print(f"⚠️  WARNING: logo not found, skipping: {path}")
        return None
    iw, ih = ImageReader(str(path)).getSize()
    scale = min(max_w / iw, max_h / ih)
    return Image(str(path), width=iw * scale, height=ih * scale)


def round_header(rnd: int, rounds_info: Dict[int, Dict[str, str]], col_w: float) -> str:
    """
    'R3' plus the venue underneath (falls back to the round name if it isn't
    just 'Round N'). The venue is set in a smaller font, shrunk further if
    needed so its longest word fits the column — ReportLab otherwise breaks
    long words mid-word ("Stratfor / d").
    """
    info = rounds_info.get(rnd) or {}
    label = info.get("venue") or ""
    if not label and info.get("name") and info["name"].lower() != f"round {rnd}":
        label = info["name"]
    if not label:
        return f"R{rnd}"
    longest = max(label.replace("/", "/ ").split(), key=lambda w: stringWidth(w, FONT, 6))
    size = 6.0
    while size > 4.5 and stringWidth(longest, FONT, size) > col_w - 8:
        size -= 0.25
    label = label.replace("/", "/ ")
    return f'R{rnd}<br/><font name="{FONT}" size="{size}">{label}</font>'


# ---------------------------------------------------------------------------
# PDF building
# ---------------------------------------------------------------------------

def build_logo_strip(league_logo: Optional[Path], sponsor_logos: List[Path], avail_w: float):
    league_img = scaled_image(league_logo, 70 * mm, 17 * mm) if league_logo else None
    sponsors = [img for img in (scaled_image(p, 40 * mm, 13 * mm) for p in sponsor_logos) if img]
    if not league_img and not sponsors:
        return None
    left_w = (league_img.drawWidth + 6 * mm) if league_img else 0
    right_w = avail_w - left_w
    sponsor_cells = sponsors or [""]
    sponsor_row = Table([sponsor_cells], colWidths=[right_w / len(sponsor_cells)] * len(sponsor_cells))
    sponsor_row.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    cells = ([league_img] if league_img else []) + [sponsor_row]
    widths = ([left_w] if league_img else []) + [right_w]
    strip = Table([cells], colWidths=widths)
    strip.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (0, 0), "LEFT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))
    return strip


def build_table(rows: List[Dict], full_names: Dict, rounds: int, best_n: int,
                rounds_info: Dict[int, Dict[str, str]], avail_w: float, short_names: bool) -> Table:
    head_style = ParagraphStyle("head", fontName=FONT_BOLD, fontSize=HEAD_SIZE,
                                leading=HEAD_SIZE + 1.2, alignment=TA_CENTER)
    head_left = ParagraphStyle("headl", parent=head_style, alignment=TA_LEFT)

    # Fixed columns (points), then share what's left between the rounds.
    fixed = [("Position", 34, head_style), ("Name", 108, head_left), ("No", 24, head_style),
             ("Category", 38, head_style), ("Club", 112, head_left), ("One<br/>bike", 26, head_style),
             (f"Best {best_n}<br/>results", 32, head_style), ("Av.<br/>Points", 30, head_style)]
    fixed_w = sum(w for _, w, _ in fixed)
    round_w = (avail_w - fixed_w) / max(rounds, 1)
    if round_w < 24:  # very long season on a small page: squeeze name/club instead
        shortfall = (24 - round_w) * rounds
        fixed[1] = (fixed[1][0], fixed[1][1] - shortfall / 2, fixed[1][2])
        fixed[4] = (fixed[4][0], fixed[4][1] - shortfall / 2, fixed[4][2])
        round_w = 24
    col_widths = [w for _, w, _ in fixed] + [round_w] * rounds
    name_w, club_w = fixed[1][1] - 6, fixed[4][1] - 6

    header = [Paragraph(label, style) for label, _, style in fixed]
    header += [Paragraph(round_header(r, rounds_info, round_w), head_style) for r in range(1, rounds + 1)]
    data = [header]

    style_cmds = [
        ("FONT", (0, 1), (-1, -1), FONT, BODY_SIZE),
        ("ALIGN", (0, 1), (-1, -1), "CENTER"),
        ("ALIGN", (1, 1), (1, -1), "LEFT"),
        ("ALIGN", (4, 1), (4, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("VALIGN", (0, 0), (-1, 0), "BOTTOM"),
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_FILL),
        ("GRID", (0, 0), (-1, -1), 0.4, GRID),
        ("BOX", (0, 0), (-1, -1), 0.9, colors.black),
        ("LINEBELOW", (0, 0), (-1, 0), 0.9, colors.black),
        ("FONT", (6, 1), (6, -1), FONT_BOLD, BODY_SIZE),   # Best N total
        ("TOPPADDING", (0, 1), (-1, -1), 1.2),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 1.2),
        ("TOPPADDING", (0, 0), (-1, 0), 2),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ]

    for i, r in enumerate(rows, start=1):
        if short_names:
            name = r["name"]
        else:
            first, last = full_names.get(r["race_number"], ("", ""))
            name = f"{(first or '').strip()} {(last or '').strip()}".strip()
        line = [
            r["position"],
            fit_text(name, FONT, BODY_SIZE, name_w),
            r["race_number"],
            r["category"] or "",
            fit_text(r["club"], FONT, BODY_SIZE, club_w),
            r["one_bx"],
            r["best_total"],
            "" if r["avg"] is None else f"{r['avg']:.1f}",
        ]
        for c, (value, is_ap) in enumerate(r["per_round"]):
            line.append(0 if value is None else value)
            if is_ap:
                style_cmds.append(("BACKGROUND", (8 + c, i), (8 + c, i), AP_FILL))
        data.append(line)
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (7, i), STRIPE_FILL))

    if not rows:
        data.append(["No riders in this table."] + [""] * (len(col_widths) - 1))
        style_cmds.append(("SPAN", (0, 1), (-1, 1)))

    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle(style_cmds))
    return t


def write_table_pdf(out_path: Path, title: str, subtitle: str, sections: List[Tuple[Optional[str], List[Dict], Dict]],
                    rounds: int, best_n: int, rounds_info: Dict[int, Dict[str, str]],
                    league_logo: Optional[Path], sponsor_logos: List[Path],
                    pagesize, footnote: str, short_names: bool):
    """
    sections: [(heading, rows, full_names), ...]. A single table passes one
    section with heading None; a merged group passes one section per table
    (e.g. "Under 8 Boys", "Under 8 Girls"), each printed as its own table.
    """
    page_w, page_h = landscape(pagesize)
    margin = 12 * mm
    avail_w = page_w - 2 * margin
    generated = dt.date.today().strftime("%d %b %Y")

    def on_page(canv, doc):
        canv.saveState()
        canv.setFont(FONT, 7.5)
        canv.setFillColor(colors.HexColor("#555555"))
        canv.drawString(margin, 7 * mm, f"{title} — generated {generated}")
        canv.drawCentredString(page_w / 2, 7 * mm, f"Page {doc.page}")
        if footnote:
            canv.setFillColor(colors.black)
            text_w = stringWidth(footnote, FONT, 7.5)
            box_x = page_w - margin - text_w - 14
            canv.setFillColor(AP_FILL)
            canv.rect(box_x, 6.3 * mm, 9, 9, stroke=0, fill=1)
            canv.setFillColor(colors.black)
            canv.drawRightString(page_w - margin, 7 * mm, footnote)
        if doc.page > 1:  # small running header on continuation pages
            canv.setFont(FONT_BOLD, 9)
            canv.drawString(margin, page_h - 8 * mm, f"{title} (continued)")
        canv.restoreState()

    doc = SimpleDocTemplate(str(out_path), pagesize=(page_w, page_h),
                            leftMargin=margin, rightMargin=margin,
                            topMargin=10 * mm, bottomMargin=13 * mm,
                            title=title, author="West Midlands Cyclo-Cross League")

    title_style = ParagraphStyle("title", fontName=FONT_BOLD, fontSize=16, leading=19, textColor=WMCCL_NAVY)
    sub_style = ParagraphStyle("sub", fontName=FONT, fontSize=8.5, leading=11, textColor=colors.HexColor("#444444"))

    story = []
    strip = build_logo_strip(league_logo, sponsor_logos, avail_w)
    if strip:
        story += [strip, Spacer(1, 4 * mm)]
    story += [Paragraph(title, title_style), Paragraph(subtitle, sub_style), Spacer(1, 3 * mm)]
    section_style = ParagraphStyle("section", fontName=FONT_BOLD, fontSize=12, leading=15,
                                   textColor=WMCCL_NAVY, spaceBefore=4 * mm, spaceAfter=1.5 * mm)
    for heading, rows, full_names in sections:
        if heading:
            # Start a new page only if there isn't room for the heading, the
            # column header and a few rows — otherwise the table is allowed
            # to split across pages (header repeats), so the second category
            # fills the rest of page 1 instead of jumping wholesale to page 2.
            story.append(CondPageBreak(38 * mm))
            story.append(Paragraph(heading, section_style))
        story.append(build_table(rows, full_names, rounds, best_n, rounds_info, avail_w, short_names))
    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Export league tables as printable PDFs (see module docstring).")
    ap.add_argument("--db", required=True, help="SQLite DB (e.g. U8.db, Youth.db, Women.db, Seniors.db)")
    ap.add_argument("--profile", required=True,
                    choices=["u8", "u10", "u12", "youth", "women", "seniors", "masters"],
                    help="Grouping rules matching your DB/race")
    ap.add_argument("--outdir", default="league_pdf", help="Output directory (default: league_pdf)")
    ap.add_argument("--rounds", type=int, default=12, help="Number of round columns to score/show (default: 12)")
    ap.add_argument("--best", type=int, default=10, help="Best N results (default: 10)")
    ap.add_argument("--upto-round", type=int, default=None,
                    help="Compute averages using results up to this round (inclusive). "
                         "Default: max round present in DB (capped by --rounds).")
    ap.add_argument("--avg-decimals", type=int, default=1, help="Decimals for the average (default 1)")
    ap.add_argument("--season", default=current_season(), help="Season shown in the title (default: %(default)s)")
    ap.add_argument("--title", action="append", default=[], metavar='TABLE="Label"',
                    help='Override a table\'s title label, e.g. --title JunM="Junior Men" (repeatable)')
    ap.add_argument("--league-logo", default=None, help="WMCCL logo image, shown top-left")
    ap.add_argument("--sponsor-logo", action="append", default=[], help="Sponsor logo image (repeatable)")
    ap.add_argument("--pagesize", choices=sorted(PAGE_SIZES), default="a4", help="Page size, landscape (default: a4)")
    ap.add_argument("--footnote", default=DEFAULT_FOOTNOTE, help="Footnote explaining the yellow AP fill")
    ap.add_argument("--full-names", action="store_true",
                    help='Show full surnames instead of the default "First S." (internal/admin copies only - not for publishing)')
    ap.add_argument("--no-merge", action="store_true",
                    help="One PDF per table, even for the MERGE_GROUPS pairs (U6/U8/U10/U12 boys+girls)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")

    titles = dict(TABLE_TITLES)
    for item in args.title:
        if "=" not in item:
            raise SystemExit(f'❌ --title must look like TABLE="Label", got: {item}')
        k, v = item.split("=", 1)
        titles[k.strip()] = v.strip()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT MAX(round) FROM results")
    max_round = cur.fetchone()[0] or 0
    upto_round = args.upto_round if args.upto_round is not None else max_round
    upto_round = min(upto_round, args.rounds)

    riders = load_riders(conn)
    results = load_results(conn, args.rounds)
    rounds_info = load_rounds(conn)
    conn.close()

    league_logo = Path(args.league_logo) if args.league_logo else None
    sponsor_logos = [Path(p) for p in args.sponsor_logo]
    pagesize = PAGE_SIZES[args.pagesize]

    print(f"\nDB: {db_path.name}   Profile: {args.profile}   Season: {args.season}")
    print(f"Rounds 1..{args.rounds}, best {args.best}, averages up to round {upto_round} (max in DB {max_round})")
    print(f"Output dir: {outdir}\n")

    subtitle = (f"Best {args.best} from {args.rounds} rounds count. "
                f"Standings after round {upto_round}.")

    # Build every non-empty table's rows first, then decide what goes in which file.
    tables = {}
    for table_name, cat_list in profile_tables(args.profile).items():
        if cat_list == ["*"]:
            subset = riders
        else:
            cat_set = set(cat_list)
            subset = [r for r in riders if (r[5] or "") in cat_set]
        if not subset:
            print(f"  Skipped {table_name}  (0 riders)")
            continue
        rows = compute_table_rows(subset, results, args.rounds, args.best, upto_round, args.avg_decimals)
        full_names = {r[1]: (r[2], r[3]) for r in subset}
        tables[table_name] = (rows, full_names)

    # (file stem, title, [table names]) — merged groups first, then leftovers.
    outputs = []
    used = set()
    if not args.no_merge:
        for group, members in MERGE_GROUPS.items():
            present = [t for t in members if t in tables]
            if len(present) >= 2:
                outputs.append((group, f"{titles.get(group, GROUP_TITLES.get(group, group))} League {args.season}", present))
                used.update(present)
    for table_name in tables:
        if table_name not in used:
            outputs.append((table_name, f"{titles.get(table_name, table_name)} League {args.season}", [table_name]))

    for stem, title, members in outputs:
        if len(members) == 1:
            rows, full_names = tables[members[0]]
            sections = [(None, rows, full_names)]
        else:
            sections = [(titles.get(t, t), *tables[t]) for t in members]
        out_path = outdir / f"{stem}.pdf"
        write_table_pdf(out_path, title, subtitle, sections, args.rounds, args.best,
                        rounds_info, league_logo, sponsor_logos, pagesize, args.footnote, not args.full_names)
        detail = ", ".join(f"{t} {len(tables[t][0])}" for t in members)
        print(f"  Wrote {out_path.name}  ({detail} riders)")

    print("\nDone.")


if __name__ == "__main__":
    main()
