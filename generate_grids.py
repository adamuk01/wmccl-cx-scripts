#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sqlite3
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import yaml  # pip install pyyaml

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas


# ----------------------------
# Models
# ----------------------------

@dataclass
class LeagueStats:
    race_number: int
    club_name: str
    avg_last_year: Optional[float]
    fin_count: int
    avg_this_year: Optional[float]
    best_finish: Optional[int]


@dataclass
class Entrant:
    # RiderHQ source fields (a subset we care about)
    entry_type: str
    bib_number: str
    first_name: str
    last_name: str
    sex: str
    dob: str
    club: str
    has_membership_str: str
    membership_number_str: str
    age_category: str


    # Derived
    race_number: Optional[int]
    is_league: bool
    avg_last_year: Optional[float]
    fin_count: int
    avg_this_year: Optional[float]
    best_finish: Optional[int]

    grid_used_raw: float
    grid_used_display: str
    ty_display: str
    ly_display: str
    grid_source: str  # "TY" or "LY" or "NL"

    # for debugging
    league_lookup_hit: bool


# ----------------------------
# Helpers
# ----------------------------

def _safe_int(s: str) -> Optional[int]:
    try:
        return int(str(s).strip())
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _fmt_1dp(v: Optional[float], decimals: int = 1) -> str:
    if v is None:
        return ""
    return f"{v:.{decimals}f}"


def _read_race_header(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _match_condition(value: str, cond: Dict[str, Any]) -> bool:
    """
    cond: {field: "...", equals: "..."} or {field: "...", regex: "..."}
    """
    field = cond.get("field")
    if field is None:
        return False
    v = "" if value is None else str(value)

    if "equals" in cond:
        return v == str(cond["equals"])
    if "regex" in cond:
        return re.search(cond["regex"], v, flags=re.IGNORECASE) is not None
    return False


def _eval_where(row: Dict[str, Any], where: Dict[str, Any]) -> bool:
    """
    Supports:
      where: {and: [cond, cond, ...]}
      where: {or:  [cond, cond, ...]}
      where: {and: [...], or: [...]}
    """
    and_conds = where.get("and", [])
    or_conds = where.get("or", [])

    and_ok = True
    for c in and_conds:
        field = c["field"]
        if field not in row:
            and_ok = False
            break
        if not _match_condition(str(row[field]), c):
            and_ok = False
            break

    or_ok = True
    if or_conds:
        or_ok = False
        for c in or_conds:
            field = c["field"]
            if field not in row:
                continue
            if _match_condition(str(row[field]), c):
                or_ok = True
                break

    return and_ok and or_ok


def _sort_key_builder(sort_fields: List[str], decimals: int) -> Any:
    """
    sort_fields example:
      ["-grid_used", "-fin_count", "-best_finish", "Last name", "First name"]

    We sort using raw numeric fields where available.
    """
    def key(row: Dict[str, Any]):
        out = []
        for f in sort_fields:
            desc = f.startswith("-")
            name = f[1:] if desc else f

            v = row.get(name)

            # normalize numeric-ish fields
            if name in ("grid_used", "grid_used_raw"):
                v = row.get("grid_used_raw", row.get("grid_used", -9999.0))
            elif name in ("fin_count",):
                v = int(row.get("fin_count", 0))
            elif name in ("best_finish",):
                bf = row.get("best_finish")
                v = int(bf) if bf is not None else -1

            # For strings, use lowercase for stable ordering
            if isinstance(v, str):
                v2 = v.lower()
            else:
                v2 = v

            # For descending, we invert numeric; for strings we use tuple trick
            if desc:
                if isinstance(v2, (int, float)):
                    out.append(-v2)
                else:
                    # strings: reverse by using a key that sorts reverse-ish
                    out.append("".join(chr(255 - ord(ch)) for ch in v2))
            else:
                out.append(v2)
        return tuple(out)
    return key


# ----------------------------
# DB Stats
# ----------------------------

def load_league_stats(db_path: str) -> Dict[int, LeagueStats]:
    """
    Builds lookup: race_number -> LeagueStats
    This-year stats computed from FIN only, excluding is_ap=1.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # sanity: ensure tables exist
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {r["name"] for r in cur.fetchall()}
    for req in ("riders", "results"):
        if req not in tables:
            raise SystemExit(f"DB '{db_path}' missing table '{req}'. Has: {', '.join(sorted(tables))}")

    sql = """
    SELECT
      r.id AS rider_id,
      r.race_number AS race_number,
      r.club_name AS club_name,
      r.average_points_last_year AS avg_last_year,
      (
        SELECT COUNT(*)
        FROM results res
        WHERE res.rider_id = r.id
          AND res.status = 'FIN'
          AND COALESCE(res.is_ap, 0) = 0
      ) AS fin_count,
      (
        SELECT AVG(res.points)
        FROM results res
        WHERE res.rider_id = r.id
          AND res.status = 'FIN'
          AND COALESCE(res.is_ap, 0) = 0
      ) AS avg_this_year,
      (
        SELECT MAX(res.points)
        FROM results res
        WHERE res.rider_id = r.id
          AND res.status = 'FIN'
          AND COALESCE(res.is_ap, 0) = 0
      ) AS best_finish
    FROM riders r;
    """

    cur.execute(sql)
    out: Dict[int, LeagueStats] = {}
    for row in cur.fetchall():
        rn = row["race_number"]
        if rn is None:
            continue
        rn_int = int(rn)
        out[rn_int] = LeagueStats(
            race_number=rn_int,
            club_name=row["club_name"] or "",
            avg_last_year=_safe_float(row["avg_last_year"]),
            fin_count=int(row["fin_count"] or 0),
            avg_this_year=_safe_float(row["avg_this_year"]),
            best_finish=int(row["best_finish"]) if row["best_finish"] is not None else None,
        )

    conn.close()
    return out


def merge_stats(lookups: List[Dict[int, LeagueStats]]) -> Dict[int, LeagueStats]:
    """
    If a race references multiple DBs, the first match wins.
    (You can refine later if you ever have overlaps.)
    """
    merged: Dict[int, LeagueStats] = {}
    for lu in lookups:
        for rn, st in lu.items():
            if rn not in merged:
                merged[rn] = st
    return merged


# ----------------------------
# RiderHQ read + enrich
# ----------------------------

def read_riderhq_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            # keep as strings
            rows.append({k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
        return rows


def enrich_entrants(
    riderhq_rows: List[Dict[str, str]],
    stats_lookup: Dict[int, LeagueStats],
    non_league_number_min: int,
    decimals: int,
) -> List[Entrant]:
    entrants: List[Entrant] = []

    for r in riderhq_rows:
        entry_type = r.get("Entry type", "")
        bib = r.get("Bib number", "")
        first = r.get("First name", "")
        last = r.get("Last name", "")
        sex = r.get("sex", "")
        dob = r.get("Date of birth", "")
        club = r.get("club", "")
        has_mem = r.get("Has membership", "")
        mem_no_str = r.get("Membership number", "")
        age_cat = r.get("Age Category", "")


        rn = _safe_int(mem_no_str)
        forced_non_league = (rn is None) or (rn >= non_league_number_min)

        st = stats_lookup.get(rn) if (rn is not None) else None
        league_hit = st is not None

        # league if not forced NL and found in DB
        is_league = (not forced_non_league) and league_hit

        avg_ly = st.avg_last_year if st else None
        fin_count = st.fin_count if st else 0
        avg_ty = st.avg_this_year if st else None
        best_finish = st.best_finish if st else None

        # Determine grid used
        if is_league:
            if fin_count >= 1 and avg_ty is not None:
                grid_used_raw = float(avg_ty)
                grid_source = "TY"
            else:
                grid_used_raw = float(avg_ly) if avg_ly is not None else 0.0
                grid_source = "LY"
            grid_used_display = _fmt_1dp(grid_used_raw, decimals)
        else:
            grid_used_raw = -9999.0
            grid_source = "NL"
            grid_used_display = ""

        ty_disp = ""
        if is_league and fin_count >= 1 and avg_ty is not None:
            ty_disp = f"{_fmt_1dp(avg_ty, decimals)} ({fin_count})"
        elif is_league:
            ty_disp = f" ({fin_count})" if fin_count > 0 else ""

        ly_disp = _fmt_1dp(avg_ly, decimals) if (is_league and avg_ly is not None) else ""

        entrants.append(
            Entrant(
                entry_type=entry_type,
                bib_number=bib,
                first_name=first,
                last_name=last,
                sex=sex,
                dob=dob,
                club=club,
                age_category=age_cat,
                has_membership_str=has_mem,
                membership_number_str=mem_no_str,
                race_number=rn,
                is_league=is_league,
                avg_last_year=avg_ly,
                fin_count=fin_count,
                avg_this_year=avg_ty,
                best_finish=best_finish,
                grid_used_raw=grid_used_raw,
                grid_used_display=grid_used_display,
                ty_display=ty_disp,
                ly_display=ly_disp,
                grid_source=grid_source,
                league_lookup_hit=league_hit,
            )
        )

    return entrants


# ----------------------------
# Anomalies report
# ----------------------------

def write_anomalies(out_csv: str, entrants: List[Entrant], non_league_number_min: int):
    # duplicate membership numbers
    seen: Dict[str, int] = {}
    for e in entrants:
        k = (e.membership_number_str or "").strip()
        if not k:
            continue
        seen[k] = seen.get(k, 0) + 1

    anomalies: List[Dict[str, str]] = []

    for e in entrants:
        rn = e.race_number
        mems = (e.membership_number_str or "").strip()

        if mems and seen.get(mems, 0) > 1:
            anomalies.append({
                "type": "DUPLICATE_ENTRY",
                "membership_number": mems,
                "name": f"{e.first_name} {e.last_name}",
                "entry_type": e.entry_type,
                "sex": e.sex,
            })

        if rn is not None and rn >= non_league_number_min and e.league_lookup_hit:
            anomalies.append({
                "type": "NON_LEAGUE_NUMBER_FOUND_IN_DB",
                "membership_number": str(rn),
                "name": f"{e.first_name} {e.last_name}",
                "entry_type": e.entry_type,
                "sex": e.sex,
            })

        if rn is not None and rn < non_league_number_min and (not e.league_lookup_hit):
            anomalies.append({
                "type": "LEAGUEISH_NUMBER_NOT_IN_DB",
                "membership_number": str(rn),
                "name": f"{e.first_name} {e.last_name}",
                "entry_type": e.entry_type,
                "sex": e.sex,
            })

    if not anomalies:
        # still write an empty file with header
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["type", "membership_number", "name", "entry_type", "sex"])
            w.writeheader()
        return

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["type", "membership_number", "name", "entry_type", "sex"])
        w.writeheader()
        for a in anomalies:
            w.writerow(a)


# ----------------------------
# PDF rendering (simple + readable)
# ----------------------------

def render_pdf(
    pdf_path: str,
    title: str,
    race_header: str,
    rows: List[Dict[str, Any]],
    decimals: int,
):
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4
    left = 12 * mm
    top = height - 12 * mm

    # Header
    c.setFont("Helvetica-Bold", 14)
    c.drawString(left, top, title)

    c.setFont("Helvetica", 10)
    hdr2 = race_header.strip()
    if hdr2:
        c.drawString(left, top - 14, hdr2)

    rule = "Ordering: This Year(TY) average from finished results. If no TY finished results, Last Year(LY) average used."
    c.setFont("Helvetica", 8.5)
    c.drawString(left, top - 28, rule)

    y = top - 44

    from reportlab.lib import colors

    # Columns (mm) – total ≈ 170mm (fits A4 with margins)
    cols = [
        ("Pos", 10),
        ("No", 12),
        ("First", 34),
        ("Last", 38),
        ("Club", 56),
        ("TY", 20),
        ("LY", 20),
    ]

    numeric_headers = {"Pos", "No", "TY", "LY"}

    row_h = 4.2 * mm
    stripe_color = colors.HexColor("#F2F2F2")
    stripe_on = False

    # Draw column headers
    c.setFont("Helvetica-Bold", 10.5)
    x = left
    for name, w in cols:
        if name in numeric_headers:
            c.drawRightString(x + w*mm - 1*mm, y, name)
        else:
            c.drawString(x, y, name)
        x += w * mm
    y -= 6 * mm
    c.setFont("Helvetica", 10.5)

    def new_page():
        nonlocal y, stripe_on
        c.showPage()

        # Page header
        c.setFont("Helvetica-Bold", 14)
        c.drawString(left, top, title)

        c.setFont("Helvetica", 11.5)
        if hdr2:
            c.drawString(left, top - 14, hdr2)

        c.setFont("Helvetica", 9.5)
        c.drawString(left, top - 28, rule)

        # Column headers
        y = top - 44
        c.setFont("Helvetica-Bold", 10.5)
        x2 = left
        for name, w in cols:
            if name in numeric_headers:
                c.drawRightString(x2 + w*mm - 1*mm, y, name)
            else:
                c.drawString(x2, y, name)
            x2 += w * mm
        y -= 6 * mm
        c.setFont("Helvetica", 10.5)

        stripe_on = False

    # Draw rows
    for r in rows:
        # Section headings
        if r.get("_type") == "heading":
            if y < 20 * mm:
                new_page()
            c.setFont("Helvetica-Bold", 12)
            c.drawString(left, y, r["text"])
            c.setFont("Helvetica", 10.5)
            y -= 6 * mm
            continue

        # Placeholders
        if r.get("_type") == "placeholder":
            if y < 20 * mm:
                new_page()
            c.setFont("Helvetica-Oblique", 10.5)
            c.drawString(left, y, r["text"])
            c.setFont("Helvetica", 10.5)
            y -= 5 * mm
            continue

        # Blank lines
        if r.get("_type") == "blank":
            y -= 5 * mm
            continue

        if y < 18 * mm:
            new_page()

        # Zebra striping
        stripe_on = not stripe_on
        if stripe_on:
            c.saveState()
            c.setFillColor(stripe_color)
            c.rect(
                left - 1*mm,
                y - 1.0*mm,
                (width - 2*left) + 2*mm,
                row_h,
                stroke=0,
                fill=1
            )
            c.restoreState()

        x = left

        # Pos
        c.drawRightString(x + cols[0][1]*mm - 1*mm, y, str(r.get("pos", "")))
        x += cols[0][1] * mm

        # No
        c.drawRightString(x + cols[1][1]*mm - 1*mm, y, str(r.get("race_number", "") or ""))
        x += cols[1][1] * mm

        # First / Last
        c.drawString(x, y, str(r.get("first_name", ""))[:20])
        x += cols[2][1] * mm

        c.drawString(x, y, str(r.get("last_name", ""))[:22])
        x += cols[3][1] * mm

        # Club
        club = str(r.get("club", "") or "")
        c.drawString(x, y, club[:32])
        x += cols[4][1] * mm

        # TY
        c.drawRightString(x + cols[5][1]*mm - 1*mm, y, str(r.get("ty_display", "")))
        x += cols[5][1] * mm

        # LY
        c.drawRightString(x + cols[6][1]*mm - 1*mm, y, str(r.get("ly_display", "")))
        x += cols[6][1] * mm

        y -= row_h



    c.save()


# ----------------------------
# Build sheets from YAML blocks
# ----------------------------

def build_block_rows(block: Dict[str, Any], entrants_rows: List[Dict[str, Any]], decimals: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if "title" in block:
        out.append({"_type": "heading", "text": block["title"]})

        where = block.get("where", {})
        sort = block.get("sort", [])

        filtered = [r for r in entrants_rows if _eval_where(r, where)]
        filtered.sort(key=_sort_key_builder(sort, decimals))

        # assign positions within this block only (judge calling order)
        for i, r in enumerate(filtered, start=1):
            rr = dict(r)
            rr["pos"] = i
            out.append(rr)
        return out

    if "placeholder" in block:
        out.append({"_type": "placeholder", "text": str(block["placeholder"])})
        return out

    if block.get("blank", False):
        out.append({"_type": "blank"})
        return out

    return out


def entrant_to_row(e: Entrant) -> Dict[str, Any]:
    # Keep keys matching YAML field names + internal fields used for sorting
    return {
        # original-ish
        "Entry type": e.entry_type,
        "Bib number": e.bib_number,
        "First name": e.first_name,
        "Last name": e.last_name,
        "sex": e.sex,
        "Date of birth": e.dob,
        "club": e.club,
        "Has membership": e.has_membership_str,
        "Membership number": e.membership_number_str,
        "Age Category": e.age_category,

        # derived
        "race_number": e.race_number,
        "first_name": e.first_name,
        "last_name": e.last_name,
        "is_league": e.is_league,

        "avg_last_year": e.avg_last_year,
        "fin_count": e.fin_count,
        "avg_this_year": e.avg_this_year,
        "best_finish": e.best_finish,

        "grid_used_raw": e.grid_used_raw,
        "grid_used_display": e.grid_used_display,
        "ty_display": e.ty_display,
        "ly_display": e.ly_display,
        "grid_source": e.grid_source,
    }


# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser(description="Generate gridding PDFs/CSVs from RiderHQ CSV + league DBs + YAML rules.")
    ap.add_argument("--config", required=True, help="Path to YAML config (e.g. ../rules/gridding/base.yml)")
    ap.add_argument("--dbdir", default=".", help="Directory where .db files live (default: .)")
    ap.add_argument("--riderhq", default="allRiders+cat.csv", help="RiderHQ CSV filename (default: allRiders+cat.csv)")
    ap.add_argument("--outdir", default="gridding", help="Output directory (default: gridding)")
    ap.add_argument("--debug", action="store_true", help="Enable debug output")
    ap.add_argument("--debug-limit", type=int, default=20, help="Max debug lines to print")


    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    outdir = cfg.get("outputs", {}).get("outdir", args.outdir)
    os.makedirs(outdir, exist_ok=True)

    riderhq_path = cfg.get("inputs", {}).get("riderhq_csv", args.riderhq)
    race_header_file = cfg.get("race_header_file", "./raceheader.txt")
    race_header = _read_race_header(race_header_file)

    gp = cfg.get("grid_points", {})
    non_league_min = int(gp.get("non_league_number_min", 900))
    decimals = int(gp.get("decimals", 1))

    # Read RiderHQ rows once
    riderhq_rows = read_riderhq_csv(riderhq_path)
    if args.debug:
        print(f"[DEBUG] RiderHQ rows loaded: {len(riderhq_rows)} from {riderhq_path}")
        if riderhq_rows:
            print(f"[DEBUG] RiderHQ columns: {sorted(riderhq_rows[0].keys())}")
            # show first few membership numbers exactly as read
            for i, r in enumerate(riderhq_rows[:min(args.debug_limit, 10)], start=1):
                mn = r.get("Membership number", None)
                print(f"[DEBUG] sample {i}: Membership number raw={mn!r}")


    # Build anomalies across ALL entrants after enrichment per-race (we’ll do a global version too)
    # We'll do global anomalies based on "membership number" duplicates just from the CSV.
    # (Race-specific anomalies are still visible in audit files.)

    # Load stats per DB once (cache)
    db_cache: Dict[str, Dict[int, LeagueStats]] = {}

    for race in cfg.get("races", []):
        race_name = race.get("name", "race")
        dbs = race.get("dbs", [])
        if not dbs:
            continue

        lookups = []
        for db in dbs:
            db_path = db
            if not os.path.isabs(db_path):
                db_path = os.path.join(args.dbdir, db_path)
            db_path = os.path.normpath(db_path)

            if db_path not in db_cache:
                db_cache[db_path] = load_league_stats(db_path)
            lookups.append(db_cache[db_path])

        stats_lookup = merge_stats(lookups)
        if args.debug :
            print(f"[DEBUG] Race '{race_name}': dbs={dbs}")
            print(f"[DEBUG] Race '{race_name}': lookup entries={len(stats_lookup)}")
            # show a few keys
            sample_keys = sorted(list(stats_lookup.keys()))[:10]
            print(f"[DEBUG] Race '{race_name}': lookup key sample={sample_keys}")



        entrants = enrich_entrants(
            riderhq_rows=riderhq_rows,
            stats_lookup=stats_lookup,
            non_league_number_min=non_league_min,
            decimals=decimals,
        )
        entrant_rows = [entrant_to_row(e) for e in entrants]
        if args.debug :
            # Riders that look like league numbers (<900) but didn't hit DB
            misses = [e for e in entrants if (e.race_number is not None and e.race_number < non_league_min and not e.league_lookup_hit)]
            hits = [e for e in entrants if e.league_lookup_hit]

            print(f"[DEBUG] Race '{race_name}': hits={len(hits)} misses(<{non_league_min})={len(misses)} total entrants={len(entrants)}")

            for e in misses[:args.debug_limit]:
                print(f"[DEBUG] MISS rn={e.race_number} mem_raw={e.membership_number_str!r} "
                      f"name='{e.first_name} {e.last_name}' entry='{e.entry_type}'")



        # For each sheet, build rows then write outputs
        for sheet in race.get("sheets", []):
            sheet_title = sheet.get("title", f"{race_name} GRIDDING")
            blocks = sheet.get("blocks", [])

            built_rows: List[Dict[str, Any]] = []
            for b in blocks:
                built_rows.extend(build_block_rows(b, entrant_rows, decimals))

            # Audit CSV: only actual rider lines
            safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", sheet_title).strip("_")
            audit_csv = os.path.join(outdir, f"audit-{safe_name}.csv")

            with open(audit_csv, "w", encoding="utf-8", newline="") as f:
                # include all keys we care about for disputes
                fieldnames = [
                    "sheet_title",
                    "pos",
                    "race_number",
                    "first_name",
                    "last_name",
                    "sex",
                    "Entry type",
                    "club",
                    "is_league",
                    "fin_count",
                    "best_finish",
                    "avg_this_year",
                    "avg_last_year",
                    "grid_source",
                    "grid_used_raw",
                    "grid_used_display",
                    "ty_display",
                    "ly_display",
                    "Membership number",
                    "Has membership",
                ]
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                for r in built_rows:
                    if r.get("_type"):
                        continue
                    row = {k: r.get(k, "") for k in fieldnames}
                    row["sheet_title"] = sheet_title
                    w.writerow(row)

            # PDF
            if cfg.get("outputs", {}).get("pdf", True):
                pdf_path = os.path.join(outdir, f"{safe_name}.pdf")
                render_pdf(
                    pdf_path=pdf_path,
                    title=sheet_title,
                    race_header=race_header,
                    rows=built_rows,
                    decimals=decimals,
                )

    # Global anomalies file: build a merged lookup across ALL dbs referenced in config
    if cfg.get("outputs", {}).get("anomalies_csv", True):
        all_db_paths = set()
        for race in cfg.get("races", []):
            for db in race.get("dbs", []):
                db_path = db
                if not os.path.isabs(db_path):
                    db_path = os.path.join(args.dbdir, db_path)
                all_db_paths.add(os.path.normpath(db_path))

        global_lookups = []
        for db_path in sorted(all_db_paths):
            if db_path not in db_cache:
                db_cache[db_path] = load_league_stats(db_path)
            global_lookups.append(db_cache[db_path])

        global_stats_lookup = merge_stats(global_lookups)

        entrants_global = enrich_entrants(
            riderhq_rows=riderhq_rows,
            stats_lookup=global_stats_lookup,
            non_league_number_min=non_league_min,
            decimals=decimals,
        )

        anomalies_csv = os.path.join(outdir, "anomalies.csv")
        write_anomalies(anomalies_csv, entrants_global, non_league_min)

    if args.debug:
        # How many global misses?
        global_misses = [e for e in entrants_global if (e.race_number is not None and e.race_number < non_league_min and not e.league_lookup_hit)]
        print(f"[DEBUG] Global lookup: entries={len(global_stats_lookup)} riderHQ={len(riderhq_rows)} misses(<{non_league_min})={len(global_misses)}")
        for e in global_misses[:args.debug_limit]:
            print(f"[DEBUG] GLOBAL MISS rn={e.race_number} mem_raw={e.membership_number_str!r} "
                  f"name='{e.first_name} {e.last_name}' entry='{e.entry_type}'")




if __name__ == "__main__":
    main()

