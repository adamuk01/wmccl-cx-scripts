#!/usr/bin/env python3
"""
check_west_mids.py
------------------
Flag entrants in a RiderHQ export who are not in the West Midlands rider list
(for the Regional Championships).

West Mids list columns : first_name,last_name,dob,membership_number
                         (dob is month/day/year, e.g. 1/24/79)
RiderHQ columns used   : First name, Last name, Date of birth,
                         Membership ID (optional)

membership_number is compared with RiderHQ's "Membership ID" (British Cycling
number). It is NOT compared with RiderHQ's "Membership number" (league race number).

U6, U8, U10 and U12 are normal races, so those entries (by RiderHQ "Entry type")
are not checked.

Each entrant gets one status:
  NOT_REQUIRED   U6 / U8 / U10 / U12 entry
  WEST_MIDS      name + DOB match, or ID match backed up by the surname or DOB
  CHECK          partial match - look at it by hand
  NOT_WEST_MIDS  nothing in the list matches

Usage:
  check_west_mids.py --entries riderhq.csv --west-mids westmids.csv
  check_west_mids.py --entries riderhq.csv --west-mids westmids.csv --out checked.csv
  check_west_mids.py --entries riderhq.csv --west-mids westmids.csv --pdf champs_check.pdf

No databases are read or changed. The input files are not modified.
"""

import argparse
import csv
import re
import sys
import unicodedata
from collections import Counter
from datetime import date, datetime
from xml.sax.saxutils import escape

# Same order of preference as the other WMCCL scripts (month/day first)
DOB_FORMATS = ["%m/%d/%y", "%m/%d/%Y", "%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"]

NEW_COLS = ["WM_status", "WM_note"]

# Matches "Under 12", "Under-8", "U10" etc. (not Under 14, Under 16 or Under 23)
EXEMPT_ENTRY = re.compile(r"\b(?:under[\s-]*|u)(?:6|8|10|12)(?!\d)", re.IGNORECASE)


def clean(s):
    return " ".join((s or "").split())


def norm_name(s):
    """Ignore case, accents, spaces, hyphens and apostrophes."""
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s.casefold() if c.isalnum())


def parse_dob(s):
    s = clean(s)
    for fmt in DOB_FORMATS:
        try:
            d = datetime.strptime(s, fmt).date()
        except ValueError:
            continue
        # Two-digit years: Python reads '44' as 2044. Nobody is born in the future.
        if d > date.today():
            d = d.replace(year=d.year - 100)
        return d
    return None


def norm_id(s):
    s = re.sub(r"\.0+$", "", clean(s))          # Excel sometimes adds .0
    return re.sub(r"\D", "", s).lstrip("0")


def read_csv(path, required):
    for enc in ("utf-8-sig", "cp1252"):         # cp1252 = Excel "CSV" save
        try:
            with open(path, newline="", encoding=enc) as f:
                reader = csv.DictReader(f)
                headers = [h.strip() for h in (reader.fieldnames or [])]
                reader.fieldnames = headers
                rows = list(reader)
            break
        except UnicodeDecodeError:
            continue
        except OSError as e:
            sys.exit(f"❌ Cannot read {path}: {e}")
    else:
        sys.exit(f"❌ Cannot decode {path}")
    missing = [c for c in required if c not in headers]
    if missing:
        sys.exit(f"❌ {path} is missing column(s): {missing}\n   Found: {headers}")
    return headers, rows


def load_west_mids(path):
    _, rows = read_csv(path, ["first_name", "last_name", "dob", "membership_number"])
    riders, bad_dob = [], []
    for line, r in enumerate(rows, start=2):
        first, last = clean(r["first_name"]), clean(r["last_name"])
        if not first and not last:
            continue
        dob = parse_dob(r["dob"])
        if dob is None:
            bad_dob.append(f"line {line}: {first} {last} dob='{r['dob']}'")
        riders.append({
            "first": norm_name(first), "last": norm_name(last), "dob": dob,
            "id": norm_id(r["membership_number"]),
            "label": f"{first} {last}, DOB {dob.strftime('%d-%b-%Y') if dob else '?'}, "
                     f"ID {clean(r['membership_number']) or 'none'}",
        })
    if not riders:
        sys.exit(f"❌ No riders found in {path}")
    return riders, bad_dob


def build_indexes(riders):
    idx = {k: {} for k in ("name_dob", "id", "name", "last_dob")}
    for r in riders:
        if r["dob"]:
            idx["name_dob"].setdefault((r["first"], r["last"], r["dob"]), []).append(r)
            idx["last_dob"].setdefault((r["last"], r["dob"]), []).append(r)
        if r["id"]:
            idx["id"].setdefault(r["id"], []).append(r)
        idx["name"].setdefault((r["first"], r["last"]), []).append(r)
    return idx


def check_entrant(first, last, dob, bc_id, idx):
    """Return (status, note)."""
    f, l = norm_name(first), norm_name(last)
    id_hits = idx["id"].get(bc_id, []) if bc_id else []

    # 1. Name + DOB: the same person, whatever the ID says
    hits = idx["name_dob"].get((f, l, dob), [])
    if hits:
        if bc_id and not any(h["id"] == bc_id for h in hits):
            return "WEST_MIDS", (f"Name+DOB match; ID differs or missing in list "
                                 f"(list: {hits[0]['label']})")
        return "WEST_MIDS", ""

    # 2. ID match - must be backed up, because IDs are typed in by hand
    for h in id_hits:
        surname_ok = h["last"] == l
        dob_ok = dob is not None and h["dob"] == dob
        if surname_ok or dob_ok:
            return "WEST_MIDS", f"ID match, details differ (list: {h['label']})"
    if id_hits:
        return "CHECK", f"ID belongs to someone else in list: {id_hits[0]['label']}"

    # 3. Partial matches without the ID
    hits = idx["name"].get((f, l), [])
    if hits:
        return "CHECK", "Same name, different DOB: " + "; ".join(h["label"] for h in hits)
    hits = idx["last_dob"].get((l, dob), [])
    if hits:
        return "CHECK", ("Same surname and DOB, different first name (nickname or twin?): "
                         + "; ".join(h["label"] for h in hits))

    return "NOT_WEST_MIDS", ""


def short_reason(r):
    """Brief wording for the printed sheet (no other riders' details)."""
    if r["WM_status"] == "NOT_WEST_MIDS":
        return "Not found in list"
    note = r["WM_note"]
    if "ID belongs to someone else" in note:
        return "Check: ID is listed under another rider"
    if "Same name, different DOB" in note:
        return "Check: name found, DOB differs"
    if "Same surname and DOB" in note:
        return "Check: surname and DOB found, first name differs"
    return "Check by hand"


def write_pdf(path, results):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                        Table, TableStyle, CondPageBreak)
    except ImportError:
        sys.exit("❌ ReportLab is needed for --pdf (pip3 install reportlab)")

    styles = getSampleStyleSheet()
    cell = styles["BodyText"]
    cell.fontSize, cell.leading = 9, 11
    para = lambda text: Paragraph(escape(text), cell)

    flagged = [r for r in results if r["WM_status"] in ("NOT_WEST_MIDS", "CHECK")]
    groups = {}
    for r in flagged:
        groups.setdefault(clean(r.get("Entry type")) or "Unknown race", []).append(r)

    story = [
        Paragraph("Regional Championships: West Midlands eligibility to be confirmed",
                  styles["Title"]),
        Paragraph("These riders were not confirmed against the West Midlands rider list. "
                  "Riders who are not West Midlands cannot take a medal. "
                  "<b>Check the licence before awarding medals.</b>", styles["Normal"]),
        Spacer(1, 2 * mm),
        Paragraph(f"Riders listed: {len(flagged)}. Printed "
                  f"{datetime.now().strftime('%d-%b-%Y %H:%M')}. U8, U10 and U12 "
                  "races are not included.", styles["Normal"]),
    ]

    widths = [w * mm for w in (18, 46, 52, 22, 55, 20, 26, 30)]
    header = ["Race no", "Rider", "Club", "BC ID", "Why listed",
              "Licence seen", "West Mids? (Y / N)", "Checked by"]
    head_style = styles["BodyText"].clone("head", fontName="Helvetica-Bold")

    for entry_type in sorted(groups):
        rows = sorted(groups[entry_type],
                      key=lambda r: (clean(r.get("Last name")).lower(),
                                     clean(r.get("First name")).lower()))
        # Race name is part of the table so it repeats if the race runs onto a new page
        title = Paragraph(f"{escape(entry_type)} ({len(rows)})", styles["Heading3"])
        data = [[title] + [""] * (len(header) - 1),
                [Paragraph(h, head_style) for h in header]]
        for r in rows:
            first = clean(r.get("First name"))
            if first.islower() or first.isupper():
                first = first.title()
            data.append([
                para(clean(r.get("Bib number")) or clean(r.get("Membership number"))),
                para(f"{clean(r.get('Last name')).upper()}, {first}"),
                para(clean(r.get("club") or r.get("Club/Team"))),
                para(clean(r.get("Membership ID")) or "none"),
                para(short_reason(r)),
                "", "", "",
            ])
        table = Table(data, colWidths=widths, repeatRows=2)
        table.setStyle(TableStyle([
            ("SPAN", (0, 0), (-1, 0)),
            ("GRID", (0, 1), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 1), (-1, 1), colors.lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 2), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 2), (-1, -1), 5),
        ]))
        story += [CondPageBreak(35 * mm), table, Spacer(1, 3 * mm)]

    if not flagged:
        story.append(Paragraph("No riders to check.", styles["Heading2"]))

    def footer(canvas, doc):
        canvas.setFont("Helvetica", 8)
        canvas.drawCentredString(landscape(A4)[0] / 2, 7 * mm, f"Page {doc.page}")

    doc = SimpleDocTemplate(path, pagesize=landscape(A4),
                            leftMargin=12 * mm, rightMargin=12 * mm,
                            topMargin=10 * mm, bottomMargin=14 * mm)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def main():
    ap = argparse.ArgumentParser(description="Flag RiderHQ entrants not in the West Mids list.")
    ap.add_argument("--entries", required=True, help="RiderHQ export CSV")
    ap.add_argument("--west-mids", required=True, help="West Mids rider list CSV")
    ap.add_argument("--out", help="Write a copy of the RiderHQ CSV with WM_status/WM_note added")
    ap.add_argument("--pdf", help="Write a printable race-day sheet of riders to be checked")
    args = ap.parse_args()

    riders, bad_dob = load_west_mids(args.west_mids)
    idx = build_indexes(riders)
    headers, rows = read_csv(args.entries, ["First name", "Last name", "Date of birth"])
    if "Entry type" not in headers:
        print("⚠️  No 'Entry type' column in entries - every rider will be checked.")
    if "Membership ID" not in headers:
        print("⚠️  No 'Membership ID' column in entries - matching on name + DOB only.")

    results = []
    for r in rows:
        first, last = clean(r.get("First name")), clean(r.get("Last name"))
        if not first and not last:
            continue
        if EXEMPT_ENTRY.search(r.get("Entry type") or ""):
            r["WM_status"], r["WM_note"] = "NOT_REQUIRED", ""
            results.append(r)
            continue
        dob = parse_dob(r.get("Date of birth"))
        status, note = check_entrant(first, last, dob, norm_id(r.get("Membership ID")), idx)
        if dob is None:
            note = f"Unreadable DOB '{r.get('Date of birth') or ''}'. {note}".strip()
        r["WM_status"], r["WM_note"] = status, note
        results.append(r)

    def show(title, status, with_note):
        group = sorted((r for r in results if r["WM_status"] == status),
                       key=lambda r: (clean(r.get("Entry type")),
                                      clean(r.get("Last name")).lower(),
                                      clean(r.get("First name")).lower()))
        print(f"\n{title} ({len(group)})")
        print("-" * 60)
        for r in group:
            club = clean(r.get("club") or r.get("Club/Team"))
            print(f"  {clean(r.get('First name'))} {clean(r.get('Last name'))}"
                  f" | {clean(r.get('Entry type')) or '?'}"
                  f" | DOB {clean(r.get('Date of birth')) or '?'}"
                  f" | ID {clean(r.get('Membership ID')) or 'none'}"
                  f" | {club or 'no club'}")
            if with_note and r["WM_note"]:
                print(f"      -> {r['WM_note']}")
        if not group:
            print("  None")

    show("❌ NOT IN WEST MIDS LIST", "NOT_WEST_MIDS", True)
    show("⚠️  CHECK BY HAND", "CHECK", True)

    count = lambda s: sum(r["WM_status"] == s for r in results)
    print(f"\nWest Mids list : {len(riders)} riders")
    print(f"Total entries  : {len(results)}")
    print(f"  Not checked   : {count('NOT_REQUIRED')} (U6-U12)")
    skipped = Counter(clean(r.get("Entry type")) for r in results
                      if r["WM_status"] == "NOT_REQUIRED")
    for entry_type, n in sorted(skipped.items()):
        print(f"      {entry_type}: {n}")
    print(f"  Checked       : {len(results) - count('NOT_REQUIRED')}")
    print(f"  West Mids     : {count('WEST_MIDS')}")
    print(f"  Check by hand : {count('CHECK')}")
    print(f"  Not West Mids : {count('NOT_WEST_MIDS')}")

    if bad_dob:
        print(f"\n⚠️  {len(bad_dob)} unreadable DOB(s) in the West Mids list:")
        for b in bad_dob:
            print("  ", b)

    if args.out:
        out_headers = [h for h in headers if h not in NEW_COLS] + NEW_COLS
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=out_headers, extrasaction="ignore")
            w.writeheader()
            w.writerows(results)
        print(f"\n✅ Wrote: {args.out}")

    if args.pdf:
        write_pdf(args.pdf, results)
        print(f"✅ Wrote: {args.pdf}")


if __name__ == "__main__":
    main()
