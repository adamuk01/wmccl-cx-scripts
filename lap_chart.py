#!/usr/bin/env python3
"""
lap_chart.py
------------
PILOT / one-off tool: builds an interactive "position by lap" chart straight
from a raw D3 RaceTec results CSV -- no database involved.

WHY:
    Sporthive-style lap charts show each rider's race position after every
    lap, so you can see overtakes happen through the race instead of only
    the final result. D3 already exports per-lap split times in columns
    like "Lap1", "Lap 2", "Lap 3", ... -- import_race_results.py explicitly
    ignores these today. This script reads them directly and does the
    position-by-lap maths in memory; nothing is written to any league DB.

KEY DESIGN DECISIONS (confirmed against real WMCCL data, 2026-09-19):

  1. Charts are built PER CATEGORY, never across the whole start list.
     Some categories share a start line with others but start at a
     genuine time offset -- e.g. in Seniors-results.csv, "M 40-49" riders
     all show a ~3:00-3:30 first split versus ~0:25-0:50 for everyone
     else in the same file (a 2-minute staggered start). Comparing raw
     cumulative time across that boundary would be meaningless. Because
     league scoring is already done per category (Cat Pos), ranking
     within a category sidesteps the stagger entirely -- everyone being
     ranked against each other genuinely started together.

  2. "Lap 1" is relabelled "Start" on the chart. It isn't a full lap --
     it's the short run from the start line to the first timing loop
     (0:25-0:50 for a normal start, ~3:00-3:30 for a staggered one) plus,
     for a staggered group, the stagger itself. It's still a real and
     interesting data point (grid position matters in CX), just not
     comparable in duration to the laps that follow, so it gets its own
     axis label rather than being called "Lap 1".

  3. A rider's line stops at their own last recorded lap. WMCCL rounds
     run to a time cutoff (the "bell lap" rule), so riders in the same
     category legitimately finish on different lap counts -- this is
     NOT the same as a DNF. A row with the literal text "DNF" in Time is
     excluded from ranking entirely (no reliable finish data).

  4. No per-rider colour palette. A category can have 40+ riders, far
     past the number of colours a human can tell apart (see the dataviz
     colour-formula: cap distinct categorical hues at ~8, fewer once
     every pair needs to be tellable apart). Instead: the top 3 finishers
     in each category get a fixed colour each (and a legend); everyone
     else renders as a thin muted line, individually identifiable via
     hover/keyboard-focus (tooltip) or the search box, and always listed
     in the standings table beside the chart (which doubles as the
     required non-chart "table view" of the same data).

  5. Rider names are shown as "First S." (first name + surname initial),
     matching export_rider_pages.py's public-page privacy convention --
     full surnames never appear anywhere in the output (chart, tooltip,
     search, standings list). Search matches against that same shortened
     form, not the hidden full surname.

  6. Embeds in a same-origin WordPress iframe (2026-09-20, at Adam's
     request), using the exact same IFRAME_RESIZE_SCRIPT already relied on
     by export_league_tables_html.py / export_rider_pages.py /
     export_team_awards_html.py -- same fixes for the same reasons (a
     pageshow listener so back/forward navigation still resizes; a 0px
     reset before each measurement so the iframe can shrink, not just
     grow; a re-entrancy guard so that reset doesn't loop forever). This
     page has no internal navigation, so the back-navigation case mostly
     doesn't apply here, but window resize and late-loading-content still
     do, so the identical script is used rather than a stripped-down one --
     one script to maintain, not two slightly-different copies. Only
     activates when the page is loaded same-origin with the parent (e.g.
     both under westmidscx.co.uk); opening the file directly, or embedding
     it cross-origin, is a harmless no-op.

  7. M 70+ riders are folded into the M60 chart (2026-09-20, at Adam's
     request -- an M70 field is typically only a handful of riders, too
     few for its own meaningful chart). This mirrors normalise_category()
     in import_race_results.py, which already scores age>=60 as one
     combined "M60" group. IMPORTANT CAVEAT: that merge is safe only if
     M60 and M70 riders share a start line. We already know from the
     Seniors-results.csv pilot that WMCCL sometimes runs a category on a
     genuinely staggered start (M 40-49 went off ~2 minutes behind
     everyone else in that file) -- if M70 turns out to be staggered
     behind M60, ranking them together by raw cumulative time would
     reintroduce exactly that problem. This script prints each merged
     group's Start-lap spread so that can be checked against a real
     Masters CSV before trusting the merged chart.

  8. Click a rider (their line, or their standings row) to pin them
     permanently highlighted with their own colour, so a second (or third)
     click on another rider lets you compare them side by side -- e.g.
     against a training partner (2026-09-20, at Adam's request). Podium
     riders can't be pinned (they already have a permanent colour and a
     legend entry); everyone else dims further once anything is pinned, so
     the pinned rider(s) stand out. Capped at 5 concurrent pins per chart
     (one category's worth of distinct colours); a 6th click while 5 are
     pinned is silently ignored. Each pinned rider also gets a removable
     chip in a small "pin legend" row so pins are visible/removable without
     hunting through the chart, and pinning is keyboard-accessible
     (Enter/Space on a focused rider, same as click). Verified end to end
     with Playwright: pin colour persists once the mouse moves away (a
     pinned rider briefly shows the ordinary hover colour while the pointer
     is actually over them -- that's the existing hover behaviour, not a
     pin bug), podium exclusion, the 5-pin cap, chip removal, and
     keyboard activation.

  9. Optional --split-gender flag: Youth (U14+U16) is currently WMCCL's
     only category where girls and boys share a single start and are
     scored/reported together in the CSV (2026-09-20, at Adam's request).
     With this flag set, any category containing 2+ riders of different
     known genders is broken into separate per-gender charts instead of
     one combined one; a rider with no usable Gender value in the CSV
     isn't dropped -- they get their own small "(gender not recorded)"
     chart. Every other category (already single-gender, or with only one
     gender actually present in that file) is completely unaffected either
     way, so this is safe to leave on by default once trusted. CAVEAT: the
     CSV's own "Cat Pos" reflects each rider's position in the ORIGINAL
     COMBINED field, not within just their gender, so it is not valid
     ground truth for a split subgroup's own computed rank -- the script
     skips that particular sanity check for split subgroups (and says so
     in the console output) rather than reporting a wall of expected,
     meaningless "mismatches".

  10. Live-site cleanup (2026-09-20, at Adam's request, once the site was
      approved to go live). The "Pilot output - not yet wired into the
      league database or site" caveat and the "Names are shown as first
      name + surname initial only" line are both dropped from the page --
      no longer needed once this is a real published page rather than a
      pilot. The remaining "Source file: ..." caveat line stays (still
      useful provenance). Also added: the per-category rider count/lap
      count ("Up to N laps") now appears both directly above and directly
      below each category's standings list, not just implicitly on the
      chart's x-axis -- useful because the standings list scrolls
      internally for big fields (e.g. 41 riders), so a reader scrolled to
      the bottom of that list still sees it without scrolling back up.

  11. Club names are capped at 20 characters in the standings list
      (2026-09-20, at Adam's request -- the names/club/time sidebar was
      too wide). format_club() truncates on a word boundary where
      reasonable and appends an ellipsis; the full club name is always
      carried in a `title` attribute, so it's still one hover away even
      when either this cap or the column's own CSS width additionally
      clips it. Only the standings-list display text is shortened -- the
      chart's own hover/focus tooltip (a separate bit of UI) always shows
      the full, untruncated club name.

  12. Optional --merge-all-categories flag (2026-09-20, at Adam's request):
      ignores the CSV's own Category column entirely and treats every
      rider in the file as one combined field, instead of one chart per
      category. For WMCCL's Women races, which run every category (Junior,
      Senior, Vet 40, Vet 50, ...) as a single shared start -- the same
      grouping league_scoring.py's "women" profile already uses for league
      points ({"Women_All": ["*"]}), just applied here to the lap chart
      too. Off by default; every other race type (charted per category as
      before) is completely unaffected. --merged-title names the resulting
      chart (default "All categories combined").

      Reuses the same machinery already built for the M60+M70 merge and
      the gender split rather than adding new logic: the merged group's
      raw sub-categories and their median "Start"-lap spread are printed
      exactly as for any other merged group (design note #6), warning if
      they don't look like they actually shared a start; and, like a
      gender-split subgroup, the CSV's own Cat Pos (computed per original
      narrow category) isn't valid ground truth for the new merged rank,
      so that sanity check is skipped for the merged group too, with the
      console saying why. Composes with --split-gender if both are given
      (merge first, then split whatever comes out of that by gender) --
      not needed for an all-female Women race, but kept general.

  13. Faint dotted vertical gridline at each lap position (2026-09-20, at
      Adam's request), lined up with the "Start"/"Lap N" label directly
      below it -- makes it easy to see exactly which lap a position change
      happened on, especially in a big/busy field. Purely decorative
      (`.gridline-v`, `pointer-events: none`): dashed, 70% opacity, same
      muted colour as the existing horizontal rank gridlines, so it reads
      as background structure rather than competing with the rider lines
      drawn on top of it.

USAGE:
    python3 lap_chart.py --csv Seniors-results.csv --out lap_chart.html \
        --title "Round 3 - Cannock Chase"

    For a Youth CSV where girls and boys race together, add --split-gender
    to get separate Female/Male charts instead of one combined one:
    python3 lap_chart.py --csv Youth-results.csv --out youth_chart.html \
        --title "Round 3 - Youth" --split-gender

    For a Women CSV where every category shares one start, add
    --merge-all-categories to get one combined chart instead of one per
    category:
    python3 lap_chart.py --csv Women-results.csv --out women_chart.html \
        --title "Round 3 - Women" --merge-all-categories

    Then open lap_chart.html in a browser. Nothing is written back to the
    CSV or to any database.
"""

import argparse
import csv
import html
import json
import re
from pathlib import Path

LAP_COL_RE = re.compile(r"^Lap\s*(\d+)$", re.IGNORECASE)

PODIUM_COLOURS_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a"]  # blue, orange, aqua
PODIUM_COLOURS_DARK = ["#3987e5", "#d95926", "#199e70"]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_time_seconds(s):
    """'HH:MM:SS.s' / 'MM:SS.s' -> seconds (float). None if blank/unparseable."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    parts = s.split(":")
    try:
        if len(parts) == 3:
            h, m, sec = parts
            return int(h) * 3600 + int(m) * 60 + float(sec)
        if len(parts) == 2:
            m, sec = parts
            return int(m) * 60 + float(sec)
        return float(parts[0])
    except ValueError:
        return None


def format_seconds(sec):
    if sec is None:
        return "-"
    sec = round(sec, 1)
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    if h:
        return f"{h}:{m:02d}:{s:04.1f}"
    return f"{m}:{s:04.1f}"


def format_name(full_name):
    """'Nicky Morris' -> 'Nicky M.' -- first name + surname initial only.
    Matches export_rider_pages.py's privacy convention: full surnames are
    never shown publicly. Single-word names (no surname on file) pass
    through unchanged."""
    full_name = (full_name or "").strip()
    if not full_name:
        return full_name
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0]
    first = parts[0]
    initial = parts[-1][0].upper() if parts[-1] else ""
    return f"{first} {initial}." if initial else first


def format_club(club, max_len=20):
    """Truncate a long club name for the compact standings-list column
    (2026-09-20, at Adam's request -- the sidebar was too wide with full
    club names). Cuts to max_len total characters including the trailing
    "..." marker, on a word boundary where possible so it doesn't end
    mid-word. Only affects the standings-list display text; the full club
    name is still used in the hover/focus tooltip and the aria-label, so
    nothing is lost, just not shown in the narrow column."""
    c = (club or "").strip()
    if len(c) <= max_len:
        return c
    cut = c[:max_len - 1].rstrip()
    # Prefer breaking on the last space if that doesn't throw away too much
    # (avoids "Wolverhampton Wh..." -> keep it a touch shorter but cleaner).
    last_space = cut.rfind(" ")
    if last_space > max_len * 0.6:
        cut = cut[:last_space]
    return cut + "…"


def normalise_gender(raw_gender):
    """'Male'/'M'/'m' -> 'M', 'Female'/'F'/'f' -> 'F', anything else -> '?'.
    Mirrors normalise_gender() in import_race_results.py."""
    g = (raw_gender or "").strip().lower()
    if g.startswith("f"):
        return "F"
    if g.startswith("m"):
        return "M"
    return "?"


def slugify(text):
    """'Youth (Female)' -> 'Youth-Female' -- used for HTML element ids."""
    out = []
    prev_dash = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        elif not prev_dash:
            out.append("-")
            prev_dash = True
    return "".join(out).strip("-")


def scoring_category(raw_category):
    """
    Groups raw D3 Category text the way the league actually scores it, for
    chart-splitting purposes only (the raw text is still kept for the
    console diagnostics). Currently just one merge: any Masters band aged
    60 and up (M60, M65, M70, M70+, ...) folds into one "M60+" group,
    mirroring normalise_category()'s age>=60 -> "M60" rule in
    import_race_results.py, and matching Adam's 2026-09-20 request to fold
    the (typically tiny) M70 field into M60.

    Only masters bands are touched -- Senior/Junior/Under 23/etc. pass
    through as their raw text unchanged.
    """
    s = (raw_category or "").strip()
    low = s.lower()
    if low.startswith("m"):
        digits = "".join(ch if ch.isdigit() else " " for ch in s).split()
        if digits:
            age = int(digits[0])
            if age >= 60:
                return "M60+"
    return s


def find_lap_columns(headers):
    """Returns [(header_name, lap_number), ...] sorted by lap number."""
    cols = []
    for h in headers:
        m = LAP_COL_RE.match(h.strip())
        if m:
            cols.append((h, int(m.group(1))))
    cols.sort(key=lambda x: x[1])
    return cols


def load_riders(csv_path: Path):
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        lap_cols = find_lap_columns(headers)
        if not lap_cols:
            raise SystemExit(
                "No Lap1/Lap 2/... columns found in this CSV -- is this a "
                "raw D3 RaceTec export?"
            )

        riders = []
        for row in reader:
            race_no = (row.get("Race No") or "").strip()
            name = (row.get("Name") or "").strip()
            category_raw = (row.get("Category") or "").strip()
            category = scoring_category(category_raw)
            gender = normalise_gender(row.get("Gender"))
            club = (row.get("Club") or "").strip()
            raw_time = row.get("Time")
            cat_pos_csv = (row.get("Cat Pos") or "").strip()

            if not race_no or not category_raw:
                continue

            is_dnf = str(raw_time or "").strip().upper() == "DNF"

            # Collect splits: contiguous non-blank values from Lap1 onward.
            splits = []
            for col_name, _lap_no in lap_cols:
                val = (row.get(col_name) or "").strip()
                if val == "":
                    break
                secs = parse_time_seconds(val)
                if secs is None:
                    break
                splits.append(secs)

            if is_dnf or not splits:
                # No reliable per-lap data to plot for this rider.
                continue

            cumulative = []
            running = 0.0
            for s in splits:
                running += s
                cumulative.append(running)

            riders.append({
                "race_no": race_no,
                "name": name or f"#{race_no}",
                "category": category,           # merged/scoring group (chart split key)
                "category_raw": category_raw,   # exact CSV text (diagnostics only)
                "gender": gender,                # 'F' / 'M' / '?' -- see --split-gender
                "club": club,
                "cumulative": cumulative,       # seconds, index 0 = "Start"
                "final_time_csv": parse_time_seconds(raw_time),
                "cat_pos_csv": int(cat_pos_csv) if cat_pos_csv.isdigit() else None,
            })

        return riders, len(lap_cols)


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

def compute_category_series(riders_in_cat, max_laps):
    """
    For each lap index k (1..max_laps), rank riders who have reached that
    lap by cumulative time so far. Returns:
      series[race_no] = [(k, rank, cum_seconds), ...]   (stops at rider's last lap)
      field_size = number of riders in this category
    """
    series = {r["race_no"]: [] for r in riders_in_cat}
    field_size = len(riders_in_cat)

    for k in range(1, max_laps + 1):
        idx = k - 1
        reached = [r for r in riders_in_cat if len(r["cumulative"]) > idx]
        reached.sort(key=lambda r: r["cumulative"][idx])
        for rank, r in enumerate(reached, start=1):
            series[r["race_no"]].append((k, rank, r["cumulative"][idx]))

    return series, field_size


def validate_against_csv(riders_in_cat, series):
    """Sanity check: our final computed rank should match the CSV's own Cat Pos
    for riders who have a Cat Pos recorded. Returns a list of mismatch strings."""
    mismatches = []
    for r in riders_in_cat:
        s = series[r["race_no"]]
        if not s or r["cat_pos_csv"] is None:
            continue
        _, our_final_rank, _ = s[-1]
        if our_final_rank != r["cat_pos_csv"] and len(s) == max(len(x["cumulative"]) for x in riders_in_cat):
            # Only flag it when this rider did the category's max lap count --
            # someone who finished early on the bell lap can legitimately have
            # a different "rank at their own last lap" than their overall Cat Pos.
            mismatches.append(
                f"  {r['name']} (#{r['race_no']}, {r['category_raw']}): "
                f"our computed rank {our_final_rank} vs CSV Cat Pos {r['cat_pos_csv']}"
            )
    return mismatches


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def esc(s):
    return html.escape(str(s), quote=True)


def render_category_chart(category, riders_in_cat, series, max_laps, dark_index):
    """Returns an HTML snippet (SVG chart + linked standings list) for one category."""
    field_size = len(riders_in_cat)
    by_race_no = {r["race_no"]: r for r in riders_in_cat}

    # Final standing (by last point in their own series) for the list + podium colours.
    finishers = []
    for r in riders_in_cat:
        s = series[r["race_no"]]
        if not s:
            continue
        last_k, last_rank, last_cum = s[-1]
        finishers.append((last_rank, last_k, r))
    finishers.sort(key=lambda t: (t[0], -t[1]))  # by rank, then who got further

    podium_race_nos = [r["race_no"] for _rank, _k, r in finishers[:3]]
    podium_colour = {
        rn: (PODIUM_COLOURS_LIGHT[i], PODIUM_COLOURS_DARK[i])
        for i, rn in enumerate(podium_race_nos)
    }

    # --- geometry ---
    margin_left, margin_right = 30, 14
    margin_top, margin_bottom = 18, 34
    plot_w = max(420, min(920, max_laps * 78))
    plot_h = max(180, min(760, field_size * 20))
    col_step = plot_w / max(1, max_laps - 1)
    row_step = plot_h / max(1, field_size - 1) if field_size > 1 else 0
    svg_w = margin_left + plot_w + margin_right
    svg_h = margin_top + plot_h + margin_bottom

    def x_of(k):
        return margin_left + (k - 1) * col_step

    def y_of(rank):
        return margin_top + (rank - 1) * row_step

    # --- gridlines + y ticks (sparser on big fields) ---
    if field_size <= 15:
        tick_ranks = list(range(1, field_size + 1))
    else:
        tick_ranks = sorted(set([1] + list(range(5, field_size, 5)) + [field_size]))

    grid_svg = []
    for rk in tick_ranks:
        y = y_of(rk)
        grid_svg.append(
            f'<line class="gridline" x1="{margin_left}" y1="{y:.1f}" '
            f'x2="{margin_left + plot_w}" y2="{y:.1f}" />'
        )
        grid_svg.append(
            f'<text class="tick-label" x="{margin_left - 8}" y="{y:.1f}" '
            f'text-anchor="end" dominant-baseline="middle">{rk}</text>'
        )

    # Faint dotted vertical line at each lap position (2026-09-20, at Adam's
    # request), so it's easy to see exactly which lap a position change
    # happened on -- lines up with the "Start"/"Lap N" label directly below.
    for k in range(1, max_laps + 1):
        x = x_of(k)
        grid_svg.append(
            f'<line class="gridline-v" x1="{x:.1f}" y1="{margin_top}" '
            f'x2="{x:.1f}" y2="{margin_top + plot_h}" />'
        )

    x_labels_svg = []
    for k in range(1, max_laps + 1):
        label = "Start" if k == 1 else f"Lap {k}"
        x_labels_svg.append(
            f'<text class="tick-label" x="{x_of(k):.1f}" y="{margin_top + plot_h + 20}" '
            f'text-anchor="middle">{label}</text>'
        )

    # --- lines ---
    lines_svg = []
    hit_svg = []
    for r in riders_in_cat:
        s = series[r["race_no"]]
        if not s:
            continue
        pts = " ".join(f"{x_of(k):.1f},{y_of(rank):.1f}" for k, rank, _cum in s)
        is_podium = r["race_no"] in podium_colour
        cls = "rider-line rider-podium" if is_podium else "rider-line rider-muted"
        style = ""
        if is_podium:
            light, dark = podium_colour[r["race_no"]]
            style = f' style="--podium-light:{light};--podium-dark:{dark};"'
        lines_svg.append(
            f'<polyline class="{cls}" points="{pts}"{style} '
            f'data-race-no="{esc(r["race_no"])}" />'
        )
        display_name = format_name(r["name"])
        hit_svg.append(
            f'<polyline class="rider-hit" points="{pts}" '
            f'data-race-no="{esc(r["race_no"])}" '
            f'data-name="{esc(display_name)}" data-club="{esc(r["club"])}" '
            f'tabindex="0" role="button" '
            f'aria-label="{esc(display_name)}, {esc(r["club"] or "no club listed")}" />'
        )

    # --- standings list (also the table-view fallback) ---
    rows_html = []
    for rank, _k, r in finishers:
        final_time = format_seconds(r["final_time_csv"])
        laps_done = len(r["cumulative"])
        podium_cls = " podium" if r["race_no"] in podium_colour else ""
        style = ""
        if r["race_no"] in podium_colour:
            light, dark = podium_colour[r["race_no"]]
            style = f' style="--podium-light:{light};--podium-dark:{dark};"'
        short_note = "" if laps_done == max_laps else f'<span class="note">{laps_done} laps</span>'
        club_full = (r["club"] or "").strip()
        club_short = format_club(club_full)
        # Always carry the full name in title -- CSS ellipsis can still clip
        # visually even when the 20-char cap above didn't have to (long
        # narrow-column names, wide characters), so this is the fallback
        # for that case too, not just for names format_club shortened.
        club_title = f' title="{esc(club_full)}"' if club_full else ""
        rows_html.append(
            f'<li class="standing-row{podium_cls}" data-race-no="{esc(r["race_no"])}" '
            f'tabindex="0"{style}>'
            f'<span class="rank">{rank}</span>'
            f'<span class="name">{esc(format_name(r["name"]))}</span>'
            f'<span class="club"{club_title}>{esc(club_short)}</span>'
            f'<span class="time">{esc(final_time)}</span>'
            f'{short_note}'
            f'</li>'
        )

    legend_items = []
    for rank, _k, r in finishers[:3]:
        light, dark = podium_colour[r["race_no"]]
        legend_items.append(
            f'<span class="legend-item" style="--podium-light:{light};--podium-dark:{dark};">'
            f'<span class="swatch"></span>{esc(format_name(r["name"]))}</span>'
        )

    return f"""
  <section class="category-block" id="cat-{esc(slugify(category))}">
    <h2>{esc(category)} <span class="field-size">({field_size} riders)</span></h2>
    <div class="legend">{''.join(legend_items)}<span class="legend-item legend-muted"><span class="swatch"></span>everyone else (hover or search)</span></div>
    <div class="pin-legend" data-hint="Click a name in the list to compare it &mdash; click another to add it, click a pinned name again to remove it."></div>
    <div class="chart-row">
      <div class="chart-scroll">
        <svg class="lap-svg" viewBox="0 0 {svg_w:.1f} {svg_h:.1f}" role="img"
             aria-label="Position by lap for {esc(category)}">
          {''.join(grid_svg)}
          {''.join(x_labels_svg)}
          {''.join(lines_svg)}
          {''.join(hit_svg)}
        </svg>
      </div>
      <div class="standings-col">
        <div class="laps-note">Up to {max_laps} laps</div>
        <ol class="standings">{''.join(rows_html)}</ol>
        <div class="laps-note">Up to {max_laps} laps</div>
      </div>
    </div>
  </section>"""


PAGE_CSS = """
:root {
  color-scheme: light;
  --surface-1:      #fcfcfb;
  --page-plane:     #f9f9f7;
  --text-primary:   #0b0b0b;
  --text-secondary: #52514e;
  --text-muted:     #898781;
  --gridline:       #e1e0d9;
  --baseline:       #c3c2b7;
  --line-muted:     #b3b1a8;
  --border:         rgba(11,11,11,0.10);
  --accent:         #2a78d6;
}
@media (prefers-color-scheme: dark) {
  :root {
    color-scheme: dark;
    --surface-1:      #1a1a19;
    --page-plane:     #0d0d0d;
    --text-primary:   #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted:     #898781;
    --gridline:       #2c2c2a;
    --baseline:       #383835;
    --line-muted:     #55534c;
    --border:         rgba(255,255,255,0.10);
    --accent:         #3987e5;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0;
  padding: 24px 16px 60px;
  background: var(--page-plane);
  color: var(--text-primary);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
}
.page-header { max-width: 1000px; margin: 0 auto 8px; }
.page-header h1 { margin: 0 0 4px; font-size: 22px; }
.page-header p { color: var(--text-secondary); margin: 4px 0; font-size: 14px; }
.page-header .caveats { color: var(--text-muted); font-size: 13px; }
.search-row { max-width: 1000px; margin: 16px auto 24px; }
.search-row input {
  width: 100%; max-width: 320px; padding: 8px 10px; font-size: 14px;
  border: 1px solid var(--border); border-radius: 6px;
  background: var(--surface-1); color: var(--text-primary);
}
.category-block {
  max-width: 1000px; margin: 0 auto 40px; background: var(--surface-1);
  border: 1px solid var(--border); border-radius: 10px; padding: 18px 20px;
}
.category-block h2 { margin: 0 0 8px; font-size: 17px; }
.field-size { color: var(--text-muted); font-weight: normal; font-size: 13px; }
.legend { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 12px; font-size: 13px; color: var(--text-secondary); }
.legend-item { display: inline-flex; align-items: center; gap: 6px; }
.legend-item .swatch { width: 14px; height: 3px; border-radius: 2px; background: var(--podium-light); }
@media (prefers-color-scheme: dark) { .legend-item .swatch { background: var(--podium-dark); } }
.legend-muted .swatch { background: var(--line-muted); }
.pin-legend { display: none; flex-wrap: wrap; gap: 8px; align-items: center; margin: -4px 0 12px; font-size: 12.5px; color: var(--text-muted); }
.pin-legend.has-hint, .pin-legend.has-pins { display: flex; }
.pin-chip {
  display: inline-flex; align-items: center; gap: 6px; padding: 3px 6px 3px 9px; border-radius: 999px;
  background: var(--gridline); color: var(--text-primary); cursor: pointer; border: none; font: inherit;
}
.pin-chip .swatch { width: 9px; height: 9px; border-radius: 50%; background: var(--pin-light); flex: none; }
@media (prefers-color-scheme: dark) { .pin-chip .swatch { background: var(--pin-dark); } }
.pin-chip .pin-remove { color: var(--text-muted); font-weight: 700; padding-left: 2px; }
.chart-row { display: flex; gap: 18px; flex-wrap: wrap; align-items: flex-start; }
.chart-scroll { overflow-x: auto; flex: 1 1 480px; min-width: 0; }
.standings-col { flex: 0 0 260px; }
.laps-note { color: var(--text-muted); font-size: 11px; padding: 3px 6px; }
.lap-svg { display: block; width: 100%; height: auto; min-width: 420px; }
.gridline { stroke: var(--gridline); stroke-width: 1; pointer-events: none; }
.gridline-v { stroke: var(--gridline); stroke-width: 1; stroke-dasharray: 1.5 3; stroke-linecap: round; opacity: 0.7; pointer-events: none; }
.tick-label { fill: var(--text-muted); font-size: 10px; pointer-events: none; }
.rider-line { fill: none; stroke-width: 1.4; pointer-events: none; }
.rider-muted { stroke: var(--line-muted); opacity: 0.55; }
.rider-podium { stroke: var(--podium-light); stroke-width: 2.2; opacity: 0.95; }
@media (prefers-color-scheme: dark) { .rider-podium { stroke: var(--podium-dark); } }
.rider-hit { fill: none; stroke: #000; opacity: 0; stroke-width: 14; cursor: pointer; pointer-events: stroke; }
.rider-hit:focus { outline: none; }
.rider-line.pinned { stroke: var(--pin-light) !important; stroke-width: 2.4 !important; opacity: 1 !important; }
@media (prefers-color-scheme: dark) { .rider-line.pinned { stroke: var(--pin-dark) !important; } }
.rider-line.pin-dim { opacity: 0.12; }
.standing-row.pinned { background: var(--gridline); box-shadow: inset 3px 0 0 var(--pin-light); }
@media (prefers-color-scheme: dark) { .standing-row.pinned { box-shadow: inset 3px 0 0 var(--pin-dark); } }
.standing-row.pinned .name { color: var(--pin-light); font-weight: 700; }
@media (prefers-color-scheme: dark) { .standing-row.pinned .name { color: var(--pin-dark); } }
.rider-line.active { stroke: var(--accent) !important; stroke-width: 3 !important; opacity: 1 !important; }
.rider-line.dimmed { opacity: 0.12; }
.rider-line.search-match { stroke: var(--accent) !important; stroke-width: 2.6 !important; opacity: 1 !important; }
.rider-line.search-dim { opacity: 0.08; }
.standings {
  list-style: none; margin: 0; padding: 0; max-height: 480px;
  overflow-y: auto; border-top: 1px solid var(--border);
}
.standing-row {
  display: flex; align-items: baseline; gap: 8px; padding: 4px 6px;
  border-bottom: 1px solid var(--border); font-size: 12.5px; cursor: pointer;
  font-variant-numeric: tabular-nums;
}
.standing-row .rank { width: 20px; color: var(--text-muted); flex: none; }
.standing-row .name { flex: 0 1 96px; min-width: 60px; color: var(--text-primary); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.standing-row .club { flex: 0 1 130px; color: var(--text-muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; display: none; }
.standing-row .time { color: var(--text-secondary); flex: none; }
.standing-row .note { color: var(--text-muted); font-size: 11px; flex: none; }
.standing-row.podium .name { color: var(--podium-light); font-weight: 600; }
@media (prefers-color-scheme: dark) { .standing-row.podium .name { color: var(--podium-dark); } }
.standing-row.active, .standing-row:hover, .standing-row:focus { background: var(--gridline); }
@media (min-width: 720px) { .standing-row .club { display: block; } }
.tooltip {
  position: fixed; pointer-events: none; background: var(--text-primary); color: var(--surface-1);
  padding: 6px 9px; border-radius: 6px; font-size: 12.5px; line-height: 1.4;
  max-width: 220px; z-index: 10; display: none; box-shadow: 0 2px 8px rgba(0,0,0,0.25);
}
.tooltip .t-name { font-weight: 600; }
.tooltip .t-value { font-variant-numeric: tabular-nums; }
"""

# Auto-resizes the iframe this page is embedded in (e.g. a WordPress page
# with an <iframe src="/lap-charts/..."> block), so the page can keep the
# site's header/footer while this chart fills the iframe. Byte-identical
# to the IFRAME_RESIZE_SCRIPT already used by export_league_tables_html.py,
# export_rider_pages.py and export_team_awards_html.py -- see
# claude/html-league-tables-plan.md's "WordPress presentation" section for
# the full story of the back-navigation / shrink-to-fit bug this fixes.
# Only does anything when window.frameElement is reachable, which is only
# true in a SAME-ORIGIN iframe -- opening the page directly, or a
# cross-origin embed, leaves it a harmless no-op.
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

PAGE_JS = """
(function () {
  var tooltip = document.createElement('div');
  tooltip.className = 'tooltip';
  document.body.appendChild(tooltip);

  function showTooltip(evt, name, club, section) {
    var nameEl = document.createElement('div');
    nameEl.className = 't-name';
    nameEl.textContent = name;
    var clubEl = document.createElement('div');
    clubEl.textContent = club || '';
    tooltip.textContent = '';
    tooltip.appendChild(nameEl);
    if (club) tooltip.appendChild(clubEl);
    tooltip.style.display = 'block';
    moveTooltip(evt);
  }
  function moveTooltip(evt) {
    var x = evt.clientX, y = evt.clientY;
    tooltip.style.left = Math.min(x + 14, window.innerWidth - 230) + 'px';
    tooltip.style.top = Math.min(y + 14, window.innerHeight - 60) + 'px';
  }
  function hideTooltip() { tooltip.style.display = 'none'; }

  document.querySelectorAll('.category-block').forEach(function (section) {
    var lines = section.querySelectorAll('.rider-line');
    var hits = section.querySelectorAll('.rider-hit');
    var rows = section.querySelectorAll('.standing-row');

    function lineFor(raceNo) {
      return section.querySelector('.rider-line[data-race-no="' + CSS.escape(raceNo) + '"]');
    }
    function rowFor(raceNo) {
      return section.querySelector('.standing-row[data-race-no="' + CSS.escape(raceNo) + '"]');
    }
    function highlight(raceNo) {
      lines.forEach(function (l) {
        if (l.getAttribute('data-race-no') === raceNo) {
          l.classList.add('active');
          l.parentNode.appendChild(l); // bring to front
        } else {
          l.classList.add('dimmed');
        }
      });
      var row = rowFor(raceNo);
      if (row) row.classList.add('active');
    }
    function clearHighlight() {
      lines.forEach(function (l) { l.classList.remove('active', 'dimmed'); });
      rows.forEach(function (r) { r.classList.remove('active'); });
      hideTooltip();
    }

    hits.forEach(function (hit) {
      var raceNo = hit.getAttribute('data-race-no');
      var name = hit.getAttribute('data-name');
      var club = hit.getAttribute('data-club');
      hit.addEventListener('pointerenter', function (e) { highlight(raceNo); showTooltip(e, name, club); });
      hit.addEventListener('pointermove', moveTooltip);
      hit.addEventListener('pointerleave', clearHighlight);
      hit.addEventListener('focus', function (e) { highlight(raceNo); showTooltip(e, name, club); });
      hit.addEventListener('blur', clearHighlight);
    });

    rows.forEach(function (row) {
      var raceNo = row.getAttribute('data-race-no');
      row.addEventListener('mouseenter', function () { highlight(raceNo); });
      row.addEventListener('focus', function () { highlight(raceNo); });
      row.addEventListener('mouseleave', clearHighlight);
      row.addEventListener('blur', clearHighlight);
    });

    // --- Click-to-pin: keeps a rider's line highlighted after the mouse
    // moves away, so two or more riders can be compared side by side.
    // Podium riders already have a permanent colour + legend entry, so
    // pinning only applies to the muted "everyone else" riders.
    var PIN_LIGHT = ['#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948'];
    var PIN_DARK  = ['#c98500', '#d55181', '#008300', '#9085e9', '#e66767'];
    var pinned = {};       // raceNo -> colour index
    var usedIdx = [];
    var pinLegend = section.querySelector('.pin-legend');

    function isPodium(raceNo) {
      var l = lineFor(raceNo);
      return !!(l && l.classList.contains('rider-podium'));
    }
    function nextFreeIndex() {
      for (var i = 0; i < PIN_LIGHT.length; i++) {
        if (usedIdx.indexOf(i) === -1) return i;
      }
      return -1;
    }
    function togglePin(raceNo) {
      if (isPodium(raceNo)) return; // already permanently highlighted
      if (Object.prototype.hasOwnProperty.call(pinned, raceNo)) {
        usedIdx.splice(usedIdx.indexOf(pinned[raceNo]), 1);
        delete pinned[raceNo];
      } else {
        var idx = nextFreeIndex();
        if (idx === -1) return; // comparison slots full - ignore further pins
        pinned[raceNo] = idx;
        usedIdx.push(idx);
      }
      renderPins();
    }
    function renderPins() {
      var raceNos = Object.keys(pinned);
      var anyPinned = raceNos.length > 0;

      lines.forEach(function (l) {
        var rn = l.getAttribute('data-race-no');
        l.classList.remove('pinned', 'pin-dim');
        l.style.removeProperty('--pin-light');
        l.style.removeProperty('--pin-dark');
        if (Object.prototype.hasOwnProperty.call(pinned, rn)) {
          var idx = pinned[rn];
          l.style.setProperty('--pin-light', PIN_LIGHT[idx]);
          l.style.setProperty('--pin-dark', PIN_DARK[idx]);
          l.classList.add('pinned');
          l.parentNode.appendChild(l); // bring to front, on top of hover reordering too
        } else if (anyPinned && l.classList.contains('rider-muted')) {
          l.classList.add('pin-dim');
        }
      });

      rows.forEach(function (r) {
        var rn = r.getAttribute('data-race-no');
        r.classList.remove('pinned');
        r.style.removeProperty('--pin-light');
        r.style.removeProperty('--pin-dark');
        if (Object.prototype.hasOwnProperty.call(pinned, rn)) {
          r.style.setProperty('--pin-light', PIN_LIGHT[pinned[rn]]);
          r.style.setProperty('--pin-dark', PIN_DARK[pinned[rn]]);
          r.classList.add('pinned');
        }
      });

      if (pinLegend) {
        pinLegend.textContent = '';
        pinLegend.classList.remove('has-hint', 'has-pins');
        if (!anyPinned) {
          pinLegend.textContent = pinLegend.getAttribute('data-hint') || '';
          pinLegend.classList.add('has-hint');
        } else {
          raceNos.forEach(function (rn) {
            var hit = section.querySelector('.rider-hit[data-race-no="' + CSS.escape(rn) + '"]');
            var name = hit ? hit.getAttribute('data-name') : rn;
            var chip = document.createElement('button');
            chip.type = 'button';
            chip.className = 'pin-chip';
            chip.style.setProperty('--pin-light', PIN_LIGHT[pinned[rn]]);
            chip.style.setProperty('--pin-dark', PIN_DARK[pinned[rn]]);
            var swatch = document.createElement('span');
            swatch.className = 'swatch';
            var label = document.createElement('span');
            label.textContent = name;
            var remove = document.createElement('span');
            remove.className = 'pin-remove';
            remove.textContent = '\\u00d7';
            chip.appendChild(swatch);
            chip.appendChild(label);
            chip.appendChild(remove);
            chip.setAttribute('aria-label', 'Remove ' + name + ' from comparison');
            chip.addEventListener('click', function () { togglePin(rn); });
            pinLegend.appendChild(chip);
          });
          pinLegend.classList.add('has-pins');
        }
      }
    }

    hits.forEach(function (hit) {
      var raceNo = hit.getAttribute('data-race-no');
      hit.addEventListener('click', function () { togglePin(raceNo); });
      hit.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); togglePin(raceNo); }
      });
    });
    rows.forEach(function (row) {
      var raceNo = row.getAttribute('data-race-no');
      row.addEventListener('click', function () { togglePin(raceNo); });
      row.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); togglePin(raceNo); }
      });
    });

    renderPins(); // shows the initial hint text
  });

  var search = document.getElementById('rider-search');
  if (search) {
    search.addEventListener('input', function () {
      var q = search.value.trim().toLowerCase();
      document.querySelectorAll('.category-block').forEach(function (section) {
        var lines = section.querySelectorAll('.rider-line');
        var hits = section.querySelectorAll('.rider-hit');
        if (!q) {
          lines.forEach(function (l) { l.classList.remove('search-match', 'search-dim'); });
          return;
        }
        var matchRaceNos = {};
        hits.forEach(function (h) {
          if ((h.getAttribute('data-name') || '').toLowerCase().indexOf(q) !== -1) {
            matchRaceNos[h.getAttribute('data-race-no')] = true;
          }
        });
        lines.forEach(function (l) {
          var rn = l.getAttribute('data-race-no');
          if (matchRaceNos[rn]) {
            l.classList.add('search-match');
            l.classList.remove('search-dim');
            l.parentNode.appendChild(l);
          } else {
            l.classList.add('search-dim');
            l.classList.remove('search-match');
          }
        });
      });
    });
  }
})();
"""


def render_page(title, category_blocks_html, caveats):
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>{PAGE_CSS}</style>
</head>
<body>
  <div class="page-header">
    <h1>{esc(title)}</h1>
    <p>Position after each lap, per category. Lines stop at each rider's own last recorded lap
       (WMCCL's time-cutoff rule means riders in the same category can legitimately finish on
       different lap counts). "Start" is the run to the first timing point, not a full lap.</p>
    <p class="caveats">{caveats}</p>
  </div>
  <div class="search-row">
    <input id="rider-search" type="text" placeholder="Find a rider by name (all categories)&hellip;">
  </div>
  {category_blocks_html}
  <script>{PAGE_JS}</script>
  {IFRAME_RESIZE_SCRIPT}
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

GENDER_LABEL = {"F": "Female", "M": "Male"}


def build_chart_groups(riders, split_gender, merge_all_categories=False, merged_title="All categories combined"):
    """
    Returns an ordered list of (chart_title, riders_subset) pairs -- normally
    one per scoring category.

    merge_all_categories (see --merge-all-categories / design note #12):
    when True, the CSV's own Category column is ignored entirely and every
    rider in the file is treated as a single combined field -- e.g. WMCCL's
    Women races, which run every category as one shared start (mirrors
    league_scoring.py's own "Women_All" group, which already merges every
    Women's category into one table for league points). merged_title names
    the resulting single chart.

    split_gender (see --split-gender / design note #9): a category (or, if
    merge_all_categories is also set, the one merged field) containing 2+
    riders of different known genders (M/F) is broken into separate
    per-gender groups instead of one combined chart -- WMCCL's Youth
    category is currently the only place this applies on its own (girls and
    boys race together on the same start, unlike every other category which
    is already single-gender or has its own DB), but it composes with
    merge_all_categories too in case that's ever useful. A rider with no
    gender on file (blank/unrecognised) is never dropped -- if a split
    happens and any such riders exist, they get their own small
    "(gender not recorded)" chart rather than silently vanishing.

    Returns (groups, split_categories). Each group is a
    (chart_title, riders_subset, skip_reason) triple. skip_reason is None
    for an ordinary, unmodified category; otherwise it's a short phrase
    explaining why the CSV's own "Cat Pos" isn't valid ground truth for
    this group's own computed rank -- gender-splitting or merging
    categories both change who a rider is being ranked against, so neither
    can be checked against a Cat Pos computed under the CSV's original
    grouping. main() uses this to skip that particular sanity check for
    the affected groups (and says why) rather than reporting a wall of
    false "mismatches". split_categories lists which original category
    names actually got gender-split (for the console summary).
    """
    if merge_all_categories:
        categories = [merged_title]
        cat_riders = {merged_title: list(riders)}
    else:
        categories = sorted(set(r["category"] for r in riders))
        cat_riders = {cat: [r for r in riders if r["category"] == cat] for cat in categories}

    groups = []
    split_categories = []
    for cat in categories:
        riders_in_cat = cat_riders[cat]
        known_genders = sorted(set(r["gender"] for r in riders_in_cat if r["gender"] in ("F", "M")))
        merge_reason = "categories merged via --merge-all-categories, CSV Cat Pos is per original category" if merge_all_categories else None
        if split_gender and len(known_genders) > 1:
            split_categories.append(cat)
            split_reason = (
                "categories merged and gender-split, CSV Cat Pos matches neither"
                if merge_all_categories
                else "CSV Cat Pos is for the combined field, not this gender split"
            )
            for g in known_genders:
                sub = [r for r in riders_in_cat if r["gender"] == g]
                groups.append((f"{cat} ({GENDER_LABEL[g]})", sub, split_reason))
            unknown = [r for r in riders_in_cat if r["gender"] not in ("F", "M")]
            if unknown:
                groups.append((f"{cat} (gender not recorded)", unknown, split_reason))
        else:
            groups.append((cat, riders_in_cat, merge_reason))
    return groups, split_categories


def main():
    ap = argparse.ArgumentParser(description="Build a one-off position-by-lap chart from a raw D3 CSV.")
    ap.add_argument("--csv", required=True, help="Raw D3 RaceTec results CSV")
    ap.add_argument("--out", default="lap_chart.html", help="Output HTML file")
    ap.add_argument("--title", default=None, help="Page title (default: derived from filename)")
    ap.add_argument(
        "--split-gender", action="store_true",
        help="Split any category containing both female and male riders into "
             "separate charts (one per gender), instead of one combined chart. "
             "Only affects a category that actually has both genders present -- "
             "currently just Youth (U14+U16), the one WMCCL category where girls "
             "and boys share a start; everything else is untouched either way."
    )
    ap.add_argument(
        "--merge-all-categories", action="store_true",
        help="Ignore the CSV's own Category column and combine every rider in "
             "the file into ONE merged chart, instead of one chart per "
             "category. For a race where every category genuinely shares a "
             "single start -- e.g. WMCCL's Women races, which league scoring "
             "already treats as one combined group for points (see "
             "league_scoring.py's 'Women_All'). Only safe if that's actually "
             "true of the file being charted: the same raw-category "
             "Start-time-spread warning used for the M60+M70 merge fires "
             "here too if the categories being merged don't look like they "
             "actually shared a start line."
    )
    ap.add_argument(
        "--merged-title", default="All categories combined",
        help="Chart title to use when --merge-all-categories is set "
             "(default: 'All categories combined')."
    )
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    riders, n_lap_cols = load_riders(csv_path)
    print(f"Loaded {len(riders)} riders with usable lap data ({n_lap_cols} lap columns in header).")

    groups, split_categories = build_chart_groups(
        riders, args.split_gender,
        merge_all_categories=args.merge_all_categories,
        merged_title=args.merged_title,
    )
    print(f"Categories found: {', '.join(sorted(set(r['category'] for r in riders)))}")
    if args.merge_all_categories:
        print(f"Merged ALL categories into one chart (--merge-all-categories): '{args.merged_title}'")
    if split_categories:
        print(f"Split by gender (--split-gender): {', '.join(split_categories)}")
    elif args.split_gender:
        print("--split-gender was set, but no category has more than one gender present -- nothing to split.")

    blocks = []
    all_mismatches = []
    any_skipped = False
    for title, riders_in_cat, skip_reason in groups:
        max_laps = max(len(r["cumulative"]) for r in riders_in_cat)
        series, field_size = compute_category_series(riders_in_cat, max_laps)
        if skip_reason:
            # Gender-splitting and/or merging categories both change who a
            # rider is being ranked against, so the CSV's own Cat Pos (computed
            # under its original grouping) isn't valid ground truth for this
            # group's own computed rank -- skip it rather than report a wall
            # of expected "mismatches".
            any_skipped = True
            note = f"  (Cat Pos sanity check skipped -- {skip_reason})"
        else:
            mismatches = validate_against_csv(riders_in_cat, series)
            all_mismatches.extend(mismatches)
            note = ""
        blocks.append(render_category_chart(title, riders_in_cat, series, max_laps, dark_index=0))
        print(f"  {title}: {field_size} riders, up to {max_laps} laps recorded{note}")

        # If this chart merges more than one raw CSV category (currently
        # only the M60+ fold-in), report the sub-groups and flag if their
        # Start-lap times look suspiciously separated -- that would mean
        # they didn't actually share a start line, and shouldn't be
        # charted together (see design note #6 at the top of this file).
        raw_subgroups = sorted(set(r["category_raw"] for r in riders_in_cat))
        if len(raw_subgroups) > 1:
            print(f"    merged from raw categories: {', '.join(raw_subgroups)}")
            spreads = {}
            for raw in raw_subgroups:
                starts = sorted(r["cumulative"][0] for r in riders_in_cat if r["category_raw"] == raw)
                mid = starts[len(starts) // 2]
                spreads[raw] = mid
                print(f"      {raw}: {len(starts)} riders, median Start split {format_seconds(mid)}")
            gap = max(spreads.values()) - min(spreads.values())
            if gap > 60:
                print(
                    f"    WARNING - median Start times differ by {format_seconds(gap)} across "
                    f"these sub-groups. That's the same signature a staggered start left in the "
                    f"Seniors pilot (M 40-49 vs everyone else) -- double-check these riders "
                    f"actually shared a start line before trusting this merged chart."
                )

    if all_mismatches:
        print("\nWARNING - computed final rank didn't match the CSV's own Cat Pos for:")
        for m in all_mismatches:
            print(m)
        print("(Check the CSV for ties broken by something other than time, or a data quirk.)")
    elif any_skipped:
        print(
            "\nSanity check passed for every group not skipped above (computed final "
            "ranks match the CSV's own Cat Pos column). Gender-split and/or merged-"
            "category groups aren't covered by this check -- see the per-group notes above."
        )
    else:
        print("\nSanity check passed: computed final ranks match the CSV's own Cat Pos column.")

    title = args.title or f"Lap-by-lap position - {csv_path.stem}"
    caveats = f"Source file: {esc(csv_path.name)}."
    page = render_page(title, "\n".join(blocks), caveats)

    out_path = Path(args.out)
    out_path.write_text(page, encoding="utf-8")
    print(f"\nWrote {out_path.resolve()}")
    print(
        "\nTo embed in WordPress: upload this file to the SAME DOMAIN as the "
        "WordPress site (e.g. alongside league_html/), then paste into a "
        "Custom HTML block:\n"
        f'  <iframe src="/path/to/{out_path.name}" style="width:100%; border:0;" '
        f'scrolling="no"></iframe>\n'
        "The auto-resize script only activates same-origin -- a different "
        "domain (or opening the file directly) just leaves the iframe at "
        "whatever height the block sets, no error."
    )


if __name__ == "__main__":
    main()
