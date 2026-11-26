#!/usr/bin/env python3
"""
rider_career.py
---------------
Career ("since 2025-26") figures, badges and previous-season links for the
HTML rider pages. Used by export_rider_pages.py; no command-line interface.

WHERE THE NUMBERS COME FROM
  * Earlier seasons: wmccl_history.db (season_stats), looked up by the
    rider's person_id (set on riders.person_id by assign_person_ids.py).
  * This season: the live race DB, computed by export_rider_pages.py's own
    compute_stats() -- so every weekly run adds the latest results on top
    of the history and nothing is ever stored as a running total.
  * Seasons are summed with `season < current season`, so archiving the
    current season at the end of the year can never double count it.

A rider with no person_id, or no earlier season in the history (a new
rider), simply gets no career section.

RULES THAT KEEP THIS SEASON CONSISTENT WITH THE ARCHIVE
  Average-points (AP) rows never carry laps or time (see sanitize_history),
  exactly as archive_season.py stores them, so the career figure at
  mid-season equals what the archive will record at season end.

PRIVACY
  Only counts and links are shown -- never DOB, never BC number.
"""

import html
import posixpath
import sqlite3
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

DEFAULT_PAGE_TEMPLATE = "/league_tables/{season}/riders/{race_number}.html"

# Career badge ladders: only the highest rung reached in each ladder is shown.
# Edit freely -- (threshold, label).
ROUNDS_LADDER = [(25, "25 career rounds"), (50, "50 career rounds"),
                 (100, "100 career rounds"), (150, "150 career rounds")]
LAPS_LADDER = [(250, "250 career laps"), (500, "500 career laps"),
               (1000, "1,000 career laps"), (2500, "2,500 career laps"),
               (5000, "5,000 career laps")]
HOURS_LADDER = [(10, "10 hours in the saddle"), (24, "A full day in the saddle (24h)"),
                (50, "50 hours in the saddle"), (100, "100 hours in the saddle")]
PODIUM_LADDER = [(1, "First podium"), (5, "5 career podiums"),
                 (10, "10 career podiums"), (25, "25 career podiums")]
SEASONS_LADDER = [(2, "2 seasons"), (3, "3 seasons"), (5, "5 seasons"), (10, "10 seasons")]

CAREER_CSS = """
  .wmccl-rider .badge.career { background: #fff1e0; color: #8a4b00; }
  .wmccl-rider .badge.founder { background: #f3e8ff; color: #5b21b6; }
  .wmccl-rider .career { margin: 1.25rem 0; }
  .wmccl-rider .career h2 { font-size: 1.05rem; margin: 0 0 0.4rem; }
  .wmccl-rider .career-table th, .wmccl-rider .career-table td { padding: 0.3rem 0.5rem; }
  .wmccl-rider .career-table td.num, .wmccl-rider .career-table th.num { text-align: right; }
  .wmccl-rider .ap-extra { font-size: 0.75rem; font-weight: 400; color: #666; }
  .wmccl-rider .career-note { font-size: 0.8rem; color: #888; margin: 0.3rem 0 0; }
  .wmccl-rider .prev-seasons { margin: 0.5rem 0 0; padding-left: 1.1rem; font-size: 0.9rem; }
  .wmccl-rider .prev-seasons li { margin: 0.15rem 0; }
"""


def esc(x) -> str:
    return html.escape(str(x if x is not None else ""))


# ---------------------------------------------------------------------------
# History access
# ---------------------------------------------------------------------------

class History:
    """Read-only handle on wmccl_history.db."""

    def __init__(self, path: str):
        p = Path(path)
        if not p.exists():
            raise SystemExit(f"❌ history DB not found: {p}")
        self.conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        have = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "season_stats" not in have:
            raise SystemExit(f"❌ {p} has no season_stats table -- is it a wmccl_history.db?")
        row = None
        if "schema_info" in have:
            row = self.conn.execute("SELECT value FROM schema_info WHERE key='page_template'").fetchone()
        self.page_template = (row[0] if row and row[0] else DEFAULT_PAGE_TEMPLATE)

    def person_for(self, season: str, source_db: str, race_number: int) -> Optional[int]:
        """person_id of an entry already ARCHIVED for `season` (lets past-season pages be
        regenerated without riders.person_id being set in that season's DBs)."""
        row = self.conn.execute(
            "SELECT person_id FROM season_stats WHERE season=? AND source_db=? AND race_number=?",
            (season, source_db, int(race_number))).fetchone()
        return row[0] if row else None

    def has_earlier_seasons(self, current_season: str) -> bool:
        """False when `current_season` is the first season in the history (nobody can be
        shown as new or returning from the history alone)."""
        return self.conn.execute(
            "SELECT 1 FROM season_stats WHERE season < ? LIMIT 1", (current_season,)).fetchone() is not None

    def prior_seasons(self, person_id: int, current_season: str) -> List[Dict]:
        cur = self.conn.execute("""
            SELECT season, source_db, race_number, race_category, races_completed, ap_rounds, laps,
                   time_seconds, golds, silvers, bronzes, full_house, page_path,
                   (SELECT COUNT(*) FROM round_results r
                     WHERE r.season = s.season AND r.source_db = s.source_db
                       AND r.race_number = s.race_number AND r.is_ap = 1
                       AND r.counts_for_career = 1) AS ap_all
            FROM season_stats s WHERE person_id = ? AND season < ?
            ORDER BY season, source_db, race_number
        """, (person_id, current_season))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def close(self):
        self.conn.close()


def sanitize_history(history: List[Tuple], ap_marker: int = 999) -> List[Tuple]:
    """AP rows never carry laps/time (matches archive_season.py)."""
    out = []
    for (rnd, cpos, opos, pts, is_ap, status, laps, time_) in history:
        if is_ap or pts == ap_marker:
            laps, time_ = None, None
        out.append((rnd, cpos, opos, pts, is_ap, status, laps, time_))
    return out


# ---------------------------------------------------------------------------
# Career summary + badges
# ---------------------------------------------------------------------------

def _podiums(g, s, b) -> int:
    return (g or 0) + (s or 0) + (b or 0)


def build_career(hist: History, person_id: Optional[int], current_season: str,
                 current_race_number: int, live_stats: Dict) -> Optional[Dict]:
    """
    Combine earlier seasons (history) with this season's live stats.
    live_stats must come from compute_stats() on sanitize_history() output.
    Returns None when there is nothing to show (no person_id / new rider).
    """
    if person_id is None:
        return None
    prior = hist.prior_seasons(person_id, current_season)
    if not prior:
        return None

    live_medals = list(live_stats.get("medal_rounds", {}).values())
    live_podiums = len(live_medals)
    live_full_house = 1 if live_stats.get("milestone_badge") == "Full House" else 0

    seasons = sorted({p["season"] for p in prior})
    career = {
        "first_season": seasons[0],
        "seasons_ridden": len(seasons) + 1,        # earlier seasons + this one
        "races": sum(p["races_completed"] or 0 for p in prior) + live_stats["races_completed"],
        # rounds = real races + average-points rounds in held rounds (drives the round badges)
        "ap_total": sum(p["ap_all"] or 0 for p in prior) + live_stats.get("ap_all", 0),
        "rounds": sum((p["races_completed"] or 0) + (p["ap_rounds"] or 0) for p in prior)
                  + live_stats["rounds_credited"],
        "laps": sum(p["laps"] or 0 for p in prior) + (live_stats["total_laps"] or 0),
        "time": sum(p["time_seconds"] or 0.0 for p in prior) + (live_stats["total_time_seconds"] or 0.0),
        "podiums": sum(_podiums(p["golds"], p["silvers"], p["bronzes"]) for p in prior) + live_podiums,
        "full_houses": sum(p["full_house"] or 0 for p in prior) + live_full_house,
        "full_house_prior": sorted(p["season"] for p in prior if p["full_house"]),
        "full_house_now": live_full_house,
        "this": {
            "ap": live_stats.get("ap_all", 0),
            "races": live_stats["races_completed"],
            "laps": live_stats["total_laps"] or 0,
            "time": live_stats["total_time_seconds"] or 0.0,
            "podiums": live_podiums,
            "full_houses": live_full_house,
        },
        "prior": prior,
    }

    # page link for each earlier season, relative to THIS rider's page
    this_path = hist.page_template.format(season=current_season, race_number=int(current_race_number))
    for p in prior:
        old_path = p["page_path"] or hist.page_template.format(season=p["season"], race_number=p["race_number"])
        p["href"] = posixpath.relpath(old_path, posixpath.dirname(this_path))
        # ...and the way back, relative to the OLD page (read by its Back link)
        p["back"] = posixpath.relpath(this_path, posixpath.dirname(old_path))
    return career


def _top_rung(value: float, ladder: List[Tuple[int, str]]) -> Optional[str]:
    label = None
    for threshold, text in ladder:
        if value >= threshold:
            label = text
    return label


def career_badges(career: Optional[Dict]) -> List[Tuple[str, str]]:
    """[(css_class, label), ...] -- highest rung of each ladder."""
    if not career:
        return []
    out: List[Tuple[str, str]] = [("founder", f"Riding since {career['first_season']}")]
    for value, ladder in ((career["seasons_ridden"], SEASONS_LADDER),
                          (career["rounds"], ROUNDS_LADDER),
                          (career["laps"], LAPS_LADDER),
                          (career["time"] / 3600.0, HOURS_LADDER),
                          (career["podiums"], PODIUM_LADDER)):
        label = _top_rung(value, ladder)
        if label:
            out.append(("career", label))
    n = career["full_houses"]
    if n >= 2:
        out.append(("career", f"{n} Full House seasons"))
    elif n == 1 and career["full_house_prior"]:
        # name the season, so it can't be mistaken for this season's own Full House badge
        out.append(("career", f"Full House in {career['full_house_prior'][0]}"))
    # n == 1 from THIS season alone: already shown as the season's Full House badge
    return out


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def render_career_block(career: Optional[Dict], current_season: str,
                        fmt_duration: Callable[[Optional[float]], str],
                        vurl: Callable[[str], str]) -> str:
    if not career:
        return ""

    def num(v) -> str:
        return f"{v:,}" if v else "—"

    t = career["this"]

    def with_ap(races, ap) -> str:
        return f'{num(races)} <span class="ap-extra">+&nbsp;{ap}&nbsp;AP</span>' if ap else num(races)

    rows = [
        ("Races completed", with_ap(t["races"], t["ap"]), with_ap(career["races"], career["ap_total"])),
        ("Laps completed", num(t["laps"]), num(career["laps"])),
        ("Time in the saddle", fmt_duration(t["time"]), fmt_duration(career["time"])),
        ("Podiums", num(t["podiums"]), num(career["podiums"])),
        ("Full Houses", num(t["full_houses"]), num(career["full_houses"])),
        ("Seasons ridden", "1", str(career["seasons_ridden"])),
    ]
    body = "".join(
        f'      <tr><td>{esc(label)}</td><td class="num">{a}</td><td class="num">{b}</td></tr>\n'
        for label, a, b in rows)

    # newest first
    items = ""
    for p in sorted(career["prior"], key=lambda r: r["season"], reverse=True):
        href = vurl(f'{p["href"]}?back={p["back"]}') if p.get("href") else ""
        pod = _podiums(p["golds"], p["silvers"], p["bronzes"])
        bits = [esc(p["race_category"] or ""),
                f'{p["races_completed"] or 0} races'
                + (f' + {p["ap_all"]} AP' if p.get("ap_all") else "")]
        if pod:
            bits.append(f"{pod} podium{'s' if pod != 1 else ''}")
        detail = " · ".join(b for b in bits if b)
        items += (f'      <li><a href="{esc(href)}">{esc(p["season"])} results</a> — {detail}</li>\n')

    return f"""  <div class="career">
    <h2>This season and career (since {esc(career['first_season'])})</h2>
    <table class="career-table">
      <thead><tr><th></th><th class="num">{esc(current_season)}</th><th class="num">Career</th></tr></thead>
      <tbody>
{body}      </tbody>
    </table>
    <p class="career-note">Career includes this season so far. AP = average-points round: counts towards round badges and Full House (if the round was held), but not as a race, laps or time.</p>
    <ul class="prev-seasons">
{items}    </ul>
  </div>
"""
