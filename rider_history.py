#!/usr/bin/env python3
"""
rider_history.py
----------------
Shared library for the central WMCCL rider-history database
(wmccl_history.db). No command-line interface -- see archive_season.py.

The history DB holds:

  person        one row per real human, keyed by a permanent person_id that
                never changes (unlike race numbers, which change every year,
                and riders.id, which is per-DB and reset every season).
  season_stats  one row per rider-entry per season: the season's totals,
                race number, category and the BC number AS ENTERED that year.
  schema_info   schema version.

Career totals are always computed with SUM() over season_stats -- never
stored as a running counter -- so a corrected old season fixes the career
figure automatically.

IDENTITY MATCHING (see match_rider)
-----------------------------------
DOB is the anchor. BC number is supporting evidence only, because it is
typed by riders/parents and is sometimes wrong or missing.

  DOB + first + surname match            -> auto (BC conflict flagged)
  DOB + BC match (name differs)          -> auto (name change / nickname)
  DOB matches, names differ, both BC
    present and different                -> NEW person (twins / siblings)
  DOB matches, first names and sex differ -> NEW person (mixed-sex twins)
  DOB matches, names differ, a BC blank  -> REVIEW
  BC matches but DOB differs             -> REVIEW, never auto
  Same name + same birth YEAR, DOB
    differs                              -> REVIEW (likely DOB typo / d-m swap)
  Anything else                          -> NEW person

Nothing ambiguous is ever guessed: it goes on the review report and is held
out of the database until resolved in the overrides file.
"""

import sqlite3
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

SCHEMA_VERSION = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS person (
    person_id    INTEGER PRIMARY KEY,
    firstname    TEXT,
    surname      TEXT,
    DOB          TEXT,      -- ISO YYYY-MM-DD (normalised); ADMIN ONLY, never publish
    gender       TEXT,
    BC_number    INTEGER,   -- latest non-blank BC number seen (supporting evidence only)
    first_season TEXT
);
CREATE INDEX IF NOT EXISTS idx_person_dob ON person(DOB);
CREATE INDEX IF NOT EXISTS idx_person_bc  ON person(BC_number);

CREATE TABLE IF NOT EXISTS season_stats (
    id                          INTEGER PRIMARY KEY,
    person_id                   INTEGER NOT NULL,
    season                      TEXT NOT NULL,     -- e.g. '2025-26'
    source_db                   TEXT NOT NULL,     -- e.g. 'Masters.db'
    race_number                 INTEGER NOT NULL,
    race_category               TEXT,
    race_category_previous_year TEXT,
    BC_number_entered           INTEGER,           -- as entered that season
    rounds_in_season            INTEGER,           -- rounds that existed for this DB
    races_completed             INTEGER,           -- real finishes (no AP, no DNF)
    laps                        INTEGER,           -- includes laps on a DNF
    time_seconds                REAL,              -- real finishes only
    avg_points                  REAL,              -- real points only (no AP)
    best_points                 REAL,
    best_cat_position           INTEGER,
    golds                       INTEGER,
    silvers                     INTEGER,
    bronzes                     INTEGER,
    full_house                  INTEGER,           -- 1 = rode every round expected of this category
    ap_rounds                   INTEGER,           -- rounds credited as average points (AP)
    ap_value                    REAL,              -- the AP value: rider's average of real points that season
    page_path                   TEXT,              -- rider page, relative to site root
    UNIQUE(season, source_db, race_number),
    FOREIGN KEY(person_id) REFERENCES person(person_id)
);
CREATE INDEX IF NOT EXISTS idx_stats_person ON season_stats(person_id, season);

-- Every round a rider appeared in, INCLUDING rounds left out of career totals
-- (counts_for_career = 0), so e.g. Regional Champs average points still show.
CREATE TABLE IF NOT EXISTS round_results (
    season           TEXT NOT NULL,
    source_db        TEXT NOT NULL,
    race_number      INTEGER NOT NULL,
    round            INTEGER NOT NULL,
    status           TEXT,
    is_ap            INTEGER,
    points           REAL,             -- 999 marker for AP rows, as in the race DBs
    effective_points REAL,             -- points the league counted: real points, or the AP value
    cat_position     INTEGER,
    overall_position INTEGER,
    laps             INTEGER,
    time_seconds     REAL,
    counts_for_career INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (season, source_db, race_number, round)
);

-- Round names/venues/dates plus whether the round counts towards career totals.
CREATE TABLE IF NOT EXISTS season_rounds (
    season            TEXT NOT NULL,
    round             INTEGER NOT NULL,
    name              TEXT,
    venue             TEXT,
    date              TEXT,
    counts_for_career INTEGER NOT NULL DEFAULT 1,
    note              TEXT,
    PRIMARY KEY (season, round)
);
"""


_ADDED_COLUMNS = [
    ("season_stats", "ap_rounds", "INTEGER"),
    ("season_stats", "ap_value", "REAL"),
    ("round_results", "effective_points", "REAL"),
]


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    # upgrade older history DBs in place
    for table, col, typ in _ADDED_COLUMNS:
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if col not in have:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
    conn.execute(
        "INSERT OR REPLACE INTO schema_info(key, value) VALUES ('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return " ".join(str(s).strip().split())


def norm_name(s: Optional[str]) -> str:
    """Lower-cased, whitespace-collapsed, stray edge punctuation removed ('Wright.' == 'Wright')."""
    return norm(s).lower().strip(" .,;:")


def norm_sex(s: Optional[str]) -> str:
    """'M' / 'F' / '' from 'Male', 'Female', 'M', 'F'."""
    s = norm(s).lower()
    return "F" if s.startswith("f") else "M" if s.startswith("m") else ""


def safe_int(x) -> Optional[int]:
    s = norm(x)
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def norm_dob(dob: Optional[str]) -> str:
    """
    Normalise DOB to YYYY-MM-DD ('' if blank/unparseable).

    Same century rule as validate_and_allocate_entrants.py: strptime('%y')
    maps 00-68 to the 2000s, so any date later than today is pulled back
    100 years (older riders with two-digit years).
    """
    d = norm(dob)
    if not d:
        return ""
    for f in ("%m/%d/%y", "%m/%d/%Y", "%d-%b-%y", "%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(d, f)
        except ValueError:
            continue
        if parsed.date() > date.today():
            try:
                parsed = parsed.replace(year=parsed.year - 100)
            except ValueError:
                continue
        return parsed.strftime("%Y-%m-%d")
    return ""


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

# Outcome kinds
AUTO = "auto"
NEW = "new"
REVIEW = "review"


class Match:
    """Result of match_rider()."""

    def __init__(self, kind: str, person_id: Optional[int] = None,
                 reason: str = "", flags: Optional[List[str]] = None,
                 candidates: Optional[List[int]] = None):
        self.kind = kind
        self.person_id = person_id
        self.reason = reason
        self.flags = flags or []
        self.candidates = candidates or []


def _bc_conflict(a: Optional[int], b: Optional[int]) -> bool:
    return a is not None and b is not None and a != b


def match_rider(conn: sqlite3.Connection, firstname: str, surname: str,
                dob_iso: str, bc: Optional[int], gender: Optional[str] = None) -> Match:
    """Find the existing person for this rider entry, or decide it is new/review."""
    fn, sn = norm_name(firstname), norm_name(surname)

    rows = conn.execute(
        "SELECT person_id, firstname, surname, DOB, BC_number, gender FROM person"
    ).fetchall()
    people = [(pid, norm_name(f), norm_name(s), d or "", b, norm_sex(g)) for pid, f, s, d, b, g in rows]

    same_dob = [p for p in people if dob_iso and p[3] == dob_iso]

    # 1. DOB + full name
    exact = [p for p in same_dob if p[1] == fn and p[2] == sn]
    if len(exact) == 1:
        flags = []
        if _bc_conflict(bc, exact[0][4]):
            flags.append(f"BC conflict: entered {bc}, on file {exact[0][4]}")
        return Match(AUTO, exact[0][0], "DOB + name", flags)
    if len(exact) > 1:
        return Match(REVIEW, reason="several people share this name + DOB",
                     candidates=[p[0] for p in exact])

    # 2. DOB + BC (name differs: name change, nickname, spelling)
    if bc is not None:
        dob_bc = [p for p in same_dob if p[4] == bc]
        if len(dob_bc) == 1:
            return Match(AUTO, dob_bc[0][0], "DOB + BC (name differs)")

    # 3. BC matches but DOB differs -> never auto
    if bc is not None:
        bc_only = [p for p in people if p[4] == bc and p not in same_dob]
        if bc_only:
            return Match(REVIEW, reason=f"BC {bc} matches but DOB differs",
                         candidates=[p[0] for p in bc_only])

    # 4. DOB matches, names differ
    if same_dob:
        sx = norm_sex(gender)
        if sx and all(p[5] and p[5] != sx and p[1] != fn for p in same_dob):
            return Match(NEW, reason="same DOB, different first name and sex (twins)")
        if bc is not None and all(_bc_conflict(bc, p[4]) for p in same_dob):
            # different BC numbers on both sides: twins / siblings -> new person
            return Match(NEW, reason="same DOB, different name and BC (twins/siblings)")
        return Match(REVIEW, reason="same DOB but different name and no BC to confirm",
                     candidates=[p[0] for p in same_dob])

    # 5. Same name, same birth year, different day/month -> probable DOB typo
    if dob_iso:
        year = dob_iso[:4]
        near = [p for p in people if p[1] == fn and p[2] == sn and p[3][:4] == year]
        if near:
            return Match(REVIEW, reason="same name and birth year, DOB differs (typo / d-m swap?)",
                         candidates=[p[0] for p in near])

    # 6. Same name, no usable DOB on either side
    if not dob_iso:
        blank = [p for p in people if p[1] == fn and p[2] == sn]
        if blank:
            return Match(REVIEW, reason="rider has no usable DOB; same name exists",
                         candidates=[p[0] for p in blank])
        return Match(REVIEW, reason="rider has no usable DOB")

    return Match(NEW, reason="no match")
