#!/usr/bin/env python3
"""
import_race_results.py
----------------------
Import a D3 RaceTec results CSV into a race SQLite database.

Core rules you asked for:
  1) Non-league riders:
       - Race No >= NONLEAGUE_THRESHOLD (default 900) are excluded completely.
  2) Re-ranking after filtering:
       - Overall position is re-ranked 1..N among LEAGUE riders.
       - Category position is re-ranked 1..K among LEAGUE riders within each category.
  3) Mixed gender races:
       - Some races have males + females in the same start but they are treated as
         effectively separate league races.
       - Use --split-genders to compute overall and category positions separately
         for M and F.
  4) Women's single-table leagues (U8/U10/U12/Youth):
       - Females race together as one league table (not per-category).
       - Use --women-single-table to set female cat_position == female overall_position
         and points based on female overall position.
       - Men still get per-category ranks.
  5) Points:
       - Default scheme: 1st gets MAX_POINTS (default 100), then -1 per place, min 1.
       - For normal men's-style: points based on category rank.
       - For women single-table: points based on female overall rank.
  6) Upserts results:
       - Inserts/updates per (rider_id, round).

Expected CSV columns (from your Seniors-results.csv sample):
  - "Pos"
  - "Race No"
  - "Time"
  - "Category"
  - "Gender"
  - (we ignore the rest)

DB assumptions:
  - riders table has: id, race_number, firstname, surname, ...
  - results table exists with at least:
        rider_id INTEGER,
        round INTEGER,
        cat_position INTEGER,
        overall_position INTEGER,
        points INTEGER,
        is_ap INTEGER,
        status TEXT,
        UNIQUE(rider_id, round)

Usage examples:
  # Men's-style (multi-category), not mixed genders:
  python3 import_race_results.py --db Seniors.db --round 1 --csv Senior-results.csv --dry-run

  # Mixed genders treated as separate leagues; women single-table (U8/U10/U12/Youth):
  python3 import_race_results.py --db U8.db --round 1 --csv U8-results.csv \
      --split-genders --women-single-table --dry-run

  # Actually write:
  python3 import_race_results.py --db U8.db --round 1 --csv U8-results.csv \
      --split-genders --women-single-table
"""

import argparse
import csv
import sqlite3
from pathlib import Path


# -----------------------------
# Helpers
# -----------------------------

def safe_int(x, default=None):
    if x is None:
        return default
    s = str(x).strip()
    if s == "":
        return default
    try:
        return int(s)
    except ValueError:
        return default


def parse_time_seconds(s):
    """
    Parse 'HH:MM:SS' or 'MM:SS' into seconds. Returns None if blank/unparseable.
    """
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


def normalise_gender(g):
    if not g:
        return "?"
    gg = str(g).strip().lower()
    if gg.startswith("f"):
        return "F"
    if gg.startswith("m"):
        return "M"
    return "?"


def normalise_category(cat):
    """
    Map D3 category strings into your base categories.
    Adjust if your CSV uses different text.

    Examples from your CSV:
      - 'Senior'   -> 'Sen'
      - 'Junior'   -> 'Jun'
      - 'U23'      -> 'U23'
      - 'M 40-49'  -> 'M40'
      - 'M 50-59'  -> 'M50'
      - 'M 60-69'  -> 'M60'
      - 'M 70+'    -> 'M70'
    """
    if cat is None:
        return ""
    s = str(cat).strip()
    if not s:
        return ""

    low = s.lower()

    if low == "senior":
        return "Sen"
    if low == "junior":
        return "Jun"
    if low in ("u23", "under 23"):
        return "U23"

    # masters strings like "M 40-49"
    if low.startswith("m"):
        # pull the first number we can find
        digits = "".join(ch if ch.isdigit() else " " for ch in s).split()
        if digits:
            age = safe_int(digits[0])
            if age in (40, 45):
                return "M40"
            if age == 50:
                return "M50"
            if age == 55:
                return "M50"
            if age == 60:
                return "M60"
            if age == 65:
                return "M60"
            if age >= 70:
                return "M60"

    return s


def compute_points(rank, max_points):
    """
    Points: max_points for 1st, then -1 per place, minimum 1.
    """
    if rank is None:
        return None
    pts = max_points - (rank - 1)
    return 1 if pts < 1 else pts


# -----------------------------
# CSV load / rerank
# -----------------------------

def load_csv_rows(csv_path: Path):
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames or []
    return rows, headers


def rerank(rows, *, nonleague_threshold, split_genders, women_single_table):
    """
    Filter non-league and recompute ranks.

    If split_genders:
      - compute ranks separately for M and F groups
      - female group can optionally be treated as single-table (cat_rank = overall_rank)
    Else:
      - compute one set of ranks over all riders (league-only)

    Returns:
      processed: list[dict] with keys:
        race_no, base_cat, gender, csv_pos, time_sec, overall_rank, cat_rank
      stats: dict
    """
    extracted = []
    stats = {
        "input_rows": 0,
        "kept_league": 0,
        "filtered_nonleague": 0,
        "skipped_missing_raceno": 0,
        "skipped_missing_ordering": 0,
        "unknown_gender": 0,
    }

    for r in rows:
        stats["input_rows"] += 1

        race_no = safe_int(r.get("Race No"))
        if race_no is None:
            stats["skipped_missing_raceno"] += 1
            continue

        if race_no >= nonleague_threshold:
            stats["filtered_nonleague"] += 1
            continue

        csv_pos = safe_int(r.get("Pos"))
        time_sec = parse_time_seconds(r.get("Time"))
        if csv_pos is None and time_sec is None:
            stats["skipped_missing_ordering"] += 1
            continue

        gender = normalise_gender(r.get("Gender"))
        if gender == "?":
            stats["unknown_gender"] += 1

        base_cat = normalise_category(r.get("Category"))

        extracted.append({
            "race_no": race_no,
            "base_cat": base_cat,
            "gender": gender,
            "csv_pos": csv_pos,
            "time_sec": time_sec,
        })

    # stable finish-order key:
    # - Prefer Pos when present (many exports have it)
    # - Otherwise fall back to time
    def sort_key(x):
        return (
            x["csv_pos"] is None,  # Pos present first
            x["csv_pos"] if x["csv_pos"] is not None else 10**9,
            x["time_sec"] is None,
            x["time_sec"] if x["time_sec"] is not None else 10**12,
            x["race_no"]
        )

    if not split_genders:
        extracted.sort(key=sort_key)

        # overall ranks after filtering
        for i, item in enumerate(extracted, start=1):
            item["overall_rank"] = i

        # per-category rank (finish order)
        counters = {}
        for item in extracted:
            c = item["base_cat"]
            counters[c] = counters.get(c, 0) + 1
            item["cat_rank"] = counters[c]

        stats["kept_league"] = len(extracted)
        return extracted, stats

    # split genders: rank separately for M and F (and ? if present)
    processed = []
    for g in ("M", "F", "?"):
        group = [x for x in extracted if x["gender"] == g]
        if not group:
            continue

        group.sort(key=sort_key)

        # overall rank within gender
        for i, item in enumerate(group, start=1):
            item["overall_rank"] = i

        # cat rank rules
        if g == "F" and women_single_table:
            for item in group:
                item["cat_rank"] = item["overall_rank"]
        else:
            counters = {}
            for item in group:
                c = item["base_cat"]
                counters[c] = counters.get(c, 0) + 1
                item["cat_rank"] = counters[c]

        processed.extend(group)

    # Sort for readable sample output: by gender then rank
    processed.sort(key=lambda x: (x["gender"], x["overall_rank"]))
    stats["kept_league"] = len(processed)
    return processed, stats


# -----------------------------
# DB import
# -----------------------------

def import_results(db_path: Path, round_no: int, processed, *,
                   women_single_table: bool,
                   max_points: int,
                   dry_run: bool):
    """
    Upsert results into DB.
    overall_position and cat_position are taken from the recomputed ranks.
    points:
      - if women_single_table and rider is F -> based on overall_rank
      - else -> based on cat_rank
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # sanity checks
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='riders'")
    if not cur.fetchone():
        conn.close()
        raise SystemExit("❌ DB missing 'riders' table")

    cur.execute("PRAGMA table_info(riders)")
    rider_cols = {row[1] for row in cur.fetchall()}
    if "race_number" not in rider_cols:
        conn.close()
        raise SystemExit("❌ DB schema issue: riders table missing 'race_number'")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results'")
    if not cur.fetchone():
        conn.close()
        raise SystemExit("❌ DB missing 'results' table")

    upserted = 0
    not_found = 0

    for item in processed:
        race_no = item["race_no"]
        gender = item["gender"]

        cur.execute("SELECT id FROM riders WHERE race_number = ?", (race_no,))
        rr = cur.fetchone()
        if not rr:
            not_found += 1
            continue
        rider_id = rr[0]

        overall_position = item["overall_rank"]
        cat_position = item["cat_rank"]

        if women_single_table and gender == "F":
            pts_rank = overall_position
        else:
            pts_rank = cat_position

        points = compute_points(pts_rank, max_points)

        if not dry_run:
            cur.execute(
                """
                INSERT INTO results (rider_id, round, cat_position, overall_position, points, is_ap, status)
                VALUES (?, ?, ?, ?, ?, 0, 'FIN')
                ON CONFLICT(rider_id, round) DO UPDATE SET
                    cat_position     = excluded.cat_position,
                    overall_position = excluded.overall_position,
                    points           = excluded.points,
                    is_ap            = excluded.is_ap,
                    status           = excluded.status
                """,
                (rider_id, round_no, cat_position, overall_position, points)
            )

        upserted += 1

    if dry_run:
        conn.rollback()
    else:
        conn.commit()

    conn.close()

    return {
        "results_upserted": upserted,
        "riders_not_found": not_found,
    }


# -----------------------------
# CLI
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Import race results CSV with league filtering + reranking.")
    ap.add_argument("--db", required=True, help="SQLite race DB (e.g. Seniors.db)")
    ap.add_argument("--round", type=int, required=True, help="Round number (1..12)")
    ap.add_argument("--csv", required=True, help="Results CSV (D3 RaceTec export)")
    ap.add_argument("--nonleague-threshold", type=int, default=900,
                    help="Race No >= this is non-league (default 900)")
    ap.add_argument("--split-genders", action="store_true",
                    help="Treat M and F as separate league tables (rerank separately).")
    ap.add_argument("--women-single-table", action="store_true",
                    help="For female riders, use single table: cat_position == overall_position.")
    ap.add_argument("--max-points", type=int, default=100,
                    help="Points for 1st place (default 100)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Do everything except write to DB")
    args = ap.parse_args()

    db_path = Path(args.db)
    csv_path = Path(args.csv)

    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")
    if not csv_path.exists():
        raise SystemExit(f"❌ CSV not found: {csv_path}")

    rows, headers = load_csv_rows(csv_path)

    required_cols = {"Race No", "Category", "Pos", "Gender"}
    missing = required_cols - set(headers)
    if missing:
        raise SystemExit(
            f"❌ CSV missing expected column(s): {sorted(missing)}\n"
            f"Headers were: {headers}"
        )

    processed, stats = rerank(
        rows,
        nonleague_threshold=args.nonleague_threshold,
        split_genders=args.split_genders,
        women_single_table=args.women_single_table
    )

    print("\nCSV processing summary")
    print(f"  Input rows                           : {stats['input_rows']}")
    print(f"  Filtered non-league (>= {args.nonleague_threshold})       : {stats['filtered_nonleague']}")
    print(f"  Skipped missing Race No               : {stats['skipped_missing_raceno']}")
    print(f"  Skipped missing Pos/Time              : {stats['skipped_missing_ordering']}")
    print(f"  Unknown/blank gender rows             : {stats['unknown_gender']}")
    print(f"  League rows kept                      : {stats['kept_league']}")
    print(f"  Split genders                         : {args.split_genders}")
    print(f"  Women single table (female only)      : {args.women_single_table}")
    print(f"  Points scheme                         : {args.max_points} down to 1")

    db_stats = import_results(
        db_path,
        args.round,
        processed,
        women_single_table=args.women_single_table,
        max_points=args.max_points,
        dry_run=args.dry_run
    )

    print("\nDB import summary")
    print(f"  Results upserted                      : {db_stats['results_upserted']}")
    print(f"  Riders not found in DB (race_number)  : {db_stats['riders_not_found']}")
    print(f"  Dry-run                               : {args.dry_run}")

    print("\nSample (first 12 after rerank):")
    for item in processed[:12]:
        print(
            f"  {item['gender']} "
            f"#{item['overall_rank']:>3}  "
            f"race_no={item['race_no']:<4}  "
            f"cat={item['base_cat']:<6}  "
            f"cat_pos={item['cat_rank']:<3}  "
            f"csv_pos={item['csv_pos']}"
        )

    print("\nDone.")


if __name__ == "__main__":
    main()

