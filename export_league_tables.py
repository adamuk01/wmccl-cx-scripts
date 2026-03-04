#!/usr/bin/env python3
"""
export_league_tables.py
-----------------------
Export league tables as separate CSV files (one per "table") from a single SQLite DB.

Scoring:
  - Best N (default 10) from up to R rounds (default 12).
  - AP rounds are stored as points=999 and/or is_ap=1 in results.
  - When exporting, AP points are replaced with rider's CURRENT season average
    (average of non-AP points in this DB up to --upto-round).

Output:
  - One CSV per table into: <outdir>/league_tables/
  - Each CSV includes (NEW ORDER):
      Position, firstname surname, race_number, race_category_current_year, club_name,
      IBX, best_X_points, average_points, r1_points..r12_points, ap_rounds

Notes:
  - ap_rounds is a single field listing rounds that were AP, e.g. "2,7,11".
  - This script does NOT modify the database.

Profiles (match your 7-db-per-race setup):
  - u8     : U6M,U6F,U8M,U8F
  - u10    : U10M,U10F
  - u12    : U12M,U12F
  - youth  : U14M,U14F,U16M,U16F
  - women  : Women_All (everything in that DB)
  - seniors: JunM; SenM+U23M; M40M; M50M; M60M+M70M
  - masters: M40M; M50M; M60M+M70M  (if you have a masters-only DB)

Usage examples:
  python3 export_league_tables.py --db U8.db     --profile u8
  python3 export_league_tables.py --db Youth.db  --profile youth
  python3 export_league_tables.py --db Women.db  --profile women
  python3 export_league_tables.py --db Seniors.db --profile seniors
"""

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Optional


AP_MARKER = 999


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


def safe_float(x, default=None):
    if x is None:
        return default
    s = str(x).strip()
    if s == "":
        return default
    try:
        return float(s)
    except ValueError:
        return default


def compute_avg(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / len(vals)


def best_n_sum(vals: List[float], n: int) -> float:
    vals_sorted = sorted(vals, reverse=True)
    return float(sum(vals_sorted[:n]))


def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='riders'")
    if not cur.fetchone():
        raise SystemExit("❌ DB missing 'riders' table")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='results'")
    if not cur.fetchone():
        raise SystemExit("❌ DB missing 'results' table")

    cur.execute("PRAGMA table_info(riders)")
    rider_cols = {row[1] for row in cur.fetchall()}
    needed_riders = {
        "id",
        "race_number",
        "firstname",
        "surname",
        "club_name",
        "race_category",
        "IBX",
    }
    missing = needed_riders - rider_cols
    if missing:
        raise SystemExit(f"❌ riders table missing columns: {sorted(missing)}")

    cur.execute("PRAGMA table_info(results)")
    res_cols = {row[1] for row in cur.fetchall()}
    needed_res = {"rider_id", "round", "points", "is_ap"}
    missing = needed_res - res_cols
    if missing:
        raise SystemExit(f"❌ results table missing columns: {sorted(missing)}")


def load_riders(conn: sqlite3.Connection) -> List[Tuple]:
    cur = conn.cursor()
    # NOTE: column name starts with digit => must quote it
    cur.execute("""
        SELECT id, race_number, firstname, surname, club_name, race_category, IBX
        FROM riders
        ORDER BY race_number
    """)
    return cur.fetchall()


def load_results(conn: sqlite3.Connection, rounds: int) -> Dict[Tuple[int, int], Tuple[Optional[float], int]]:
    """
    results[(rider_id, round)] = (points, is_ap)
    """
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


def rider_avg_this_season(rider_id: int,
                          results: Dict[Tuple[int, int], Tuple[Optional[float], int]],
                          upto_round: int) -> Optional[float]:
    vals = []
    for rnd in range(1, upto_round + 1):
        pts, is_ap = results.get((rider_id, rnd), (None, 0))
        if pts is None:
            continue
        if is_ap or pts == AP_MARKER:
            continue
        vals.append(float(pts))
    return compute_avg(vals)


def effective_points_for_round(rider_id: int,
                               rnd: int,
                               results: Dict[Tuple[int, int], Tuple[Optional[float], int]],
                               avg_pts: Optional[float]) -> Tuple[Optional[float], int]:
    """
    returns (effective_points, ap_flag)
    """
    pts, is_ap = results.get((rider_id, rnd), (None, 0))
    ap_flag = 1 if (is_ap or pts == AP_MARKER) else 0
    if ap_flag:
        return (avg_pts, 1)
    return (pts, 0)


def profile_tables(profile: str) -> Dict[str, List[str]]:
    """
    table_name -> list of race_category values to include (exact match).
    Special case: ["*"] means include all riders in DB.
    """
    if profile == "women":
        return {"Women_All": ["*"]}

    if profile == "u8":
        return {
            "U6M": ["U6M"],
            "U6F": ["U6F"],
            "U8M": ["U8M"],
            "U8F": ["U8F"],
        }

    if profile == "u10":
        return {
            "U10M": ["U10M"],
            "U10F": ["U10F"],
        }

    if profile == "u12":
        return {
            "U12M": ["U12M"],
            "U12F": ["U12F"],
        }

    if profile == "youth":
        return {
            "U14M": ["U14M"],
            "U14F": ["U14F"],
            "U16M": ["U16M"],
            "U16F": ["U16F"],
        }

    if profile == "seniors":
        return {
            "JunM": ["JunM"],
            "Sen_U23_M": ["SenM", "U23M"],

            # Masters groupings as requested
            "M40M": ["M40M", "M45M"],
            "M50M": ["M50M", "M55M"],
            "M60M": ["M60M", "M65M"],
            "M70M": ["M70M"],
        }

    if profile == "masters":
        return {
            # Masters groupings as requested
            "M40M": ["M40M", "M45M"],
            "M50M": ["M50M", "M55M"],
            "M60M": ["M60M", "M65M"],
            "M70M": ["M70M"],
        }

    raise SystemExit(f"❌ Unknown profile: {profile}")


def export_table(csv_path: Path,
                 riders_subset: List[Tuple],
                 results: Dict[Tuple[int, int], Tuple[Optional[float], int]],
                 rounds: int,
                 best_n: int,
                 upto_round: int,
                 avg_decimals: int = 0,
                 ap_prefix: str = ""):
    """
    riders_subset rows: (id, race_number, firstname, surname, club_name, race_category, one_bx)
    ap_prefix: ""  -> "2,7"
               "R" -> "R2,R7"
    """
    # precompute averages
    avgs = {rider_id: rider_avg_this_season(rider_id, results, upto_round)
            for (rider_id, *_rest) in riders_subset}

    headers = [
        "Position",
        "firstname surname",
        "race_number",
        "race_category_current_year",
        "club_name",
        "IBX",
        f"best_{best_n}_points",
        "average_points",
    ]
    for rnd in range(1, rounds + 1):
        headers.append(f"r{rnd}_points")
    headers.append("ap_rounds")

    rows_out = []

    for rider_id, race_no, first, last, club, cat, one_bx in riders_subset:
        avg = avgs.get(rider_id)
        avg_disp = "" if avg is None else round(avg, avg_decimals)

        eff_points_for_totals: List[float] = []
        total_points = 0.0
        per_round_points = []
        ap_rounds_list: List[str] = []

        for rnd in range(1, rounds + 1):
            eff, ap_flag = effective_points_for_round(rider_id, rnd, results, avg)
            if ap_flag:
                ap_rounds_list.append(f"{ap_prefix}{rnd}")

            if eff is None:
                per_round_points.append(0)
                continue

            eff_f = float(eff)
            eff_points_for_totals.append(eff_f)
            total_points += eff_f

            # display point
            if ap_flag:
                # AP replacement: show average with decimals
                per_round_points.append(round(eff_f, avg_decimals))
            else:
                # Real race result: always whole number
                per_round_points.append(int(round(eff_f, 0)))

        best_total = best_n_sum(eff_points_for_totals, best_n) if eff_points_for_totals else 0.0
        ap_rounds_str = ",".join(ap_rounds_list)

        full_name = f"{first} {last}".strip()
        ibx = (one_bx or "").strip()  # expected "Y" or "N"

        row = [
            None,  # Position placeholder (filled after sorting)
            full_name,
            race_no,
            cat,
            club,
            ibx,
            int(round(best_total, 0)),
            avg_disp,
            *per_round_points,
            ap_rounds_str
        ]

        # SAFETY CHECK — ensures headers and row stay aligned
        if len(row) != len(headers):
            raise RuntimeError(
                f"Row/headers mismatch for rider {race_no}: "
                f"{len(row)} values vs {len(headers)} headers"
            )

        # Sort: bestN desc, then total desc, then surname/firstname
        rows_out.append((best_total, total_points, last.lower(), first.lower(), row))

    rows_out.sort(key=lambda x: (-x[0], -x[1], x[2], x[3]))

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for pos, (*_keys, row) in enumerate(rows_out, start=1):
            row[0] = pos
            w.writerow(row)


def main():
    ap = argparse.ArgumentParser(description="Export league table CSVs into league_tables/")
    ap.add_argument("--db", required=True, help="SQLite DB (e.g. U8.db, Youth.db, Women.db, Seniors.db)")
    ap.add_argument("--profile", required=True,
                    choices=["u8", "u10", "u12", "youth", "women", "seniors", "masters"],
                    help="Grouping rules matching your DB/race")
    ap.add_argument("--outdir", default=".", help="Base output directory (default: .)")
    ap.add_argument("--rounds", type=int, default=12, help="Number of rounds columns to export (default: 12)")
    ap.add_argument("--best", type=int, default=10, help="Best N results (default: 10)")
    ap.add_argument("--upto-round", type=int, default=None,
                    help="Compute averages using results up to this round (inclusive). "
                         "Default: max round present in DB (capped by --rounds).")
    ap.add_argument("--avg-decimals", type=int, default=0, help="Decimals for displayed points (default 0)")
    ap.add_argument("--ap-prefix", default="", choices=["", "R"],
                    help="Prefix for AP rounds list: '' => '2,7' ; 'R' => 'R2,R7'")
    ap.add_argument("--skip-empty", action="store_true", default=True,
                    help="Skip writing CSVs for tables with 0 riders (default: on).")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)

    # decide upto_round
    cur = conn.cursor()
    cur.execute("SELECT MAX(round) FROM results")
    max_round = cur.fetchone()[0] or 0
    upto_round = args.upto_round if args.upto_round is not None else max_round
    if upto_round > args.rounds:
        upto_round = args.rounds

    riders = load_riders(conn)
    results = load_results(conn, args.rounds)

    tables = profile_tables(args.profile)

    out_base = Path(args.outdir) / "league_tables"
    out_base.mkdir(parents=True, exist_ok=True)

    print(f"\nDB: {db_path.name}")
    print(f"Profile: {args.profile}")
    print(f"Rounds exported: 1..{args.rounds}")
    print(f"Averages computed up to round: {upto_round} (max in DB was {max_round})")
    print(f"Best {args.best} scoring")
    print(f"Output dir: {out_base}\n")

    for table_name, cat_list in tables.items():
        if cat_list == ["*"]:
            subset = riders
        else:
            cat_set = set(cat_list)
            subset = [r for r in riders if (r[5] or "") in cat_set]  # r[5] = race_category

        if args.skip_empty and not subset:
            print(f"  Skipped {table_name}.csv  (0 riders)")
            continue

        out_csv = out_base / f"{table_name}.csv"
        export_table(
            out_csv,
            subset,
            results,
            args.rounds,
            args.best,
            upto_round,
            avg_decimals=args.avg_decimals,
            ap_prefix=args.ap_prefix
        )
        print(f"  Wrote {out_csv.name}  ({len(subset)} riders)")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

