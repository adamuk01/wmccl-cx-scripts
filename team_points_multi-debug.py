#!/usr/bin/env python3
import sqlite3
import argparse
import csv
import sys
from typing import List, Tuple


def _print_debug(cur, union_sql, union_name_sql, exclude_club_clause,
                 exclude_status_clause, top_n, filter_params):
    """
    Print per-round, per-club detail to stderr showing every rider,
    their points, and whether they were picked (COUNTED) or left out (--).
    """
    sql = f"""
    WITH AllRows AS (
        {union_name_sql}
    ),
    Filtered AS (
        SELECT club_name, round_number, rider_name, race_number, is_ap, points, status
        FROM AllRows
        WHERE 1=1
          {exclude_club_clause}
          {exclude_status_clause}
    ),
    Ranked AS (
        SELECT
            club_name,
            round_number,
            rider_name,
            race_number,
            is_ap,
            points,
            ROW_NUMBER() OVER (
                PARTITION BY club_name, round_number
                ORDER BY points DESC
            ) AS rn
        FROM Filtered
    )
    SELECT
        round_number,
        club_name,
        rider_name,
        race_number,
        is_ap,
        points,
        rn,
        CASE WHEN rn <= ? THEN 'COUNTED' ELSE '--' END AS picked
    FROM Ranked
    ORDER BY round_number ASC, club_name ASC, rn ASC;
    """
    cur.execute(sql, filter_params + [top_n])
    rows = cur.fetchall()

    current_round = None
    current_club = None

    print("\n=== DEBUG: Per-round rider selection ===", file=sys.stderr)
    for r in rows:
        rnd       = r["round_number"]
        club      = r["club_name"]
        name      = r["rider_name"] or "Unknown"
        race_no   = r["race_number"] or "?"
        is_ap     = r["is_ap"]
        pts       = r["points"]
        picked    = r["picked"]

        if rnd != current_round:
            current_round = rnd
            current_club = None
            print(f"\n--- Round {rnd} ---", file=sys.stderr)

        if club != current_club:
            current_club = club
            print(f"  {club}:", file=sys.stderr)

        ap_tag = " [AP]" if is_ap else ""
        print(f"    {picked}  #{race_no:<4} {name:<30} {pts:>7.1f}{ap_tag}",
              file=sys.stderr)

    print("\n=== END DEBUG ===\n", file=sys.stderr)


def compute_team_points_multi(
    db_paths: List[str],
    top_n: int = 6,
    exclude_clubs: List[str] | None = None,
    exclude_statuses: List[str] | None = None,
    per_round: bool = False,
    debug: bool = False,
) -> List[Tuple]:
    """
    Combine multiple category DBs (same schema) and compute:
      - per club, per round: top_n points across ALL included DBs
      - overall: sum across rounds
    Optionally return per-round totals.

    Average points (AP) rounds ARE included. The 999 sentinel value stored for
    AP results is replaced with the rider's real average (mean of their non-AP
    FIN results in that DB) so they score a representative amount rather than
    inflating or being excluded from the team totals.

    Expected schema in each DB:
      riders(id, club_name, ...)
      results(rider_id, round, points, is_ap, status, ...)
    """

    exclude_clubs = [c.strip() for c in (exclude_clubs or [])]
    exclude_statuses = exclude_statuses or ["DNS"]

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Attach DBs
    aliases = []
    for i, path in enumerate(db_paths):
        alias = f"db{i}"
        aliases.append(alias)
        cur.execute(f"ATTACH DATABASE ? AS {alias};", (path,))

    # Build UNION of all result rows across attached DBs.
    # For AP rows (is_ap=1 or points=999), substitute the rider's real average
    # (mean of their non-AP FIN results in the same DB) so they score correctly
    # rather than carrying the 999 sentinel value.
    union_parts = []
    for a in aliases:
        union_parts.append(f"""
            SELECT
                TRIM(r.club_name) AS club_name,
                res.round AS round_number,
                CASE
                    WHEN COALESCE(res.is_ap, 0) = 1 OR res.points = 999 THEN
                        COALESCE((
                            SELECT AVG(r2.points)
                            FROM {a}.results r2
                            WHERE r2.rider_id = res.rider_id
                              AND COALESCE(r2.is_ap, 0) = 0
                              AND r2.points IS NOT NULL
                              AND r2.points != 999
                              AND r2.status = 'FIN'
                        ), 0)
                    ELSE COALESCE(res.points, 0)
                END AS points,
                COALESCE(res.is_ap, 0) AS is_ap,
                COALESCE(res.status, 'FIN') AS status
            FROM {a}.results res
            JOIN {a}.riders r ON r.id = res.rider_id
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    # Debug variant: same query but also pulls rider name and ap flag for display
    union_name_parts = []
    for a in aliases:
        union_name_parts.append(f"""
            SELECT
                TRIM(r.club_name) AS club_name,
                res.round AS round_number,
                r.firstname || ' ' || r.surname AS rider_name,
                r.race_number AS race_number,
                COALESCE(res.is_ap, 0) AS is_ap,
                COALESCE(res.status, 'FIN') AS status,
                CASE
                    WHEN COALESCE(res.is_ap, 0) = 1 OR res.points = 999 THEN
                        COALESCE((
                            SELECT AVG(r2.points)
                            FROM {a}.results r2
                            WHERE r2.rider_id = res.rider_id
                              AND COALESCE(r2.is_ap, 0) = 0
                              AND r2.points IS NOT NULL
                              AND r2.points != 999
                              AND r2.status = 'FIN'
                        ), 0)
                    ELSE COALESCE(res.points, 0)
                END AS points
            FROM {a}.results res
            JOIN {a}.riders r ON r.id = res.rider_id
        """)
    union_name_sql = "\nUNION ALL\n".join(union_name_parts)

    # Filters applied ONCE after union (so placeholders aren't repeated per DB)
    params: List[object] = []

    exclude_club_clause = ""
    if exclude_clubs:
        exclude_club_clause = f"AND club_name NOT IN ({','.join(['?'] * len(exclude_clubs))})"
        params.extend(exclude_clubs)

    exclude_status_clause = ""
    if exclude_statuses:
        exclude_status_clause = f"AND status NOT IN ({','.join(['?'] * len(exclude_statuses))})"
        params.extend(exclude_statuses)

    if per_round:
        sql = f"""
        WITH AllRows AS (
            {union_sql}
        ),
        Filtered AS (
            SELECT club_name, round_number, points
            FROM AllRows
            WHERE 1=1
              {exclude_club_clause}
              {exclude_status_clause}
        ),
        Ranked AS (
            SELECT
                club_name,
                round_number,
                points,
                ROW_NUMBER() OVER (
                    PARTITION BY club_name, round_number
                    ORDER BY points DESC
                ) AS rn
            FROM Filtered
        )
        SELECT
            round_number,
            club_name,
            SUM(points) AS topn_points
        FROM Ranked
        WHERE rn <= ?
        GROUP BY round_number, club_name
        HAVING SUM(points) > 0
        ORDER BY round_number ASC, topn_points DESC, club_name ASC;
        """
        params.append(top_n)
        cur.execute(sql, params)
        rows = cur.fetchall()

        if debug:
            _print_debug(cur, union_sql, union_name_sql, exclude_club_clause,
                         exclude_status_clause, top_n, list(params[:-1]))

        return [(r["round_number"], r["club_name"], round(r["topn_points"])) for r in rows]

    else:
        sql = f"""
        WITH AllRows AS (
            {union_sql}
        ),
        Filtered AS (
            SELECT club_name, round_number, points
            FROM AllRows
            WHERE 1=1
              {exclude_club_clause}
              {exclude_status_clause}
        ),
        Ranked AS (
            SELECT
                club_name,
                round_number,
                points,
                ROW_NUMBER() OVER (
                    PARTITION BY club_name, round_number
                    ORDER BY points DESC
                ) AS rn
            FROM Filtered
        )
        SELECT
            club_name,
            SUM(points) AS total_points
        FROM Ranked
        WHERE rn <= ?
        GROUP BY club_name
        HAVING SUM(points) > 0
        ORDER BY total_points DESC, club_name ASC;
        """
        params.append(top_n)
        cur.execute(sql, params)
        rows = cur.fetchall()

        if debug:
            _print_debug(cur, union_sql, union_name_sql, exclude_club_clause,
                         exclude_status_clause, top_n, list(params[:-1]))

        return [(r["club_name"], round(r["total_points"])) for r in rows]


def main():
    p = argparse.ArgumentParser(
        description="Compute club standings across multiple category DBs (combined top-N per club per round). CSV output."
    )
    p.add_argument("db", nargs="+", help="One or more SQLite DB files (same schema)")
    p.add_argument("--top-n", type=int, default=6, help="Top N riders per club per round (default: 6)")
    p.add_argument("--exclude-club", action="append", default=[], help="Exclude club (repeatable)")
    p.add_argument("--exclude-status", action="append", default=["DNS"], help="Exclude status (repeatable), default: DNS")
    p.add_argument("--per-round", action="store_true", help="Output per-round totals instead of overall season totals")
    p.add_argument("--debug", action="store_true", help="Print per-round rider selection detail to stderr")
    args = p.parse_args()

    rows = compute_team_points_multi(
        db_paths=args.db,
        top_n=args.top_n,
        exclude_clubs=args.exclude_club,
        exclude_statuses=args.exclude_status,
        per_round=args.per_round,
        debug=args.debug,
    )

    w = csv.writer(sys.stdout)

    if args.per_round:
        w.writerow(["round", "position", "club_name", "top6_points"])
        current_round = None
        rank = 0
        for rnd, club, pts in rows:
            if rnd != current_round:
                current_round = rnd
                rank = 1
            else:
                rank += 1
            w.writerow([rnd, rank, club, pts])
    else:
        w.writerow(["position", "club_name", "total_points"])
        for i, (club, pts) in enumerate(rows, start=1):
            w.writerow([i, club, pts])


if __name__ == "__main__":
    main()
