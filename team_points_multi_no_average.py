#!/usr/bin/env python3
import sqlite3
import argparse
import csv
import sys
from typing import List, Tuple


def compute_team_points_multi(
    db_paths: List[str],
    top_n: int = 6,
    exclude_clubs: List[str] | None = None,
    exclude_statuses: List[str] | None = None,
    per_round: bool = False,
) -> List[Tuple]:
    """
    Combine multiple category DBs (same schema) and compute:
      - per club, per round: top_n points across ALL included DBs
      - overall: sum across rounds
    Optionally return per-round totals.

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

    # Build UNION of all result rows across attached DBs
    union_parts = []
    for a in aliases:
        union_parts.append(f"""
            SELECT
                TRIM(r.club_name) AS club_name,
                res.round AS round_number,
                COALESCE(res.points, 0) AS points,
                COALESCE(res.is_ap, 0) AS is_ap,
                COALESCE(res.status, 'FIN') AS status
            FROM {a}.results res
            JOIN {a}.riders r ON r.id = res.rider_id
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

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
            WHERE is_ap = 0
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
        return [(r["round_number"], r["club_name"], r["topn_points"]) for r in rows]

    else:
        sql = f"""
        WITH AllRows AS (
            {union_sql}
        ),
        Filtered AS (
            SELECT club_name, round_number, points
            FROM AllRows
            WHERE is_ap = 0
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
        return [(r["club_name"], r["total_points"]) for r in rows]


def main():
    p = argparse.ArgumentParser(
        description="Compute club standings across multiple category DBs (combined top-N per club per round). CSV output."
    )
    p.add_argument("db", nargs="+", help="One or more SQLite DB files (same schema)")
    p.add_argument("--top-n", type=int, default=6, help="Top N riders per club per round (default: 6)")
    p.add_argument("--exclude-club", action="append", default=[], help="Exclude club (repeatable)")
    p.add_argument("--exclude-status", action="append", default=["DNS"], help="Exclude status (repeatable), default: DNS")
    p.add_argument("--per-round", action="store_true", help="Output per-round totals instead of overall season totals")
    args = p.parse_args()

    rows = compute_team_points_multi(
        db_paths=args.db,
        top_n=args.top_n,
        exclude_clubs=args.exclude_club,
        exclude_statuses=args.exclude_status,
        per_round=args.per_round,
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

