#!/usr/bin/env python3
"""
team_awards_scoring.py
------------------------
Shared scoring logic for the WMCCL team/club awards — the club-level
counterpart to league_scoring.py's rider-level scoring.

This module is imported by BOTH the CSV scripts (team_points_multi.py,
club_completed_rides_multi.py) and the new HTML awards page generator
(export_team_awards_html.py), so all three outputs can never disagree on
a club's points or rides-counted total. Extracted 2026-09-19 from
team_points_multi.py / club_completed_rides_multi.py, behaviour-preserving
(CSV output verified byte-identical before/after for both scripts).

Nothing in here touches argv or writes CSV — it's pure DB-in,
plain-Python-data-structures-out, mirroring league_scoring.py's own
no-I/O-beyond-the-DB-query convention.
"""

import sqlite3
from typing import List, Optional, Tuple


def compute_team_points_multi(
    db_paths: List[str],
    top_n: int = 6,
    exclude_clubs: Optional[List[str]] = None,
    exclude_statuses: Optional[List[str]] = None,
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

    Returns, when per_round=False: [(club_name, total_points), ...]
    ordered by total_points desc, club_name asc.

    Returns, when per_round=True: [(round_number, club_name, topn_points), ...]
    ordered by round_number asc, topn_points desc, club_name asc.
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
        conn.close()
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
        conn.close()
        return [(r["club_name"], r["total_points"]) for r in rows]


def compute_completed_rides_multi(
    db_paths: List[str],
    exclude_clubs: Optional[List[str]] = None,
) -> List[Tuple]:
    """
    Count rides counted towards participation per club across multiple
    DBs — the Mick Ives Participation Award calculation.

    Counts BOTH completed races (status='FIN') AND rounds where the rider
    was awarded Average Points (status='AP', is_ap=1) instead of racing.
    AP is commonly given to a club's own riders/volunteers who are
    marshalling or organising their own round rather than racing it, so
    excluding AP rows (the original behaviour) systematically
    disadvantaged clubs that host events. Changed 2026-09-19 at Adam's
    request — AP rounds now count the same as a finish for this award.

    Schema assumed:
      riders(id, club_name, ...)
      results(rider_id, round, status, is_ap, ...)

    Returns [(club_name, rides_counted), ...] ordered by rides_counted
    desc, club_name asc.
    """

    exclude_clubs = [c.strip() for c in (exclude_clubs or [])]

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Attach DBs and validate schema
    aliases = []
    for i, path in enumerate(db_paths):
        alias = f"db{i}"
        aliases.append(alias)
        cur.execute(f"ATTACH DATABASE ? AS {alias};", (path,))

        cur.execute(f"SELECT name FROM {alias}.sqlite_master WHERE type='table';")
        tables = {row[0] for row in cur.fetchall()}
        missing = [t for t in ("riders", "results") if t not in tables]
        if missing:
            raise SystemExit(
                f"DB '{path}' is missing table(s): {', '.join(missing)}. "
                f"Tables present: {', '.join(sorted(tables))}"
            )

    # Build UNION of all rides that count towards participation: a real
    # finish, or a round where the rider was awarded Average Points
    # (status='AP') instead of racing — see docstring above for why AP
    # counts here.
    union_parts = []
    for a in aliases:
        union_parts.append(f"""
            SELECT
                TRIM(r.club_name) AS club_name
            FROM {a}.results res
            JOIN {a}.riders r ON r.id = res.rider_id
            WHERE res.status IN ('FIN', 'AP')
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    params: List[object] = []
    exclude_clause = ""
    if exclude_clubs:
        exclude_clause = f"WHERE club_name NOT IN ({','.join(['?'] * len(exclude_clubs))})"
        params.extend(exclude_clubs)

    sql = f"""
    WITH AllCounted AS (
        {union_sql}
    )
    SELECT
        club_name,
        COUNT(*) AS completed_rides
    FROM AllCounted
    {exclude_clause}
    GROUP BY club_name
    HAVING COUNT(*) > 0
    ORDER BY completed_rides DESC, club_name ASC;
    """

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [(r["club_name"], r["completed_rides"]) for r in rows]
