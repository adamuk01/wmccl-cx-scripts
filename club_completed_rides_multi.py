#!/usr/bin/env python3
import sqlite3
import argparse
import csv
import sys
from typing import List, Tuple


def compute_completed_rides_multi(
    db_paths: List[str],
    exclude_clubs: List[str] | None = None,
):
    """
    Count COMPLETED rides (status='FIN') per club across multiple DBs.

    Schema assumed:
      riders(id, club_name, ...)
      results(rider_id, round, status, is_ap, ...)
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

    # Build UNION of all completed rides
    union_parts = []
    for a in aliases:
        union_parts.append(f"""
            SELECT
                TRIM(r.club_name) AS club_name
            FROM {a}.results res
            JOIN {a}.riders r ON r.id = res.rider_id
            WHERE res.status = 'FIN'
              AND COALESCE(res.is_ap, 0) = 0
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    params: List[object] = []
    exclude_clause = ""
    if exclude_clubs:
        exclude_clause = f"WHERE club_name NOT IN ({','.join(['?'] * len(exclude_clubs))})"
        params.extend(exclude_clubs)

    sql = f"""
    WITH AllFinished AS (
        {union_sql}
    )
    SELECT
        club_name,
        COUNT(*) AS completed_rides
    FROM AllFinished
    {exclude_clause}
    GROUP BY club_name
    HAVING COUNT(*) > 0
    ORDER BY completed_rides DESC, club_name ASC;
    """

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return [(r["club_name"], r["completed_rides"]) for r in rows]


def main():
    p = argparse.ArgumentParser(
        description="Count COMPLETED rides (FIN) per club across multiple category DBs (CSV output)."
    )
    p.add_argument("db", nargs="+", help="One or more SQLite DB files")
    p.add_argument("--exclude-club", action="append", default=["No Club/Team"], help="Exclude club (repeatable)")
    args = p.parse_args()

    rows = compute_completed_rides_multi(
        db_paths=args.db,
        exclude_clubs=args.exclude_club,
    )

    writer = csv.writer(sys.stdout)
    writer.writerow(["position", "club_name", "completed_rides"])
    for i, (club, count) in enumerate(rows, start=1):
        writer.writerow([i, club, count])


if __name__ == "__main__":
    main()

