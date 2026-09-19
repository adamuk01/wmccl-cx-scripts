#!/usr/bin/env python3
import argparse
import csv
import sys

from team_awards_scoring import compute_team_points_multi


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
