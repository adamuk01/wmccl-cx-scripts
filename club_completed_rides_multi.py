#!/usr/bin/env python3
import argparse
import csv
import sys

from team_awards_scoring import compute_completed_rides_multi


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
