#!/usr/bin/env python3
import csv
import argparse
from datetime import datetime, date
from typing import Optional, Dict, Tuple

try:
    import yaml  # pip install pyyaml
except ImportError:
    yaml = None


DOB_FORMATS = [
    # RiderHQ variants seen in the wild
    "%d-%b-%y",   # 12-Aug-48
    "%d-%b-%Y",   # 14-Feb-2008
    "%d-%b-%Y",   # 14-JUN-2008 (works)
    "%d-%b-%y",   # 14-JUN-08 (works)
    # Slash formats (depending on report/export)
    "%d/%m/%Y",
    "%d/%m/%y",
    "%m/%d/%Y",
    "%m/%d/%y",
]


def parse_dob(dob_str: str, max_year: int) -> Optional[date]:
    """
    Parse RiderHQ DOB strings in multiple formats.

    Key fix for the infamous '%y' problem:
      - Python's %y window can interpret '48' as 2048.
      - We correct any parsed year > max_year by subtracting 100 years.
        max_year should be the season "as-of" year, not today's year.

    Example:
      dob_str="12-Aug-48" -> parses to 2048-08-12 -> corrected to 1948-08-12
    """
    s = (dob_str or "").strip()
    if not s:
        return None

    for fmt in DOB_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year > max_year:
                dt = dt.replace(year=dt.year - 100)
            return dt.date()
        except Exception:
            continue
    return None


def age_on(dob: date, as_of: date) -> int:
    """Compute age in full years on the as_of date."""
    years = as_of.year - dob.year
    if (as_of.month, as_of.day) < (dob.month, dob.day):
        years -= 1
    return years


def as_of_from_season(season_start_year: int, rule: str) -> date:
    """
    season_start_year=2025 -> season is 2025/26
    rule:
      - 'start' => 31 Dec 2025
      - 'end'   => 31 Dec 2026
    """
    if rule not in ("start", "end"):
        raise ValueError("rule must be 'start' or 'end'")
    year = season_start_year if rule == "start" else season_start_year + 1
    return date(year, 12, 31)


def get_age_category_from_age(age: Optional[int]) -> str:
    """
    Map age (in years) to your league labels.
    (Matches the strings you're using in YAML filters.)
    """
    if age is None:
        return "Unknown"
    if age <= 5:
        return "Under-6"
    elif age <= 7:
        return "Under-8"
    elif age <= 9:
        return "Under-10"
    elif age <= 11:
        return "Under-12"
    elif age <= 13:
        return "Under-14"
    elif age <= 15:
        return "Under-16"
    elif age <= 17:
        return "Junior"
    elif age <= 22:
        return "Under-23"
    elif age <= 39:
        return "Senior"
    elif age <= 44:
        return "Masters 40-44"
    elif age <= 49:
        return "Masters 45-49"
    elif age <= 54:
        return "Masters 50-54"
    elif age <= 59:
        return "Masters 55-59"
    elif age <= 64:
        return "Masters 60-64"
    elif age <= 69:
        return "Masters 65-69"
    elif age <= 74:
        return "Masters 70-74"
    else:
        return "Masters 75+"


def load_overrides(path: Optional[str]) -> Dict[str, str]:
    """
    Optional YAML overrides file format:
      overrides:
        "560": "Junior"
        "925": "Junior"

    Keys are Membership number as string.
    """
    if not path:
        return {}
    if yaml is None:
        raise SystemExit("PyYAML not installed. Run: pip3 install pyyaml")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    ov = data.get("overrides", {}) or {}
    return {str(k).strip(): str(v).strip() for k, v in ov.items()}


def categorize_csv(
    input_file: str,
    output_file: str,
    as_of: date,
    overrides_path: Optional[str],
    debug: bool = False,
) -> Tuple[int, Dict[str, int], int]:
    """
    Adds/updates 'Age Category' column based on Date of birth and season as_of date.
    Returns: (num_rows, category_counts, parse_failures)
    """
    overrides = load_overrides(overrides_path)
    category_count: Dict[str, int] = {}
    num_rows = 0
    parse_failures = 0

    with open(input_file, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames:
            raise SystemExit("Input CSV has no header row.")

        fieldnames = list(reader.fieldnames)
        if "Age Category" not in fieldnames:
            fieldnames.append("Age Category")

        with open(output_file, "w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                mem = str(row.get("Membership number", "") or "").strip()
                dob_str = row.get("Date of birth", "") or ""

                if mem and mem in overrides:
                    category = overrides[mem]
                    reason = "override"
                else:
                    dob = parse_dob(dob_str, max_year=as_of.year)
                    if dob is None:
                        category = "Unknown"
                        reason = "dob-parse-failed"
                        parse_failures += 1
                    else:
                        age = age_on(dob, as_of)
                        category = get_age_category_from_age(age)
                        reason = f"age={age}"

                row["Age Category"] = category
                writer.writerow(row)

                category_count[category] = category_count.get(category, 0) + 1
                num_rows += 1

                if debug:
                    print(f"[DEBUG] {num_rows}: mem={mem} dob='{dob_str}' -> {category} ({reason})")

    return num_rows, category_count, parse_failures


def main():
    p = argparse.ArgumentParser(
        description="Add/Update 'Age Category' column in RiderHQ CSV based on season (and optional overrides)."
    )
    p.add_argument("input_file", help="Input RiderHQ CSV (e.g. allRiders.csv)")
    p.add_argument("output_file", help="Output CSV with Age Category (e.g. allRiders+cat.csv)")

    # Season controls
    p.add_argument(
        "--season-start",
        type=int,
        default=None,
        help="Season start year (e.g. 2025 for season 2025/26). Handy for 'pretend it's last season'.",
    )
    p.add_argument(
        "--as-of-rule",
        choices=["start", "end"],
        default="end",
        help="When using --season-start, compute ages as of 31 Dec of season 'start' year or 'end' year. Default: end.",
    )

    # Absolute override
    p.add_argument(
        "--as-of",
        default=None,
        help="Override as-of date directly (YYYY-MM-DD). If set, beats --season-start.",
    )

    # Manual correction mechanism
    p.add_argument(
        "--overrides",
        default=None,
        help="Optional YAML overrides mapping Membership number -> Age Category.",
    )

    p.add_argument("-D", "--debug", action="store_true", help="Enable debug output")

    args = p.parse_args()

    # Decide as_of date
    if args.as_of:
        try:
            as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date()
        except Exception:
            raise SystemExit("Invalid --as-of format. Use YYYY-MM-DD.")
    elif args.season_start is not None:
        as_of = as_of_from_season(args.season_start, args.as_of_rule)
    else:
        # fallback: current date (not recommended for CX season, but useful in a pinch)
        raise SystemExit("You must specify --season-start (and optional --as-of-rule) or --as-of YYYY-MM-DD")


    num_rows, category_count, parse_failures = categorize_csv(
        args.input_file,
        args.output_file,
        as_of=as_of,
        overrides_path=args.overrides,
        debug=args.debug,
    )

    print("\nProcessing complete.")
    print(f"Total rows processed: {num_rows}")
    print(f"As-of date used for categorisation: {as_of.isoformat()}")

    if args.season_start is not None and not args.as_of:
        print(f"Season interpreted as: {args.season_start}/{str(args.season_start + 1)[-2:]} (as-of-rule={args.as_of_rule})")

    if parse_failures:
        print(f"WARNING: {parse_failures} rows had DOB parse failures (Age Category set to 'Unknown').")

    print("Category breakdown:")
    for cat, count in sorted(category_count.items(), key=lambda x: x[0]):
        print(f"  {cat}: {count}")

    print(f"Updated data saved to: {args.output_file}")


if __name__ == "__main__":
    main()

