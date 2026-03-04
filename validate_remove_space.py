#!/usr/bin/env python3
import csv
import sys
import re
import unicodedata


def strip_accents(text):
    """
    Convert accented characters to ASCII equivalents.
    e.g. é -> e, Ķ -> K
    """
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def clean_field(field):
    # 1. Strip leading/trailing whitespace
    field = field.strip()

    # 2. Collapse multiple internal spaces to one
    field = re.sub(r"\s+", " ", field)

    # 3. Replace accented characters with ASCII equivalents
    field = strip_accents(field)

    return field


def check_and_fix_csv(csv_file):
    corrected_name = "corrected_" + csv_file
    lines_modified = 0

    with open(csv_file, "r", newline="", encoding="utf-8") as infile, \
         open(corrected_name, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for line_number, line in enumerate(reader, start=1):
            clean_line = []
            modified = False

            for field in line:
                cleaned = clean_field(field)
                if cleaned != field:
                    modified = True
                clean_line.append(cleaned)

            if modified:
                lines_modified += 1

            writer.writerow(clean_line)

    if lines_modified:
        print(f"Found and corrected issues on {lines_modified} line(s).")
    else:
        print("No issues found.")

    print(f"Clean file written to '{corrected_name}'.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fix_spaces.py <csv_file>")
        sys.exit(1)

    check_and_fix_csv(sys.argv[1])

