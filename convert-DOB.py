#!/usr/bin/env python3
"""
Quick and dirty script to convert date from DD-MON-YY into mm/dd/yy
Used after exporting data form previous year and neet to match new date format
"""
import csv
from datetime import datetime

INPUT_CSV = "aidan.csv"
OUTPUT_CSV = "corrected_aidan.csv"

with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
     open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
    writer.writeheader()

    for row in reader:
        dob = row.get("Date of birth", "")
        if dob:
            try:
                dt = datetime.strptime(dob, "%d-%b-%y")
                # month/day/year – no leading zeros
                row["Date of birth"] = f"{dt.month}/{dt.day}/{dt.strftime('%y')}"
            except ValueError:
                pass

        writer.writerow(row)
