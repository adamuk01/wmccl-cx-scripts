#!/usr/bin/env python3
import csv
import sys

# Expected header fields (in exact order)
EXPECTED_FIELDS = [
    "Entry type",
    "Bib number",
    "First name",
    "Last name",
    "Has membership",
    "Entered by",
    "Amount paid",
    "sex",
    "Date of birth",
    "club",
    "Are you a member of British Cycling?",
    "Membership ID",
    "emergency_contact_det",
    "Membership number"
]

def check_csv_headers(csv_file_path):
    try:
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            actual_fields = next(reader)

            missing_fields = [field for field in EXPECTED_FIELDS if field not in actual_fields]
            extra_fields = [field for field in actual_fields if field not in EXPECTED_FIELDS]

            print("✅ Checked file:", csv_file_path)
            print()

            if actual_fields == EXPECTED_FIELDS:
                print("✅ CSV has the correct headers in the correct order.")
            else:
                print("⚠️ CSV headers do not match exactly.")
                if missing_fields:
                    print("❌ Missing fields:")
                    for field in missing_fields:
                        print("   -", field)
                if extra_fields:
                    print("⚠️ Extra/unexpected fields:")
                    for field in extra_fields:
                        print("   -", field)
                print()
                print("ℹ️ Expected fields:")
                print("   ", EXPECTED_FIELDS)
                print("ℹ️ Actual fields:")
                print("   ", actual_fields)

    except FileNotFoundError:
        print(f"❌ File not found: {csv_file_path}")
    except Exception as e:
        print(f"❌ Error processing file: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: check_csv_headers.py <csv_file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    check_csv_headers(file_path)

