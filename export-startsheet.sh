
# Script to create the start sheets for each category

mkdir StartSheet

echo U8
export_start_sheet.py \
  --db U8.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Under 8" \
  --out StartSheet/U8_start_sheet.csv

echo U10
export_start_sheet.py \
  --db U10.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Under 10" \
  --out StartSheet/U10_start_sheet.csv

echo U12
export_start_sheet.py \
  --db U12.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Under 12" \
  --out StartSheet/U12_start_sheet.csv

echo Youth
export_start_sheet.py \
  --db Youth.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Youth (U-14 & U-16)" \
  --out StartSheet/Youth_start_sheet.csv

echo Women
export_start_sheet.py \
  --db Women.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Senior/Masters Female" \
  --out StartSheet/Women_start_sheet.csv

echo Women - Juniors
export_start_sheet.py \
  --db Women.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Junior Female" \
  --out StartSheet/Women-Junior_start_sheet.csv

echo Masters
export_start_sheet.py \
  --db Masters.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Masters 50+ Open" \
  --out StartSheet/Masters_start_sheet.csv

echo Seniors - M40
export_start_sheet.py \
  --db Seniors.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Masters 40-49 Open" \
  --out StartSheet/Seniors-M40_start_sheet.csv

echo Seniors
export_start_sheet.py \
  --db Seniors.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Senior Open" \
  --out StartSheet/Seniors_start_sheet.csv

echo Seniors
export_start_sheet.py \
  --db Seniors.db \
  --entrants corrected_WMCCLRiderEntry.csv \
  --entry-type "Junior Open" \
  --out StartSheet/Seniors_junior_start_sheet.csv
