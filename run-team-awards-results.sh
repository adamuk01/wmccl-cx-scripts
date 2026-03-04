#!/usr/bin/env bash
set -euo pipefail

# Run from the directory containing the DB files (as you noted)
# Make sure our python tools are found (../bin relative to this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$PATH:$SCRIPT_DIR/../bin"

OUTDIR="teamawards"
mkdir -p "$OUTDIR"

# DB files required in the *current* directory
REQUIRED_DBS=(
  "Seniors.db"
  "Women.db"
  "Masters.db"
  "Youth.db"
  "U8.db"
  "U10.db"
  "U12.db"
)

for f in "${REQUIRED_DBS[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "Cannot open DB file '$f'. Exiting."
    exit 5
  fi
done

echo "Running TEAM COMPETITION - Highest scoring 6 riders from each club per round (combined categories)"

# Youth team (U8+U10+U12 combined)
team_points_multi.py U8.db U10.db U12.db \
  --exclude-club "No Club/Team" \
  > "$OUTDIR/youth_team.csv"

# Adults team (Women+Seniors+Masters+Youth combined)
team_points_multi.py Women.db Seniors.db Masters.db Youth.db \
  --exclude-club "No Club/Team" \
  > "$OUTDIR/adult_team.csv"

echo "Running Participation Award (Completed rides - FIN only) for the Mick Ives Trophy"

club_completed_rides_multi.py U8.db U10.db U12.db Women.db Seniors.db Masters.db Youth.db \
  --exclude-club "No Club/Team" \
  > "$OUTDIR/participation.csv"

echo "Done. Outputs in $OUTDIR/"
ls -1 "$OUTDIR"

