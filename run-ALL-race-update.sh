#!/usr/bin/env bash
set -euo pipefail

# Run from the directory containing the CSV result files.
# Usage: ./run-round-imports.sh <round 1-12> [--strict]

PATH="$PATH:../bin"

usage() {
  echo "Usage: $0 <round 1-12> [--strict]"
  exit 1
}

[[ $# -ge 1 ]] || usage

ROUND="$1"
STRICT=0
if [[ "${2:-}" == "--strict" ]]; then
  STRICT=1
fi

# Validate round
if ! [[ "$ROUND" =~ ^[0-9]+$ ]] || (( ROUND < 1 || ROUND > 12 )); then
  echo "ERROR: round must be an integer between 1 and 12 (got: '$ROUND')"
  exit 1
fi

# Helper: check files
require_file() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    if (( STRICT )); then
      echo "ERROR: missing required file: $f"
      exit 1
    else
      echo "WARN: missing file (skipping): $f"
      return 1
    fi
  fi
  return 0
}

run_import() {
  local label="$1"; shift
  echo "==> $label"
  "$@"
}

# Each entry: label|db|csv|extra_args...
TASKS=(
  "U8 race update|U8.db|U8-results.csv|--split-genders"
  "U10 race update|U10.db|U10-results.csv|--split-genders"
  "U12 race update|U12.db|U12-results.csv|--split-genders"
  "Youth race update|Youth.db|Youth-results.csv|--split-genders"
  "Womens race update|Women.db|Women-results.csv|--women-single-table"
  "Masters race update|Masters.db|Masters-results.csv|"
  "Senior race update|Seniors.db|Seniors-results.csv|"
)

for t in "${TASKS[@]}"; do
  IFS='|' read -r label db csv extra <<< "$t"

  if require_file "$csv"; then
    # shellcheck disable=SC2086
    remove_bins.py "$csv"
    run_import "Running $label" \
      import_race_results.py --db "$db" --round "$ROUND" --csv "$csv" ${extra}
  fi
done

echo "Done. Round $ROUND imports complete."

