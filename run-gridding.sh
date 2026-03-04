#!/usr/bin/env bash
set -euo pipefail

# Produce gridding sheets after deriving rider categories from RiderHQ

PATH="$PATH:../bin"

usage() {
  echo "Usage: $0 [--config <yaml>]"
  echo "Defaults:"
  echo "  config: ../rules/gridding/master.yaml"
  exit 1
}

CONFIG="../rules/gridding/master.yaml"

if [[ "${1:-}" == "--config" ]]; then
  [[ $# -eq 2 ]] || usage
  CONFIG="$2"
fi

# Inputs / outputs
RIDERHQ_IN="WMCCLRiderEntry.csv"
RIDERHQ_OUT="allRiders+cat.csv"
OUTDIR="gridding"

# Sanity checks
for f in "$RIDERHQ_IN" "$CONFIG"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing required file: $f"
    exit 1
  fi
done

for cmd in produce_category_from_riderHQ.py generate_grids.py; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found in PATH: $cmd"
    exit 1
  fi
done

mkdir -p "$OUTDIR"

echo "==> Producing RiderHQ category CSV"
produce_category_from_riderHQ.py --as-of 2025-12-31 "$RIDERHQ_IN" "$RIDERHQ_OUT"

echo "==> Generating grids using config: $CONFIG"
generate_grids.py \
  --config "$CONFIG" \
  --riderhq "$RIDERHQ_OUT" \
  --outdir "$OUTDIR"

echo "Done. Grids written to ./$OUTDIR/"

