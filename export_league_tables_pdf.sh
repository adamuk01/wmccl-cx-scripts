#!/usr/bin/bash
#
# Generates the printable PDF league tables - one PDF per table, into
# league_pdf/ (e.g. league_pdf/JunM.pdf). Boys + girls for U6, U8, U10 and
# U12 are merged into one PDF each (U6.pdf, U8.pdf, U10.pdf, U12.pdf) - add
# --no-merge to COMMON below to get separate PDFs instead. PDF counterpart to
# export_league_tables_html.sh; same scoring, same DBs, same conventions.
#
# Run this from your round's working directory (the same directory you run
# produce-lt.sh / export_league_tables_html.sh from), alongside the season's DBs.
#
# Logos live in ../sponsors/ (one directory up), same as the HTML script.
# The WMCCL logo (top-left of each PDF) is ../sponsors/WMCCL.png.
#
# --rounds / --best: keep in sync with produce-lt.sh and
# export_league_tables_html.sh each season (2026 season: 11 rounds, best 9).

# Set PATH to include binary
PATH=$PATH:../bin

OUT=league_pdf
LOGOS="--league-logo ../sponsors/WMCCL.png --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png"
COMMON="--outdir $OUT --rounds 11 --best 9"

mkdir -p $OUT

echo "Running U8 league table PDFs"
export_league_tables_pdf.py --db U8.db --profile u8 $COMMON $LOGOS

echo "Running U10 league table PDFs"
export_league_tables_pdf.py --db U10.db --profile u10 $COMMON $LOGOS

echo "Running U12 league table PDFs"
export_league_tables_pdf.py --db U12.db --profile u12 $COMMON $LOGOS

echo "Running Youth league table PDFs"
export_league_tables_pdf.py --db Youth.db --profile youth $COMMON $LOGOS

echo "Running Womens league table PDFs"
export_league_tables_pdf.py --db Women.db --profile women $COMMON $LOGOS

echo "Running Masters league table PDFs"
export_league_tables_pdf.py --db Masters.db --profile masters $COMMON $LOGOS

echo "Running Senior league table PDFs"
export_league_tables_pdf.py --db Seniors.db --profile seniors $COMMON $LOGOS

chmod -R 777 $OUT

cd $OUT
zip -r UPLOAD-league_tables_pdf.zip *


exit 0
