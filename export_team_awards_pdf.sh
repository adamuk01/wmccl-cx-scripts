#!/usr/bin/bash
#
# Generates printable PDFs of the 3 team/club awards (Team Competition,
# U12 Team Competition, Mick Ives Participation Award) into league_pdf/,
# alongside the league-table PDFs. PDF counterpart to export_team_awards_html.sh.
#
# Run this from your round's working directory, alongside the season's DBs
# (same directory as produce-lt.sh / export_league_tables_pdf.sh).
# Only the WMCCL logo is used (../sponsors/WMCCL.png) - no sponsor logos on the awards.
#
# --top-n and --exclude-club: keep in sync with export_team_awards_html.sh
# and run-team-awards-results.sh.

# Set PATH to include binary
PATH=$PATH:../bin

mkdir -p league_pdf

echo "Running Team & Club Awards PDFs (Team Competition, U12 Team Competition, Mick Ives Participation Award)"
export_team_awards_pdf.py --outdir league_pdf \
      --top-n 6 \
      --exclude-club "No Club/Team" \
      --league-logo ../sponsors/WMCCL.png

chmod -R 777 league_pdf

exit 0
