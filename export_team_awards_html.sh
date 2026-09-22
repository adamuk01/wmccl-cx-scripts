#!/usr/bin/bash
#
# This will call the python script to generate the 3 team/club award pages
# (Team Competition, U12 Team Competition, Mick Ives Participation Award)
# into the SAME site as export_league_tables_html.sh.
#
# Run this from your round's working directory, alongside the season's DBs
# -- same directory you run produce-lt.sh / export_league_tables_html.sh
# from. Run it AFTER export_league_tables_html.sh in a given round update
# so the sponsor logos already cached by that script show up on the award
# pages too (running it first still works, it just won't have sponsor
# logos on the award pages until export_league_tables_html.sh next runs
# and this script is re-run).
#
# --top-n controls how many of each club's scoring riders count per round
# for the two team competitions -- passed explicitly here (default 6) so
# it can never silently drift, matching the same fix already applied to
# --rounds/--best on the category tables.

# Set PATH to include binary
PATH=$PATH:../bin

mkdir -p league_html

echo "Running Team & Club Awards pages (Team Competition, U12 Team Competition, Mick Ives Participation Award)"
export_team_awards_html.py --outdir league_html \
      --top-n 6 \
      --exclude-club "No Club or Team"

chmod -R 777 league_html

exit 0
