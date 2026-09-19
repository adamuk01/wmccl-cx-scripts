#!/usr/bin/bash
#
# This will call the python script to generate the league table pages.
#
# Run this from your round's working directory (the same directory you run
# produce-lt.sh from), alongside the season's DBs.
#
# Sponsor logos live in a "sponsors" folder ONE DIRECTORY UP (../sponsors/),
# not copied into each round's folder - so they're not duplicated every time
# you create a new Round directory. Adjust the ../sponsors/ paths below if
# you keep them somewhere else.
#
# --rounds / --best control the season length and how many results count
# towards the total - update these each season if they change (2026 season:
# 11 rounds, best 9 count). Keep this in sync with produce-lt.sh.

# Set PATH to include binary
PATH=$PATH:../bin

mkdir -p league_html

echo "Running U8 league table pages"
export_league_tables_html.py --db U8.db --profile u8 --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

echo "Running U10 league table pages"
export_league_tables_html.py --db U10.db --profile u10 --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

echo "Running U12 league table pages"
export_league_tables_html.py --db U12.db --profile u12 --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

echo "Running Youth league table pages"
export_league_tables_html.py --db Youth.db --profile youth --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

echo "Running Womens league table pages"
export_league_tables_html.py --db Women.db --profile women --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

echo "Running Senior league table pages"
export_league_tables_html.py --db Seniors.db --profile seniors --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

echo "Running Masters league table pages"
export_league_tables_html.py --db Masters.db --profile masters --outdir league_html \
      --rounds 11 --best 9 \
      --sponsor-logo ../sponsors/BikeFood.jpg --sponsor-logo ../sponsors/Lazer.png --sponsor-logo ../sponsors/Shimano.png

chmod -R 777 league_html

exit 0
