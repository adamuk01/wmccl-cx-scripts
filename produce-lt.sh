#!/usr/bin/bash

# You need to change into the directory with ALL the results files & tell this script which week/round you are loading.
# It will then run ALL the script to update all the races...

# Set PATH to include binary
PATH=$PATH:../bin

echo "Running U8 league tables"
export_league_tables.py --db U8.db --profile u8 --avg-decimals 1

echo "Running U10 league tables"
export_league_tables.py --db U10.db --profile u10 --avg-decimals 1

echo "Running U12 league tables"
export_league_tables.py --db U12.db --profile u12 --avg-decimals 1

echo "Running Youth league tables"
export_league_tables.py --db Youth.db --profile youth --avg-decimals 1

echo "Running Womens league tables"
export_league_tables.py --db Women.db --profile women --avg-decimals 1

echo "Running Masters league tables"
export_league_tables.py --db Masters.db --profile masters --avg-decimals 1

echo "Running Senior league tables"
export_league_tables.py --db Seniors.db --profile seniors --avg-decimals 1

#echo "Running Team awards update"
#run-team-awards-results.sh $Week
#

chmod -R 777 league_tables

exit 0
