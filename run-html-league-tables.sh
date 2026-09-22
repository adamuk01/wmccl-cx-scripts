#!/bin/bash
#
export_league_tables_html.sh

export_team_awards_html.sh

export_rider_pages.sh

chmod -R 755 league_html

cd league_html

zip -r UPLOAD-league_tables.zip *


echo Zip file in league_html ready for upload!
