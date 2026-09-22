#!/bin/bash
#
# Run the lapchart generation AFTER we have the D3 results.
#
#

mkdir lapcharts

Race=$*

lap_chart.py --csv Youth-results.csv --out lapcharts/Youth.html --split-gender --title "$Race"
lap_chart.py --csv Seniors-results.csv --out lapcharts/Seniors.html --title "$Race"
lap_chart.py --csv Masters-results.csv --out lapcharts/Masters.html --title "$Race"
lap_chart.py --csv Women-results.csv --out lapcharts/Women.html --merge-all-categories --title "$Race"
