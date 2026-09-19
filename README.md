# WMCCL League Scripts

Tools for running the **West Midlands Cyclo-Cross League (WMCCL)** season: importing race results, tracking rider/team standings, producing start sheets and start-grid PDFs, and publishing league tables, rider pages, and team/club awards as a website.

Each race category (U8, U10, U12, Youth, Women, Masters, Seniors) has its own SQLite database. Scripts operate per-database (or across all seven at once for whole-season tasks like awards).

## How the season works

1. **Set up** the season's databases from RiderHQ membership data, carrying forward last year's category and average points.
2. **Each round**, before racing: validate the entry list, produce start sheets, and generate start grids based on current league standings.
3. **Each round**, after racing: import the results CSV, then regenerate league tables, team/club awards, and the public website.
4. **Ad hoc**: allocate average points (for marshals, no-shows, regional champs), fix data issues, and re-check rider/club counts.

## Project layout

- **`create_db.py`** — creates the seven category databases with the core schema (riders, results, rounds).
- **`claude/`** — newer scripts and design notes, mostly for the HTML site (see below) and recent refactors; kept separate from the older root-level scripts.
- **Round working directories** — Adam creates a new "Round N" folder each week (results CSVs, DB copies, sponsor logos), so any week can be rolled back to.

## Setting up a new season

```
create_db.py                                   # creates the 7 DBs
validate_remove_space.py RiderHQMembership...csv
initial-riderHQ-data-import.py --csv ... --validate-only
initial-riderHQ-data-import.py --db U8.db --csv ... --category-filter "Under 8"
# ...repeat per category, splitting Women/Masters/Seniors by sex+age (see Instructions.rtf)
assign-category.py U8.db                       # ...per DB
export_prev_year_stats.py --db-pattern "/path/to/last/season/*.db" --out previousyear.csv
apply_prev_year_stats.py --db Masters.db --csv previousyear.csv   # ...per DB
adjust_prev_avg_on_cat_change.py --db Women.db --delta -5          # category-change adjustment
normalise-rider-names.py DATABASE.db            # ...per DB
count-riders.py *.db                            # sanity-check counts
list_clubs.py --db-pattern "*.db"                # catch duplicate/incorrect club names
```

## Weekly round workflow

**Before the race — entrants, start sheets, gridding:**

```
validate_csv_report.py WMCCLRiderEntry.csv
validate_remove_space.py WMCCLRiderEntry.csv
mv corrected_WMCCLRiderEntry_...csv WMCCLRiderEntry.csv
validate-riderHQ-file.sh                        # → corrected_WMCCLRiderEntry.csv, allocates non-league numbers

export-startsheet.sh                            # → per-category start sheet CSVs
summerise-riders.sh WMCCLRiderEntry.csv          # counts, for the organiser

# edit raceheader.txt (date/location for the round), then:
run-gridding.sh                                  # category assignment + YAML-driven grid sheets (generate_grids.py)
```

**After the race — import results and publish:**

```
import_race_results.py --db Women.db --round N --csv Women-results.csv --women-single-table
import_race_results.py --db U8.db --round N --csv U8-results.csv --split-genders
# ...one per category (or run-ALL-race-update.sh to do all seven)

produce-lt.sh                                    # league table CSVs (export_league_tables.py, per profile)
run-team-awards-results.sh                       # team/club award CSVs (team_points_multi.py, club_completed_rides_multi.py)

export_league_tables_html.sh                     # league table HTML site (one call per category)
export_rider_pages.py --db ... --outdir league_html --rounds 11 --rounds-file rounds.csv
export_team_awards_html.sh                       # team/club award HTML pages
```

Import automatically strips binary characters, excludes non-league/DNF riders from scoring, and re-orders finishing and category positions. League table and rider CSVs are then copied to a Mac and pasted into the season's spreadsheet/PDF for the old-style paper output.

## The HTML site

A static site generated alongside the existing CSV/spreadsheet workflow (not a replacement for it):

- **League tables** (`export_league_tables_html.py` / `.sh`) — one page per category, season index, sponsor logos.
- **Rider pages** (`export_rider_pages.py`) — one page per rider (`riders/<race_number>.html`), privacy-conscious (first name + surname initial only), with medals, milestone badges, and results history.
- **Team & club awards** (`export_team_awards_html.py` / `.sh`) — Team Competition, U12 Team Competition, and the Mick Ives Participation Award, each as a standalone page under `awards/`.

All three generators share a scoring core (`league_scoring.py`, `team_awards_scoring.py`) with the CSV exporters, so HTML and CSV output can't drift apart, and share CSS/helpers so the site reads as one system. The site is designed to be embedded in a WordPress page via `<iframe>` (same-origin, with an auto-resizing script) so it inherits the club site's header and navigation.

## Average points & regional champs

```
allocate-average-points.py --db Masters.db --round N --race-number 300           # give AP, with confirmation prompt
allocate-average-points.py --db Masters.db --round N --race-number 301 --clear   # remove AP
allocate-average-points.py --db Masters.db --round N --race-number 300 --no-prompt
```

## Validation & housekeeping

- `count-riders.py`, `list_clubs.py` — sanity-check rider counts and catch club-name inconsistencies (e.g. "CC" vs "Cycling Club").
- `validate_csv_report.py`, `validate_remove_space.py`, `validate_and_allocate_entrants.py` — clean and validate incoming CSVs from RiderHQ and the race organiser.
- `most_improved_rider.py`, `pace_grade_scoring.py` — supplementary rider-performance scoring/awards.

## Requirements

Python 3 with SQLite (standard library), plus ReportLab for grid-sheet PDF generation. Race results come from the RaceTec/D3 timing system; entry data from RiderHQ; the public site is embedded in the WMCCL WordPress site.

## Status

The CSV/spreadsheet workflow is the long-standing production path. The HTML site (league tables, rider pages, team/club awards) is newer, has been run successfully against real season data, and is being rolled out alongside it — see `claude/html-league-tables-plan.md` for the detailed build history and open items.
