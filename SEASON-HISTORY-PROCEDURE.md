# Season history procedure — end of season and start of the next

How rider history (career totals, badges, links to previous-season pages) is kept from year to year.

## The idea in one paragraph

Race numbers change every year, so a rider is identified by a permanent **`person_id`** held in the central **`wmccl_history.db`**. At the **end of a season** that season is *archived* into the history DB (`archive_season.py`). At the **start of the next season**, each new rider entry is *linked* to its person (`assign_person_ids.py`), mainly by **date of birth** (name and BC number are supporting evidence only, because BC numbers are often wrong or missing). The weekly rider-page export then adds the live season on top of the archived seasons — so nothing is ever a running counter, and archiving can never double count.

## Files and what each does

| File | Purpose |
|---|---|
| `wmccl_history.db` | The central history. Contains DOBs — **admin only, never publish it**. |
| `person_overrides.csv` | Your decisions where matching can't be sure. Cumulative: **keep every line, only ever append.** |
| `archive_season.py` | Writes a finished season into the history. |
| `assign_person_ids.py` | Puts `person_id` on this season's riders (`riders.person_id`). |
| `export_rider_pages.py` | Weekly rider pages; with `--history … --season …` adds career sections and badges. |
| `rider_history.py`, `rider_career.py` | Libraries used by the three scripts above. **Must sit in the same folder as them.** |

Overrides file format (`season,source_db,race_number,decision`), decision is one of:
`new` (a different person from anyone on file), `person:<id>` (is that existing person), `skip` (leave out).

Season labels are always written `2026-27`.

---

## A. END OF SEASON (after the last round is final)

1. **Finish the season normally.** Final results imported, league tables and rider pages produced, average points awarded.
2. **Fix data errors at source** in the race DBs (misspelt names, wrong DOBs, emails typed as surnames, wrong categories). Whatever is wrong now becomes the permanent record.
3. **Back up the history DB:** `cp wmccl_history.db wmccl_history.db.before-2026-27`
4. **Decide which rounds are special:**
   - A round where a category was given average points for everyone (e.g. Regional Champs) → `--ap-only ROUND:DB` for **each DB affected** (e.g. `--ap-only 11:Seniors.db`). Categories that raced it for real (e.g. U8–U12 in 2025-26) are *not* listed.
   - **Average points count as a round completed** for the "5 Rounds" / "10 Rounds" / "Full House" badges and the career rounds badges — but only for rounds that were *held*: a round counts as held when at least one rider has a real finish in it. A round where **everyone in every category** got average points has no real finishers, so tell the scripts it is a real round with `--count-round N` (on both `archive_season.py` and `export_rider_pages.py`, same values). Then AP in that round counts and a rider who skipped it misses Full House (2024-25 round 10 was like this). AP never adds laps, time or points.
   - A round that shouldn't count at all → `--exclude-round N`.
   - Add a note shown with the round: `--round-note "11=Regional Champs: average points awarded to all except U8–U12"`.
5. **Dry run first** (no `--apply` — nothing is written):

   ```
   archive_season.py --season 2026-27 --history wmccl_history.db \
     --overrides person_overrides.csv --report archive_report.csv \
     --ap-only 11:Youth.db --ap-only 11:Women.db --ap-only 11:Masters.db --ap-only 11:Seniors.db \
     --round-note "11=Regional Champs: average points awarded to all except U8-U12" \
     --db U8.db U10.db U12.db Youth.db Women.db Masters.db Seniors.db
   ```

6. **Read the output.** Check:
   - *Season length for Full House* lists the rounds you expect.
   - *Rounds with NO real finishers* — only the rounds you meant to be all-AP.
   - Rider counts per DB look right ("registered but never raced" riders are deliberately left out).
   - **REVIEW** list: riders held out until you decide. Add a line to `person_overrides.csv` for each, then re-run the dry run until REVIEW is empty.
   - FLAGs (BC conflicts) are informational — the rider is linked correctly by DOB.
7. **Apply:** re-run the same command with `--apply`. It is safe to re-run (same season is replaced, IDs stay stable).
8. **Keep the pages.** Leave `/league_tables/2026-27/…` on the site untouched — next year's pages link back to it. The previous-season links assume `/league_tables/<season>/riders/<number>.html`.
9. **File away:** the season's DBs, `person_overrides.csv` and `wmccl_history.db` (copy somewhere safe / commit the CSV to git; do not put the history DB on the web).

## B. START OF THE NEW SEASON

1. **Build the new season as usual:** new DBs (`create_db.py`), RiderHQ import, `validate_and_allocate_entrants.py`, categories with `assign-category.py` (check `--season-end-year` — auto-detect treats Aug–Dec as the *next* year; use the right one when re-building old data).
2. **Link riders to people first (step 3 below), then fill last-year category and average from the history** — this replaces `export_prev_year_stats.py` + `apply_prev_year_stats.py` (and the separate `adjust_prev_avg_on_cat_change.py`):

   ```
   apply_prev_year_from_history.py --history wmccl_history.db --season 2027-28 \
       --db U8.db --cat-change-delta -10 --report prev_U8.csv        # dry run
   # happy? add --apply.  Repeat per DB with its own delta (-10 youth DBs, -5 Women/Seniors, +5 Masters)
   ```

   It uses each rider's `person_id` (so name changes and new race numbers don't matter), takes their newest earlier season's category and average, only fills riders whose two fields are blank, and applies the category-change adjustment **only to the riders it fills in that run** — so re-running for late entries never adjusts anyone twice. Riders who skipped last season are listed (GAP) with the season used. New riders (no `person_id`) stay blank, as before.

3. **Link riders to people** (do this before step 2). Dry run, read, then apply:

   ```
   assign_person_ids.py --season 2027-28 --history wmccl_history.db \
     --overrides person_overrides.csv --report assign_report.csv \
     --db U8.db U10.db U12.db Youth.db Women.db Masters.db Seniors.db
   # happy? add --apply
   ```

   - Only riders **without** an ID are filled; existing IDs are never changed, so it is safe to run again whenever more entries arrive (late entries, new riders).
   - **REVIEW** items → decide in `person_overrides.csv` (`person:<id>` / `new`) and re-run.
   - Riders with no match are **new** — they get an ID when their first season is archived, and no career section until they have an earlier season.
   - Non-league numbers (900 and over) are never tracked.
4. **Weekly rider pages**, adding two options to the existing command:

   ```
   export_rider_pages.py … --history ../Initial-load/wmccl_history.db --season 2027-28
   ```

   (`--history` and `--season` must be given together.) Pages for riders with no earlier season are unchanged.
5. **Regenerate last season's pages once** if the page template changed (e.g. the Back-button change); for a past season whose award pages aren't published add `--no-award-links`.

## C. Rules to remember

- **Never rebuild `wmccl_history.db` from scratch** once riders hold IDs — IDs would be renumbered and silently point at the wrong people. Add seasons *on top* of the existing file (any order is fine, including older seasons found later). If you ever must rebuild, re-link every season's DBs with `assign_person_ids.py --force`.
- **Never delete lines from `person_overrides.csv`** — `person:<id>` and `new` decisions are what keep re-runs stable.
- **A rider's number never changes mid-season** (a lost number is reissued as the same number). Riding in the wrong category earns no points or position.
- **Name changes / DOB typos / twins and siblings** are exactly what the REVIEW list and overrides are for: twins and siblings with a shared BC number are separate people (`new`); the same name and BC with a different DOB is usually the same person (`person:<id>`).
- **Fix mistakes at source and re-run** — the archive is rebuilt from the race DBs, so correcting a DB and re-running `archive_season.py --apply` corrects the history. Corrections to *older* seasons need that old season's DBs.
- **Mid-season DOB corrections** after IDs are assigned: the rider keeps their ID in the race DB, but at archive time the DOB is compared with the history again, so such a rider may appear on the REVIEW list — answer it with `person:<id>` in the overrides file.
- **Privacy:** pages show counts and links only — never DOB or BC number. Don't publish the history DB or the report CSVs.

## D. Quick checklist

**End of season:** fix data → back up history → dry run (with `--ap-only` / `--round-note`) → clear REVIEW via overrides → `--apply` → keep the season's pages online → store DBs + overrides.

**Start of season:** build DBs → check categories/season-end-year → `assign_person_ids.py` dry run → clear REVIEW → `--apply` (repeat as entries arrive) → fix carry-forward items from the AUDIT → weekly export with `--history … --season …`.
