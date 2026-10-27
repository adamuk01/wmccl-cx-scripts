#!/bin/bash

export_rider_pages.py --db U8.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db
export_rider_pages.py --db U10.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db
export_rider_pages.py --db U12.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db

export_rider_pages.py --db Youth.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db
export_rider_pages.py --db Women.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db

export_rider_pages.py --db Seniors.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db
export_rider_pages.py --db Masters.db --outdir league_html --rounds 11 --rounds-file ../Initial-load/Rounds.csv --history ../Initial-load/wmccl_history.db --season 2026-27 --history ../Initial-load/wmccl_history.db
