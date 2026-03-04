#!/usr/bin/env python3
"""
create_league_dbs.py

Create multiple SQLite databases for a CX league: one DB per race entity.

Each database gets:
  - riders       (identity + category)
  - results      (one row per rider per round)
  - vw_rider_round_matrix  (spreadsheet-style: r1_points, r1_AP, ..., rN_points, rN_AP)
  - vw_rider_stats         (auto averages & totals)
  - vw_ranked_results      (per-rider ranking of results by points)
  - vw_bestN_points        (best-N points sum)
  - vw_league_table_bestN  (final league table using best-N)

Edit RACE_DATABASES, MAX_ROUNDS and BEST_N below as needed.
"""
#!/usr/bin/env python3
import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

DB_DIR = ""  # leave empty to create in current folder

RACE_DATABASES = [
    {"filename": "U8.db", "label": "Under 8"},
    {"filename": "U10.db", "label": "Under 10"},
    {"filename": "U12.db", "label": "Under 12"},
    {"filename": "Youth.db", "label": "U14 and U16"},
    {"filename": "Masters.db", "label": "Open Masters 50+"},
    {"filename": "Women.db", "label": "Females Junior+"},
    {"filename": "Seniors.db", "label": "Open Senior/Junior/M40"},
]

MAX_ROUNDS = 12   # change any season
BEST_N = 10       # best-X rule

# ---------------------------------------------------------------------------

def create_core_tables(conn):
    conn.executescript("""
        PRAGMA foreign_keys = ON;

        -- Riders (updated version)
        CREATE TABLE IF NOT EXISTS riders (
            id INTEGER PRIMARY KEY,
            race_number INTEGER UNIQUE NOT NULL,
            BC_number INTEGER,

            firstname TEXT,
            surname TEXT,
            gender TEXT,
            club_name TEXT,

            race_category TEXT,               -- current year
            race_category_previous_year TEXT, -- NEW for grids
            average_points_last_year REAL,    -- NEW for grids

            DOB TEXT,
            YOB INTEGER,
            IBX TEXT
        );

        -- One row per rider per round
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY,
            rider_id INTEGER NOT NULL,
            round INTEGER NOT NULL,

            cat_position INTEGER,
            overall_position INTEGER,
            points INTEGER,
            is_ap INTEGER DEFAULT 0,
            status TEXT DEFAULT 'FIN',
            notes TEXT,

            UNIQUE(rider_id, round),
            FOREIGN KEY(rider_id) REFERENCES riders(id)
        );

        CREATE INDEX IF NOT EXISTS idx_results_rider_round
            ON results(rider_id, round);
    """)

def create_pivot_view(conn, max_rounds):
    cols = []
    for i in range(1, max_rounds + 1):
        cols.append(f"""
    MAX(CASE WHEN res.round = {i} THEN res.points END) AS r{i}_points,
    CASE WHEN MAX(CASE WHEN res.round = {i} AND res.is_ap = 1 THEN 1 END) = 1
         THEN 'AP' ELSE '' END AS r{i}_AP
""".rstrip())
    cols_sql = ",\n".join(cols)
    conn.execute(f"""
    CREATE VIEW IF NOT EXISTS vw_rider_round_matrix AS
    SELECT r.id AS rider_id, r.race_number, r.firstname, r.surname,
           r.race_category, r.race_category_previous_year,
           r.average_points_last_year,
{cols_sql}
    FROM riders r
    LEFT JOIN results res ON res.rider_id = r.id
    GROUP BY r.id;
    """)

def create_stats_view(conn):
    conn.executescript("""
    CREATE VIEW IF NOT EXISTS vw_rider_stats AS
    SELECT
        r.id AS rider_id, r.race_number, r.firstname, r.surname, r.race_category,
        r.race_category_previous_year, r.average_points_last_year,

        COUNT(CASE WHEN res.status='FIN' AND res.is_ap=0 AND res.points<>999 THEN 1 END)
            AS races_finished_real,

        AVG(CASE WHEN res.status='FIN' AND res.is_ap=0 AND res.points<>999 THEN res.points END)
            AS avg_points_real,

        SUM(CASE WHEN res.points<>999 THEN res.points ELSE 0 END)
            AS total_points_incl_ap

    FROM riders r
    LEFT JOIN results res ON res.rider_id = r.id
    GROUP BY r.id;
    """)

def create_bestN_views(conn, best_n):
    conn.executescript(f"""
    CREATE VIEW IF NOT EXISTS vw_ranked_results AS
    SELECT
        res.id,res.rider_id,res.round,res.points,res.is_ap,res.status,
        ROW_NUMBER() OVER (PARTITION BY res.rider_id ORDER BY res.points DESC)
            AS rank_by_points
    FROM results res
    WHERE res.points IS NOT NULL AND res.points<>999;

    CREATE VIEW IF NOT EXISTS vw_best{best_n}_points AS
    SELECT rider_id, SUM(points) AS best{best_n}_points
    FROM vw_ranked_results
    WHERE rank_by_points <= {best_n}
    GROUP BY rider_id;

    CREATE VIEW IF NOT EXISTS vw_league_table_best{best_n} AS
    SELECT
        s.*, COALESCE(b.best{best_n}_points,0) AS best{best_n}_points
    FROM vw_rider_stats s
    LEFT JOIN vw_best{best_n}_points b ON b.rider_id = s.rider_id
    ORDER BY best{best_n}_points DESC, s.total_points_incl_ap DESC;
    """)

def create_schema(path):
    print(f"→ Creating {path}")
    conn = sqlite3.connect(path)
    create_core_tables(conn)
    create_pivot_view(conn, MAX_ROUNDS)
    create_stats_view(conn)
    create_bestN_views(conn, BEST_N)
    conn.commit()
    conn.close()

def main():
    base = Path(DB_DIR or ".")
    for race in RACE_DATABASES:
        create_schema(base / race["filename"])
    print("\nAll databases created successfully 👍")

if __name__ == "__main__":
    from pathlib import Path
    main()

