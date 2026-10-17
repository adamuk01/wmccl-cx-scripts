#!/usr/bin/env python3
"""
find_rider.py

Fuzzy-search every league database (*.db) in the current directory for a
rider by name and print their race number, name, club and which DB they're in.

Handy when allocating average points and all you've been given is a name.

Usage:
    python3 find_rider.py "Tom Pidcock"
    python3 find_rider.py tom pidcok              # quotes optional, typos OK
    python3 find_rider.py "Pidcock, Tom"          # surname-first works too
    python3 find_rider.py "tom p" --threshold 0.5 --limit 20
    python3 find_rider.py "Smith" --dir /path/to/dbs

Matching:
  - case, accents, punctuation and extra spaces are ignored
  - word order doesn't matter ("Smith John" == "John Smith")
  - typos and near-misses are scored with difflib similarity
  - initials / prefixes match ("J Smith", "Jon" -> "Jonathan")
"""
import argparse
import sqlite3
import sys
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path


def normalise(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = "".join(c if c.isalnum() else " " for c in s)
    return " ".join(s.split())


def ratio(a, b):
    return SequenceMatcher(None, a, b).ratio()


def token_score(qt, nt):
    """Score one query token against one name token."""
    if qt == nt:
        return 1.0
    # initial or prefix ("j" -> "john", "jon" -> "jonathan")
    if nt.startswith(qt):
        return 0.9 if len(qt) > 1 else 0.8
    return ratio(qt, nt)


def score(query, first, last):
    q = normalise(query)
    full = normalise(f"{first} {last}")
    if not q or not full:
        return 0.0
    if q == full:
        return 1.0

    # Whole-string similarity, both name orders
    whole = max(ratio(q, full), ratio(q, normalise(f"{last} {first}")))

    # Token-wise: each query word matched to its best name word
    qtoks, ntoks = q.split(), full.split()
    tok = sum(max(token_score(qt, nt) for nt in ntoks) for qt in qtoks) / len(qtoks)
    # Small penalty if the query has fewer words than the name (e.g. surname only)
    coverage = min(1.0, len(qtoks) / len(ntoks))
    tok = tok * (0.85 + 0.15 * coverage)

    return max(whole, tok)


def search_db(db_path, query):
    hits = []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cols = {r[1] for r in conn.execute("PRAGMA table_info(riders)")}
        if not cols:
            return hits  # no riders table - not a league DB
        cat = "race_category" if "race_category" in cols else "NULL"
        rows = conn.execute(
            f"SELECT race_number, firstname, surname, club_name, {cat} AS cat FROM riders"
        ).fetchall()
        conn.close()
    except sqlite3.Error as e:
        print(f"  ! skipping {db_path.name}: {e}", file=sys.stderr)
        return hits

    for r in rows:
        s = score(query, r["firstname"], r["surname"])
        hits.append((s, db_path.stem, r))
    return hits


def main():
    ap = argparse.ArgumentParser(description="Fuzzy-find a rider across all league DBs.")
    ap.add_argument("name", nargs="+", help="Rider name (any order, typos OK)")
    ap.add_argument("--dir", default=".", help="Folder containing the .db files (default: current)")
    ap.add_argument("--threshold", type=float, default=0.65,
                    help="Minimum match score 0-1 (default 0.65)")
    ap.add_argument("--limit", type=int, default=10, help="Max results to show (default 10)")
    args = ap.parse_args()

    query = " ".join(args.name)
    dbs = sorted(Path(args.dir).glob("*.db"))
    if not dbs:
        sys.exit(f"No .db files found in {Path(args.dir).resolve()}")

    hits = []
    for db in dbs:
        hits.extend(search_db(db, query))

    hits = [h for h in hits if h[0] >= args.threshold]
    hits.sort(key=lambda h: (-h[0], h[1], h[2]["surname"] or ""))
    hits = hits[: args.limit]

    if not hits:
        print(f'No matches for "{query}" (threshold {args.threshold}). '
              f"Try a lower --threshold.")
        return

    rows = [(f"{s:.0%}", db, str(r["race_number"] or ""),
             f"{r['firstname'] or ''} {r['surname'] or ''}".strip(),
             r["club_name"] or "", r["cat"] or "")
            for s, db, r in hits]
    hdr = ("Match", "DB", "No.", "Name", "Club", "Cat")
    w = [max(len(hdr[i]), *(len(row[i]) for row in rows)) for i in range(len(hdr))]
    fmt = "  ".join(f"{{:<{x}}}" for x in w)

    print(f'Searching {len(dbs)} DB(s) for "{query}"\n')
    print(fmt.format(*hdr))
    print(fmt.format(*("-" * x for x in w)))
    for row in rows:
        print(fmt.format(*row))


if __name__ == "__main__":
    main()
