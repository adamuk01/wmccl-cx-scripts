#!/usr/bin/env python3
"""
league_scoring.py
------------------
Shared scoring logic for WMCCL league tables — best-N scoring, AP (average
points) substitution, and category-grouping ("profile") rules.

This module is imported by BOTH export_league_tables.py (CSV) and
export_league_tables_html.py (HTML), so the two outputs can never disagree
on how points/averages/best-N totals are computed. Extracted 2026-09-19
from export_league_tables.py, behaviour-preserving (CSV output verified
byte-identical before/after).

Nothing in here touches the database or does any I/O — it's pure scoring
logic operating on plain Python data structures, so it's easy to reuse and
easy to unit-test.
"""

from typing import Dict, List, Optional, Tuple


AP_MARKER = 999


def safe_int(x, default=None):
    if x is None:
        return default
    s = str(x).strip()
    if s == "":
        return default
    try:
        return int(s)
    except ValueError:
        return default


def safe_float(x, default=None):
    if x is None:
        return default
    s = str(x).strip()
    if s == "":
        return default
    try:
        return float(s)
    except ValueError:
        return default


def compute_avg(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(vals) / len(vals)


def best_n_sum(vals: List[float], n: int) -> float:
    vals_sorted = sorted(vals, reverse=True)
    return float(sum(vals_sorted[:n]))


def rider_avg_this_season(rider_id: int,
                          results: Dict[Tuple[int, int], Tuple[Optional[float], int]],
                          upto_round: int) -> Optional[float]:
    vals = []
    for rnd in range(1, upto_round + 1):
        pts, is_ap = results.get((rider_id, rnd), (None, 0))
        if pts is None:
            continue
        if is_ap or pts == AP_MARKER:
            continue
        vals.append(float(pts))
    return compute_avg(vals)


def effective_points_for_round(rider_id: int,
                               rnd: int,
                               results: Dict[Tuple[int, int], Tuple[Optional[float], int]],
                               avg_pts: Optional[float]) -> Tuple[Optional[float], int]:
    """
    returns (effective_points, ap_flag)
    """
    pts, is_ap = results.get((rider_id, rnd), (None, 0))
    ap_flag = 1 if (is_ap or pts == AP_MARKER) else 0
    if ap_flag:
        return (avg_pts, 1)
    return (pts, 0)


def profile_tables(profile: str) -> Dict[str, List[str]]:
    """
    table_name -> list of race_category values to include (exact match).
    Special case: ["*"] means include all riders in DB.
    """
    if profile == "women":
        return {"Women_All": ["*"]}

    if profile == "u8":
        return {
            "U6M": ["U6M"],
            "U6F": ["U6F"],
            "U8M": ["U8M"],
            "U8F": ["U8F"],
        }

    if profile == "u10":
        return {
            "U10M": ["U10M"],
            "U10F": ["U10F"],
        }

    if profile == "u12":
        return {
            "U12M": ["U12M"],
            "U12F": ["U12F"],
        }

    if profile == "youth":
        return {
            "U14M": ["U14M"],
            "U14F": ["U14F"],
            "U16M": ["U16M"],
            "U16F": ["U16F"],
        }

    if profile == "seniors":
        return {
            "JunM": ["JunM"],
            "Sen_U23_M": ["SenM", "U23M"],

            # Masters groupings as requested
            "M40M": ["M40M", "M45M"],
            "M50M": ["M50M", "M55M"],
            "M60M": ["M60M", "M65M"],
            "M70M": ["M70M"],
        }

    if profile == "masters":
        return {
            # Masters groupings as requested
            "M40M": ["M40M", "M45M"],
            "M50M": ["M50M", "M55M"],
            "M60M": ["M60M", "M65M"],
            "M70M": ["M70M"],
        }

    raise SystemExit(f"❌ Unknown profile: {profile}")
