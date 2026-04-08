#!/usr/bin/env python3
"""
Build pipeline for xgdata.

Fetches player and team statistics from understat.com for the configured
competitions, computes derived metrics, and writes:

    data/player_stats.parquet
    data/team_stats.parquet
    data/build_manifest.json

All raw API responses are cached under data/cache/ so subsequent runs are
fully local.

Usage
-----
    python scripts/build_data.py               # build all competitions
    python scripts/build_data.py --force       # ignore cache, re-fetch all
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

# Make sure src/ is importable regardless of working directory
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from src.data_loader import (
    CACHE_DIR,
    get_league_players,
    get_league_teams,
    load_manifest,
    save_manifest,
)
from src.stats import compute_player_stats, compute_team_stats

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — which competitions to build
# ---------------------------------------------------------------------------
# Each entry is (league_name, start_year).  Season 2023 → 2023/24.
# Understat supports: EPL, La_liga, Bundesliga, Serie_A, Ligue_1, RFPL
COMPETITIONS: list[tuple[str, int]] = [
    ("EPL", 2023),
    ("EPL", 2022),
    ("Bundesliga", 2023),
    ("Bundesliga", 2022),
    ("La_liga", 2023),
    ("La_liga", 2022),
    ("Serie_A", 2023),
    ("Ligue_1", 2023),
]

PLAYER_FILE = _ROOT / "data" / "player_stats.parquet"
TEAM_FILE = _ROOT / "data" / "team_stats.parquet"


def build(force: bool = False) -> None:
    if force:
        logger.info("--force flag set — clearing cache …")
        for f in CACHE_DIR.glob("*.parquet"):
            f.unlink()

    all_players: list[pd.DataFrame] = []
    all_teams: list[pd.DataFrame] = []

    for league, season in COMPETITIONS:
        logger.info("=== %s %d/%s ===", league, season, str(season + 1)[-2:])
        try:
            players = get_league_players(league, season)
            teams = get_league_teams(league, season)
        except Exception as exc:
            logger.error("  Failed for %s %d: %s", league, season, exc)
            continue

        logger.info("  Players: %d  |  Teams: %d", len(players), len(teams))
        all_players.append(players)
        all_teams.append(teams)

    if not all_players:
        logger.error("No data fetched — aborting.")
        sys.exit(1)

    logger.info("Computing player stats …")
    player_df = compute_player_stats(pd.concat(all_players, ignore_index=True))

    logger.info("Computing team stats …")
    team_df = compute_team_stats(pd.concat(all_teams, ignore_index=True))

    PLAYER_FILE.parent.mkdir(parents=True, exist_ok=True)
    player_df.to_parquet(PLAYER_FILE, index=False)
    team_df.to_parquet(TEAM_FILE, index=False)

    logger.info("Written %s (%d rows)", PLAYER_FILE.name, len(player_df))
    logger.info("Written %s (%d rows)", TEAM_FILE.name, len(team_df))

    manifest = {
        "built_at": datetime.datetime.utcnow().isoformat(),
        "competitions": [{"league": l, "season": s} for l, s in COMPETITIONS],
        "player_rows": len(player_df),
        "team_rows": len(team_df),
    }
    save_manifest(manifest)
    logger.info("Manifest saved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build xgdata Parquet files")
    parser.add_argument("--force", action="store_true", help="Clear cache before building")
    args = parser.parse_args()
    build(force=args.force)
