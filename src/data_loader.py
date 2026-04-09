"""
Data loader for xgdata.

Fetches player and team statistics from understat.com via the `understat`
async Python library, caches results as Parquet files under data/cache/, and
exposes synchronous helpers consumed by the build pipeline.

Understat covers six leagues (EPL, La_liga, Bundesliga, Serie_A, Ligue_1,
RFPL) from the 2014/15 season onwards.  The season integer is the calendar
year in which the season *starts* — e.g. 2023 → 2023/24.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd

try:
    from understat import Understat
except ImportError as exc:  # pragma: no cover
    raise ImportError("understat package not installed – run: pip install understat") from exc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent.parent
CACHE_DIR = _ROOT / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Internal async helpers
# ---------------------------------------------------------------------------


async def _fetch_league_players(league: str, season: int) -> list[dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        u = Understat(session)
        return await u.get_league_players(league, season)


async def _fetch_league_teams(league: str, season: int) -> list[dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        u = Understat(session)
        return await u.get_teams(league, season)


async def _fetch_league_results(league: str, season: int) -> list[dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        u = Understat(session)
        return await u.get_league_results(league, season)


def _run(coro):
    """Run a coroutine, creating a new event loop when needed (script context)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_league_players(league: str, season: int) -> pd.DataFrame:
    """Return per-player stats for *league* / *season*, reading from cache if available.

    Parameters
    ----------
    league : str
        One of ``EPL``, ``La_liga``, ``Bundesliga``, ``Serie_A``, ``Ligue_1``, ``RFPL``.
    season : int
        Start year of the season (e.g. ``2023`` for 2023/24).

    Returns
    -------
    pd.DataFrame
        One row per player.  Numeric columns are cast to float.
    """
    cache_path = CACHE_DIR / f"players_{league}_{season}.parquet"
    if cache_path.exists():
        logger.info("Cache hit: %s", cache_path.name)
        return pd.read_parquet(cache_path)

    logger.info("Fetching players – %s %s …", league, season)
    raw: list[dict] = _run(_fetch_league_players(league, season))
    df = pd.DataFrame(raw)

    # Cast numeric columns
    num_cols = ["games", "time", "goals", "xG", "assists", "xA", "shots",
                "key_passes", "yellow_cards", "red_cards", "npg", "npxG",
                "xGChain", "xGBuildup"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["league"] = league
    df["season"] = season

    df.to_parquet(cache_path, index=False)
    logger.info("  → %d players cached to %s", len(df), cache_path.name)
    return df


def get_league_teams(league: str, season: int) -> pd.DataFrame:
    """Return per-team stats for *league* / *season*, reading from cache if available."""
    cache_path = CACHE_DIR / f"teams_{league}_{season}.parquet"
    if cache_path.exists():
        logger.info("Cache hit: %s", cache_path.name)
        return pd.read_parquet(cache_path)

    logger.info("Fetching teams – %s %s …", league, season)
    raw: list[dict] = _run(_fetch_league_teams(league, season))
    df = _normalize_team_rows(raw)
    df["league"] = league
    df["season"] = season

    df.to_parquet(cache_path, index=False)
    logger.info("  → %d teams cached to %s", len(df), cache_path.name)
    return df


def get_league_results(league: str, season: int) -> pd.DataFrame:
    """Return match-level results for *league* / *season*."""
    cache_path = CACHE_DIR / f"results_{league}_{season}.parquet"
    if cache_path.exists():
        logger.info("Cache hit: %s", cache_path.name)
        return pd.read_parquet(cache_path)

    logger.info("Fetching results – %s %s …", league, season)
    raw: list[dict] = _run(_fetch_league_results(league, season))
    df = pd.json_normalize(raw)
    df["league"] = league
    df["season"] = season

    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    df.to_parquet(cache_path, index=False)
    logger.info("  → %d matches cached to %s", len(df), cache_path.name)
    return df


# ---------------------------------------------------------------------------
# Internal normalisation helpers
# ---------------------------------------------------------------------------


def _normalize_team_rows(raw: list[dict]) -> pd.DataFrame:
    """Flatten the nested ``ppda`` / ``ppda_allowed`` / ``history`` fields."""
    rows = []
    for team in raw:
        row = {k: v for k, v in team.items() if k not in ("ppda", "ppda_allowed", "history")}

        # PPDA objects: {"att": N, "def": M}  → coefficient = att / def
        ppda = team.get("ppda") or {}
        ppda_att = float(ppda.get("att") or 0)
        ppda_def = float(ppda.get("def") or 1)
        row["ppda_coef"] = ppda_att / ppda_def if ppda_def else float("nan")

        ppda_a = team.get("ppda_allowed") or {}
        ppda_a_att = float(ppda_a.get("att") or 0)
        ppda_a_def = float(ppda_a.get("def") or 1)
        row["ppda_allowed_coef"] = ppda_a_att / ppda_a_def if ppda_a_def else float("nan")

        rows.append(row)

    df = pd.DataFrame(rows)
    num_cols = ["wins", "draws", "loses", "scored", "missed", "pts",
                "xG", "xGA", "npxG", "npxGA", "deep", "deep_allowed",
                "xGD", "npxGD", "xpts", "xpts_diff", "ppda_coef", "ppda_allowed_coef"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Build-manifest helpers
# ---------------------------------------------------------------------------

MANIFEST_PATH = _ROOT / "data" / "build_manifest.json"


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, default=str))
