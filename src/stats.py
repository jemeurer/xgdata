"""
Statistics computation for xgdata.

``compute_player_stats`` and ``compute_team_stats`` consume the raw DataFrames
produced by ``data_loader`` and return enriched DataFrames with:

* per-90 columns  (suffix ``_p90``)
* percentile-rank columns within the same league/season cohort (suffix ``_ptile``)
* derived rate / difference metrics

The understat source provides the following raw player fields:
    id, player_name, games, time (minutes), goals, xG, assists, xA, shots,
    key_passes, yellow_cards, red_cards, position, team_title, npg, npxG,
    xGChain, xGBuildup

And the following raw team fields (after normalisation in data_loader):
    id, title, wins, draws, loses, scored, missed, pts, xG, xGA,
    npxG, npxGA, ppda_coef, ppda_allowed_coef, deep, deep_allowed,
    xGD, npxGD, xpts, xpts_diff
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Minimum minutes threshold for per-90 / percentile calculations
MIN_MINUTES = 90

# ---------------------------------------------------------------------------
# Player statistics
# ---------------------------------------------------------------------------

# Columns that receive a per-90 variant (require time > 0)
_P90_COLS = [
    "goals",
    "xG",
    "npg",
    "npxG",
    "assists",
    "xA",
    "shots",
    "key_passes",
    "xGChain",
    "xGBuildup",
    "yellow_cards",
    "red_cards",
]

# Columns that receive a percentile-rank (higher = better, computed within league/season)
_PTILE_HIGHER = [
    "goals",
    "xG",
    "npg",
    "npxG",
    "assists",
    "xA",
    "shots",
    "key_passes",
    "xGChain",
    "xGBuildup",
    "goals_p90",
    "xG_p90",
    "npxG_p90",
    "assists_p90",
    "xA_p90",
    "shots_p90",
    "key_passes_p90",
    "xGChain_p90",
    "xGBuildup_p90",
    "shot_conversion",
    "xG_overperf",
    "npxG_overperf",
]

# Columns where *lower* value = better percentile (e.g. cards)
_PTILE_LOWER = [
    "yellow_cards_p90",
    "red_cards_p90",
]


def compute_player_stats(players_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich raw understat player DataFrame with derived and normalised columns.

    Parameters
    ----------
    players_df : pd.DataFrame
        Concatenation of ``get_league_players()`` calls across competitions.

    Returns
    -------
    pd.DataFrame
        One row per player per league/season with added ``_p90`` and ``_ptile``
        columns plus human-readable ``season_label``.
    """
    df = players_df.copy()

    # Ensure numeric types
    for col in _P90_COLS + ["time", "games"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # --- Derived rate/difference columns ------------------------------------
    df["shot_conversion"] = np.where(
        df["shots"] > 0, df["goals"] / df["shots"], np.nan
    )
    df["xG_overperf"] = df["goals"] - df["xG"]
    df["npxG_overperf"] = df["npg"] - df["npxG"]
    df["xA_plus_xG"] = df["xG"] + df["xA"]
    df["npxG_plus_xA"] = df["npxG"] + df["xA"]

    # --- Per-90 columns (only for players with >= MIN_MINUTES) --------------
    minutes = df["time"].clip(lower=1)  # avoid /0
    ninety = minutes / 90.0

    for col in _P90_COLS:
        if col in df.columns:
            df[f"{col}_p90"] = df[col] / ninety

    # --- Season label --------------------------------------------------------
    df["season_label"] = df["season"].apply(lambda y: f"{y}/{str(y + 1)[-2:]}")

    # --- Percentile ranks (within league × season cohort) -------------------
    qualified = df["time"] >= MIN_MINUTES

    for col in _PTILE_HIGHER:
        if col not in df.columns:
            continue
        df[f"{col}_ptile"] = np.nan
        for (league, season), grp in df[qualified].groupby(["league", "season"]):
            mask = (df["league"] == league) & (df["season"] == season) & qualified
            df.loc[mask, f"{col}_ptile"] = (
                df.loc[mask, col].rank(pct=True, na_option="keep") * 100
            ).round(1)

    for col in _PTILE_LOWER:
        if col not in df.columns:
            continue
        df[f"{col}_ptile"] = np.nan
        for (league, season), grp in df[qualified].groupby(["league", "season"]):
            mask = (df["league"] == league) & (df["season"] == season) & qualified
            df.loc[mask, f"{col}_ptile"] = (
                (1 - df.loc[mask, col].rank(pct=True, na_option="keep")) * 100
            ).round(1)

    return df


# ---------------------------------------------------------------------------
# Team statistics
# ---------------------------------------------------------------------------

_TEAM_P90_COLS = [
    "xG",
    "xGA",
    "npxG",
    "npxGA",
    "scored",
    "missed",
]

_TEAM_PTILE_HIGHER = [
    "pts",
    "scored",
    "xG",
    "npxG",
    "xpts",
    "deep",
    "xGD",
    "npxGD",
    "ppda_allowed_coef",  # high ppda_allowed means opponent presses less → good
]

_TEAM_PTILE_LOWER = [
    "loses",
    "missed",
    "xGA",
    "npxGA",
    "ppda_coef",  # lower PPDA = more pressing intensity = better
]


def compute_team_stats(teams_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich raw understat team DataFrame with derived and normalised columns.

    Parameters
    ----------
    teams_df : pd.DataFrame
        Concatenation of ``get_league_teams()`` calls across competitions.

    Returns
    -------
    pd.DataFrame
        One row per team per league/season.
    """
    df = teams_df.copy()

    num_cols = ["wins", "draws", "loses", "scored", "missed", "pts",
                "xG", "xGA", "npxG", "npxGA", "ppda_coef", "ppda_allowed_coef",
                "deep", "deep_allowed", "xGD", "npxGD", "xpts", "xpts_diff"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Derived columns
    df["GD"] = df["scored"] - df["missed"]
    df["matches"] = df["wins"] + df["draws"] + df["loses"]
    df["xGD"] = df.get("xGD", df["xG"] - df["xGA"])
    df["npxGD"] = df.get("npxGD", df["npxG"] - df["npxGA"])
    df["pts_per_game"] = df["pts"] / df["matches"].clip(lower=1)
    df["xpts_diff"] = df.get("xpts_diff", df["xpts"] - df["pts"])
    df["deep_diff"] = df["deep"] - df["deep_allowed"]

    # Season label
    df["season_label"] = df["season"].apply(lambda y: f"{y}/{str(y + 1)[-2:]}")

    # Percentile ranks within league × season
    for col in _TEAM_PTILE_HIGHER:
        if col not in df.columns:
            continue
        df[f"{col}_ptile"] = np.nan
        for (league, season), _ in df.groupby(["league", "season"]):
            mask = (df["league"] == league) & (df["season"] == season)
            df.loc[mask, f"{col}_ptile"] = (
                df.loc[mask, col].rank(pct=True, na_option="keep") * 100
            ).round(1)

    for col in _TEAM_PTILE_LOWER:
        if col not in df.columns:
            continue
        df[f"{col}_ptile"] = np.nan
        for (league, season), _ in df.groupby(["league", "season"]):
            mask = (df["league"] == league) & (df["season"] == season)
            df.loc[mask, f"{col}_ptile"] = (
                (1 - df.loc[mask, col].rank(pct=True, na_option="keep")) * 100
            ).round(1)

    return df
