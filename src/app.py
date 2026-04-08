"""
xgdata — FBref-style football statistics browser powered by understat.com data.

Pages
-----
1. Player Stats   — filterable / searchable stat table with stat-group selector
                    and per-90 / raw toggle.
2. Team Stats     — league table sorted by points, with xG and pressing metrics.
3. Player Profile — detailed stat cards, percentile bars, radar chart, similar
                    players.
4. Stats Explained — reference documentation.

Run with:
    streamlit run src/app.py
"""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent.parent
PLAYER_FILE = _ROOT / "data" / "player_stats.parquet"
TEAM_FILE = _ROOT / "data" / "team_stats.parquet"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="xgdata | Football Statistics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------
def _inject_css() -> None:
    st.markdown(
        """
        <style>
        /* Slightly larger base font for readability */
        html, body, [class*="css"] { font-size: 15px; }

        /* Stat cards */
        .stat-card {
            background: #1e2230;
            border-radius: 10px;
            padding: 14px 18px;
            text-align: center;
        }
        .stat-card .value {
            font-size: 2rem;
            font-weight: 700;
            color: #e8eaf6;
        }
        .stat-card .label {
            font-size: 0.8rem;
            color: #9e9e9e;
            margin-top: 2px;
        }

        /* Team badge placeholder */
        .badge-circle {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 36px;
            height: 36px;
            border-radius: 50%;
            font-weight: 700;
            font-size: 0.8rem;
            color: #fff;
            margin-right: 8px;
        }

        /* Percentile bar label */
        .ptile-row { margin: 4px 0; }
        .ptile-label { font-size: 0.78rem; color: #9e9e9e; }

        /* Tighten dataframe padding */
        .stDataFrame td { padding: 4px 8px !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )


_inject_css()

# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

@st.cache_data
def load_player_data() -> pd.DataFrame:
    if not PLAYER_FILE.exists():
        return pd.DataFrame()
    return pd.read_parquet(PLAYER_FILE)


@st.cache_data
def load_team_data() -> pd.DataFrame:
    if not TEAM_FILE.exists():
        return pd.DataFrame()
    return pd.read_parquet(TEAM_FILE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LEAGUE_LABELS = {
    "EPL": "Premier League",
    "La_liga": "La Liga",
    "Bundesliga": "Bundesliga",
    "Serie_A": "Serie A",
    "Ligue_1": "Ligue 1",
    "RFPL": "Russian Premier League",
}

_LEAGUE_FLAGS = {
    "EPL": "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
    "La_liga": "🇪🇸",
    "Bundesliga": "🇩🇪",
    "Serie_A": "🇮🇹",
    "Ligue_1": "🇫🇷",
    "RFPL": "🇷🇺",
}


def _league_label(code: str) -> str:
    return f"{_LEAGUE_FLAGS.get(code, '')} {_LEAGUE_LABELS.get(code, code)}"


def _badge_color(name: str) -> str:
    """Deterministic hex colour derived from team name."""
    h = int(hashlib.md5(name.encode()).hexdigest()[:6], 16)
    r = ((h >> 16) & 0xFF) // 2 + 64
    g = ((h >> 8) & 0xFF) // 2 + 64
    b = (h & 0xFF) // 2 + 64
    return f"#{r:02x}{g:02x}{b:02x}"


def _badge_html(team: str) -> str:
    initials = "".join(w[0].upper() for w in team.split()[:2])
    color = _badge_color(team)
    return (
        f'<span class="badge-circle" style="background:{color}">'
        f"{initials}</span>"
    )


@st.cache_data(ttl=86400)
def _team_logo_url(team: str) -> Optional[str]:
    """Attempt Wikipedia thumbnail lookup; return None on failure."""
    import urllib.request
    import json as _json

    slug = team.replace(" ", "_")
    url = (
        f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}_F.C."
    )
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            data = _json.loads(r.read())
            return data.get("thumbnail", {}).get("source")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Stat group definitions
# ---------------------------------------------------------------------------

_STAT_GROUPS: dict[str, list[str]] = {
    "Summary": [
        "player_name", "team_title", "position", "games", "time",
        "goals", "xG", "assists", "xA", "npg", "npxG",
    ],
    "Shooting": [
        "player_name", "team_title", "position", "time",
        "goals", "shots", "shot_conversion", "xG", "npxG",
        "xG_overperf", "npxG_overperf",
        "goals_p90", "shots_p90", "xG_p90", "npxG_p90",
    ],
    "Passing / Chance Creation": [
        "player_name", "team_title", "position", "time",
        "assists", "xA", "key_passes",
        "assists_p90", "xA_p90", "key_passes_p90",
        "xA_plus_xG", "npxG_plus_xA",
    ],
    "Possession Involvement": [
        "player_name", "team_title", "position", "time",
        "xGChain", "xGBuildup",
        "xGChain_p90", "xGBuildup_p90",
    ],
    "Discipline": [
        "player_name", "team_title", "position", "games", "time",
        "yellow_cards", "red_cards",
        "yellow_cards_p90", "red_cards_p90",
    ],
}

# Columns that are "per-90" variants (rounded to 2 dp in display)
_P90_DISPLAY_COLS = {c for g in _STAT_GROUPS.values() for c in g if c.endswith("_p90")}


def _fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    """Round float columns for display."""
    out = df.copy()
    for col in out.select_dtypes(include="float"):
        if col in _P90_DISPLAY_COLS or col in ("shot_conversion", "xG_overperf",
                                                 "npxG_overperf", "xA_plus_xG",
                                                 "npxG_plus_xA"):
            out[col] = out[col].round(2)
        else:
            out[col] = out[col].round(2)
    return out


# ---------------------------------------------------------------------------
# Page: Player Stats
# ---------------------------------------------------------------------------

def page_players(players: pd.DataFrame) -> None:
    st.header("⚽ Player Statistics")

    if players.empty:
        st.error("No player data found. Run `python scripts/build_data.py` first.")
        return

    # --- Sidebar filters ---
    st.sidebar.subheader("Filters")

    leagues = sorted(players["league"].unique())
    sel_leagues = st.sidebar.multiselect(
        "League",
        options=leagues,
        default=leagues,
        format_func=_league_label,
    )

    seasons = sorted(players["season_label"].unique(), reverse=True)
    sel_seasons = st.sidebar.multiselect(
        "Season",
        options=seasons,
        default=[seasons[0]] if seasons else [],
    )

    positions = ["All"] + sorted(players["position"].dropna().unique())
    sel_pos = st.sidebar.selectbox("Position", options=positions)

    min_mins = st.sidebar.slider(
        "Min. minutes played", min_value=0, max_value=3000, value=450, step=90
    )

    search = st.sidebar.text_input("Search player name", "")

    stat_group = st.sidebar.selectbox("Stat group", list(_STAT_GROUPS.keys()))
    per90 = st.sidebar.toggle("Per-90 view", value=False)

    # --- Filter ---
    mask = (
        players["league"].isin(sel_leagues)
        & players["season_label"].isin(sel_seasons)
        & (players["time"] >= min_mins)
    )
    if sel_pos != "All":
        mask &= players["position"] == sel_pos
    if search:
        mask &= players["player_name"].str.contains(search, case=False, na=False)

    filtered = players[mask].copy()

    # --- Column selection ---
    cols = _STAT_GROUPS[stat_group]
    # Add league/season context
    display_cols = ["league", "season_label"] + [c for c in cols if c in filtered.columns]

    if per90:
        # Swap raw counting cols with their _p90 equivalents where available
        new_cols = []
        for c in display_cols:
            p90_c = f"{c}_p90"
            if p90_c in filtered.columns and c not in ("player_name", "team_title",
                                                        "position", "league", "season_label",
                                                        "games", "time"):
                new_cols.append(p90_c)
            else:
                new_cols.append(c)
        display_cols = list(dict.fromkeys(new_cols))  # deduplicate keeping order

    display_cols = [c for c in display_cols if c in filtered.columns]

    st.caption(f"Showing **{len(filtered):,}** players")
    st.dataframe(
        _fmt_df(filtered[display_cols]).reset_index(drop=True),
        use_container_width=True,
        height=600,
    )


# ---------------------------------------------------------------------------
# Page: Team Stats
# ---------------------------------------------------------------------------

def page_teams(teams: pd.DataFrame) -> None:
    st.header("🏆 Team Statistics")

    if teams.empty:
        st.error("No team data found. Run `python scripts/build_data.py` first.")
        return

    st.sidebar.subheader("Filters")

    leagues = sorted(teams["league"].unique())
    sel_league = st.sidebar.selectbox(
        "League", options=leagues, format_func=_league_label
    )

    seasons = sorted(teams[teams["league"] == sel_league]["season_label"].unique(), reverse=True)
    sel_season = st.sidebar.selectbox("Season", options=seasons)

    view = st.sidebar.radio(
        "View",
        ["League Table", "xG & Attacking", "Pressing & Deep"],
        horizontal=True,
    )

    filtered = teams[
        (teams["league"] == sel_league) & (teams["season_label"] == sel_season)
    ].copy()

    if filtered.empty:
        st.info("No data for this selection.")
        return

    if view == "League Table":
        filtered = filtered.sort_values("pts", ascending=False)
        filtered.insert(0, "Pos", range(1, len(filtered) + 1))
        cols = ["Pos", "title", "matches", "wins", "draws", "loses",
                "scored", "missed", "GD", "pts", "xpts", "xpts_diff"]
        cols = [c for c in cols if c in filtered.columns]
        _render_team_table(filtered, cols)

    elif view == "xG & Attacking":
        filtered = filtered.sort_values("xG", ascending=False)
        cols = ["title", "matches", "scored", "xG", "npxG",
                "xGA", "npxGA", "xGD", "npxGD", "deep", "deep_allowed"]
        cols = [c for c in cols if c in filtered.columns]
        _render_team_table(filtered, cols)

    else:  # Pressing
        filtered = filtered.sort_values("ppda_coef", ascending=True)
        cols = ["title", "pts", "ppda_coef", "ppda_allowed_coef", "deep", "deep_allowed", "deep_diff"]
        cols = [c for c in cols if c in filtered.columns]
        st.caption("PPDA = passes allowed per defensive action (lower = more pressing intensity)")
        _render_team_table(filtered, cols)


def _render_team_table(df: pd.DataFrame, cols: list[str]) -> None:
    display = df[cols].copy()
    for col in display.select_dtypes(include="float"):
        display[col] = display[col].round(2)
    st.dataframe(display.reset_index(drop=True), use_container_width=True, height=560)


# ---------------------------------------------------------------------------
# Page: Player Profile
# ---------------------------------------------------------------------------

_RADAR_GROUPS: dict[str, list[str]] = {
    "Shooting": ["goals_p90", "xG_p90", "npxG_p90", "shots_p90", "shot_conversion"],
    "Creating": ["assists_p90", "xA_p90", "key_passes_p90", "xGChain_p90", "xGBuildup_p90"],
}

_ALL_RADAR_COLS = [c for cols in _RADAR_GROUPS.values() for c in cols]


def page_player_profile(players: pd.DataFrame) -> None:
    st.header("👤 Player Profile")

    if players.empty:
        st.error("No player data. Run `python scripts/build_data.py` first.")
        return

    # --- Player selector ---
    sorted_names = sorted(players["player_name"].unique())
    player_name = st.selectbox("Select player", sorted_names)

    player_rows = players[players["player_name"] == player_name].sort_values(
        ["season_label"], ascending=False
    )

    if player_rows.empty:
        st.info("No data found.")
        return

    # If multiple seasons, let user pick
    season_options = player_rows["season_label"].tolist()
    if len(season_options) > 1:
        sel_season = st.selectbox("Season", season_options)
        row = player_rows[player_rows["season_label"] == sel_season].iloc[0]
    else:
        row = player_rows.iloc[0]

    league = row["league"]
    season = row["season"]

    # --- Header ---
    col1, col2 = st.columns([1, 4])
    with col1:
        st.markdown(
            f'<div style="font-size:4rem; text-align:center">'
            f'{_LEAGUE_FLAGS.get(league,"")}</div>',
            unsafe_allow_html=True,
        )
    with col2:
        st.subheader(f"{player_name}")
        st.caption(
            f"{row.get('position','—')}  ·  "
            f"{row.get('team_title','—')}  ·  "
            f"{_LEAGUE_LABELS.get(league, league)}  ·  "
            f"{row.get('season_label','—')}"
        )

    st.divider()

    # --- Stat cards ---
    tab_shoot, tab_create, tab_poss, tab_radar, tab_similar = st.tabs(
        ["⚽ Shooting", "🎯 Creating", "🔗 Possession", "📡 Radar", "👥 Similar Players"]
    )

    with tab_shoot:
        _stat_cards(row, [
            ("Goals", "goals"), ("xG", "xG"), ("npG", "npg"), ("npxG", "npxG"),
            ("Shots", "shots"), ("Conversion", "shot_conversion"),
            ("G – xG", "xG_overperf"), ("npG – npxG", "npxG_overperf"),
        ])
        st.divider()
        _ptile_bars(row, [
            ("Goals / 90", "goals_p90"), ("xG / 90", "xG_p90"),
            ("npxG / 90", "npxG_p90"), ("Shots / 90", "shots_p90"),
            ("Shot conversion", "shot_conversion"),
            ("xG overperf", "xG_overperf"),
        ])

    with tab_create:
        _stat_cards(row, [
            ("Assists", "assists"), ("xA", "xA"), ("Key Passes", "key_passes"),
            ("xA + xG", "xA_plus_xG"), ("npxG + xA", "npxG_plus_xA"),
        ])
        st.divider()
        _ptile_bars(row, [
            ("Assists / 90", "assists_p90"), ("xA / 90", "xA_p90"),
            ("Key Passes / 90", "key_passes_p90"),
        ])

    with tab_poss:
        _stat_cards(row, [
            ("xGChain", "xGChain"), ("xGBuildup", "xGBuildup"),
            ("Minutes", "time"), ("Games", "games"),
        ])
        st.divider()
        _ptile_bars(row, [
            ("xGChain / 90", "xGChain_p90"), ("xGBuildup / 90", "xGBuildup_p90"),
        ])

    with tab_radar:
        _radar_chart(players, row, player_name)

    with tab_similar:
        _similar_players(players, row, player_name)


def _stat_cards(row: pd.Series, specs: list[tuple[str, str]]) -> None:
    cols = st.columns(min(len(specs), 5))
    for i, (label, field) in enumerate(specs):
        val = row.get(field, float("nan"))
        if isinstance(val, float) and math.isnan(val):
            display = "—"
        elif isinstance(val, float):
            display = f"{val:.2f}"
        else:
            display = str(int(val)) if float(val) == int(float(val)) else f"{float(val):.2f}"
        with cols[i % len(cols)]:
            st.markdown(
                f'<div class="stat-card"><div class="value">{display}</div>'
                f'<div class="label">{label}</div></div>',
                unsafe_allow_html=True,
            )
    st.write("")  # spacing


def _ptile_bars(row: pd.Series, specs: list[tuple[str, str]]) -> None:
    """Render horizontal percentile bars for given stat columns."""
    st.caption("Percentile rank vs. league / season peers (min. 90 min)")
    data = {}
    for label, col in specs:
        ptile_col = f"{col}_ptile"
        val = row.get(ptile_col, float("nan"))
        if not (isinstance(val, float) and math.isnan(val)):
            data[label] = float(val)

    if not data:
        st.info("Percentile data not available (min. minutes threshold not met).")
        return

    chart_df = pd.DataFrame({"Percentile": data}).T
    st.bar_chart(chart_df, height=200)


def _radar_chart(players: pd.DataFrame, row: pd.Series, player_name: str) -> None:
    """Render a radar / spider chart using percentile scores."""
    league, season = row["league"], row["season"]
    cohort = players[(players["league"] == league) & (players["season"] == season)]

    # Up to 4 comparison players
    other_names = [n for n in sorted(cohort["player_name"].unique()) if n != player_name]
    compare = st.multiselect(
        "Compare with (up to 4)",
        options=other_names,
        max_selections=4,
    )

    radar_cols = _ALL_RADAR_COLS
    radar_labels = [c.replace("_p90", "/90").replace("_", " ").title() for c in radar_cols]

    def _get_ptile_values(r: pd.Series) -> list[float]:
        return [float(r.get(f"{c}_ptile", 0) or 0) for c in radar_cols]

    fig = go.Figure()
    palette = ["#4fc3f7", "#ef5350", "#66bb6a", "#ffa726", "#ab47bc"]

    for idx, name in enumerate([player_name] + compare):
        rows = cohort[cohort["player_name"] == name]
        if rows.empty:
            continue
        r = rows.iloc[0]
        vals = _get_ptile_values(r)
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]],
            theta=radar_labels + [radar_labels[0]],
            fill="toself",
            name=name,
            line_color=palette[idx % len(palette)],
            opacity=0.7,
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        height=500,
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font_color="#e0e0e0",
    )
    st.plotly_chart(fig, use_container_width=True)


def _similar_players(players: pd.DataFrame, row: pd.Series, player_name: str) -> None:
    """Find most similar players by Euclidean distance on percentile columns."""
    league, season = row["league"], row["season"]
    cohort = players[
        (players["league"] == league)
        & (players["season"] == season)
        & (players["time"] >= 450)
    ].copy()

    ptile_cols = [c for c in cohort.columns if c.endswith("_ptile")]
    if not ptile_cols:
        st.info("No percentile data available.")
        return

    ref = cohort[cohort["player_name"] == player_name]
    if ref.empty:
        st.info("Player not found in cohort.")
        return
    ref_vec = ref[ptile_cols].fillna(50).values[0]

    others = cohort[cohort["player_name"] != player_name].copy()
    others_mat = others[ptile_cols].fillna(50).values
    dists = np.linalg.norm(others_mat - ref_vec, axis=1)
    others["_dist"] = dists
    top = others.nsmallest(10, "_dist")[["player_name", "team_title", "position",
                                         "goals", "xG", "assists", "xA", "time"]]
    top.columns = ["Player", "Team", "Position", "Goals", "xG", "Assists", "xA", "Minutes"]
    st.dataframe(top.reset_index(drop=True), use_container_width=True)


# ---------------------------------------------------------------------------
# Page: Stats Explained
# ---------------------------------------------------------------------------

def page_stats_explained() -> None:
    st.header("📖 Stats Explained")

    st.markdown("""
## Data Source

All statistics are sourced from **[understat.com](https://understat.com)** via the
[understat](https://github.com/amosbastian/understat) Python library.

Understat covers **six leagues** from **2014/15** onwards:

| League | Code |
|--------|------|
| Premier League | `EPL` |
| La Liga | `La_liga` |
| Bundesliga | `Bundesliga` |
| Serie A | `Serie_A` |
| Ligue 1 | `Ligue_1` |
| Russian Premier League | `RFPL` |

---

## Player Statistics

| Column | Description |
|--------|-------------|
| `goals` | Total goals scored |
| `xG` | Expected goals — sum of shot xG values |
| `npg` | Non-penalty goals |
| `npxG` | Non-penalty expected goals |
| `assists` | Recorded assists |
| `xA` | Expected assists — xG of shots assisted |
| `shots` | Total shots |
| `key_passes` | Passes directly leading to a shot |
| `xGChain` | xG from all possessions the player was involved in |
| `xGBuildup` | xG from possessions excluding shots and key passes |
| `yellow_cards` | Yellow cards received |
| `red_cards` | Red cards received |
| `games` | Appearances |
| `time` | Minutes played |

### Derived Metrics

| Column | Formula |
|--------|---------|
| `shot_conversion` | `goals / shots` |
| `xG_overperf` | `goals − xG` (positive = outperforming xG) |
| `npxG_overperf` | `npg − npxG` |
| `xA_plus_xG` | `xA + xG` |
| `npxG_plus_xA` | `npxG + xA` |
| `*_p90` | Raw stat divided by 90-minute blocks (`time / 90`) |
| `*_ptile` | Percentile rank vs. same league/season peers with ≥ 90 min |

---

## Team Statistics

| Column | Description |
|--------|-------------|
| `pts` | Points |
| `wins / draws / loses` | Match outcomes |
| `scored / missed` | Goals for / against |
| `GD` | Goal difference |
| `xG / xGA` | Expected goals for / against |
| `npxG / npxGA` | Non-penalty xG for / against |
| `xGD` | xG difference (`xG − xGA`) |
| `npxGD` | Non-penalty xGD |
| `xpts` | Expected points based on xG model |
| `xpts_diff` | `xpts − pts` (positive = unlucky) |
| `deep` | Passes completed into the opponent's 18-yard box |
| `deep_allowed` | Same for opponent |
| `ppda_coef` | Passes Per Defensive Action — pressing intensity (lower = more pressing) |
| `ppda_allowed_coef` | Opponent PPDA |

---

## Notes on Understat xG Model

Understat uses a proprietary machine-learning xG model trained on
historical shot data.  It accounts for shot location, type (foot, header),
assist type, and game-state context.  It is **not** the Opta/StatsBomb model,
but it is well-regarded for its consistency across seasons and leagues.
    """)


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

def main() -> None:
    players = load_player_data()
    teams = load_team_data()

    with st.sidebar:
        st.title("⚽ xgdata")
        st.caption("Football stats powered by understat.com")
        st.divider()
        page = st.radio(
            "Navigate",
            ["Player Stats", "Team Stats", "Player Profile", "Stats Explained"],
            label_visibility="collapsed",
        )
        st.divider()

        if not players.empty:
            leagues_built = players["league"].unique()
            seasons_built = sorted(players["season_label"].unique(), reverse=True)
            st.caption(
                f"**{len(leagues_built)} leagues** · "
                f"**{len(seasons_built)} seasons**\n\n"
                f"Seasons: {', '.join(seasons_built)}"
            )
        else:
            st.warning(
                "No data found.\n\n"
                "Run `python scripts/build_data.py` to fetch data."
            )

    if page == "Player Stats":
        page_players(players)
    elif page == "Team Stats":
        page_teams(teams)
    elif page == "Player Profile":
        page_player_profile(players)
    elif page == "Stats Explained":
        page_stats_explained()


if __name__ == "__main__":
    main()
