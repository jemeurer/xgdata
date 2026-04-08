# xgdata

An FBref-style football statistics browser built on **[understat.com](https://understat.com)** data — a free, public source of xG-enriched player and team stats covering **six major leagues** (EPL, La Liga, Bundesliga, Serie A, Ligue 1, RFPL) from **2014/15 onwards**.

> Inspired by [footballdata](https://github.com/jemeurer/footballdata) but using a much broader data source: understat covers all major European leagues across a decade of seasons, compared to the handful of competitions available in StatsBomb Open Data.

---

## Features

- **Player Statistics** — filterable/searchable table with stat-group selector (Shooting, Creating, Possession Involvement, Discipline) and per-90 / raw toggle
- **Team Statistics** — league table view plus xG attacking metrics and pressing (PPDA) breakdowns
- **Player Profile** — stat cards, percentile bars vs. league/season peers, radar chart with up to 4 player overlays, and nearest-neighbour similar-player finder
- **Stats Explained** — reference documentation for every metric

## Data Source

All data is fetched from **understat.com** via the [`understat`](https://github.com/amosbastian/understat) Python library — **no API keys or credentials required**.

| League | Code |
|--------|------|
| Premier League | `EPL` |
| La Liga | `La_liga` |
| Bundesliga | `Bundesliga` |
| Serie A | `Serie_A` |
| Ligue 1 | `Ligue_1` |
| Russian Premier League | `RFPL` |

Available from season **2014/15** onwards.  Raw API responses are cached as Parquet files so repeat runs are fully local and fast.

---

## Quick Start

```bash
# 1. Clone & install dependencies
git clone https://github.com/jemeurer/xgdata.git
cd xgdata
pip install -r requirements.txt

# 2. One-command build + run
python scripts/run.py

# Or in two steps:
python scripts/build_data.py   # fetch data & write Parquet files (once)
streamlit run src/app.py        # start app at http://localhost:8501
```

### Options

```
python scripts/run.py --force       # re-fetch all data ignoring cache
python scripts/run.py --app-only    # skip build, start app directly
python scripts/build_data.py --force  # clear cache and re-fetch
```

---

## Pipeline

```
understat.com  (via understat Python library — no auth required)
       │
       ▼ src/data_loader.py
  get_league_players(league, season)  → data/cache/players_{league}_{season}.parquet
  get_league_teams(league, season)    → data/cache/teams_{league}_{season}.parquet
  get_league_results(league, season)  → data/cache/results_{league}_{season}.parquet
       │
       ▼ scripts/build_data.py  (orchestrator)
  src/stats.py
    compute_player_stats()  → one row per player per league/season
    compute_team_stats()    → one row per team per league/season
       │
       ▼
  data/player_stats.parquet   (primary output, git-ignored)
  data/team_stats.parquet     (primary output, git-ignored)
  data/build_manifest.json    (build metadata, git-ignored)
       │
       ▼  streamlit run src/app.py
  Interactive browser at http://localhost:8501
```

## Configured Competitions (default)

Edit `COMPETITIONS` in `scripts/build_data.py` to add or remove leagues/seasons.

| League | Seasons |
|--------|---------|
| EPL | 2022/23, 2023/24 |
| Bundesliga | 2022/23, 2023/24 |
| La Liga | 2022/23, 2023/24 |
| Serie A | 2023/24 |
| Ligue 1 | 2023/24 |

## Generated Artifacts (all git-ignored)

| Path | Description |
|------|-------------|
| `data/cache/players_{league}_{season}.parquet` | Raw player stats per league/season |
| `data/cache/teams_{league}_{season}.parquet` | Raw team stats per league/season |
| `data/cache/results_{league}_{season}.parquet` | Match results per league/season |
| `data/player_stats.parquet` | Final player stats with `_p90` and `_ptile` columns |
| `data/team_stats.parquet` | Final team stats with derived and percentile columns |
| `data/build_manifest.json` | Build metadata |
