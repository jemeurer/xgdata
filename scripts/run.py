#!/usr/bin/env python3
"""
One-command launcher for xgdata.

Usage
-----
    python scripts/run.py              # build data (if needed) then start app
    python scripts/run.py --force      # force re-fetch then start app
    python scripts/run.py --app-only   # skip build, just start the app
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
PLAYER_FILE = _ROOT / "data" / "player_stats.parquet"
TEAM_FILE = _ROOT / "data" / "team_stats.parquet"


def _ensure_deps() -> None:
    req = _ROOT / "requirements.txt"
    if not req.exists():
        return
    print("Installing / verifying dependencies …")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)]
    )


def _build(force: bool) -> None:
    cmd = [sys.executable, str(_ROOT / "scripts" / "build_data.py")]
    if force:
        cmd.append("--force")
    subprocess.check_call(cmd)


def _start_app() -> None:
    subprocess.check_call(
        [sys.executable, "-m", "streamlit", "run", str(_ROOT / "src" / "app.py")]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run xgdata end-to-end")
    parser.add_argument("--force", action="store_true", help="Force re-fetch data")
    parser.add_argument("--app-only", action="store_true", help="Skip data build")
    parser.add_argument("--no-deps", action="store_true", help="Skip pip install")
    args = parser.parse_args()

    if not args.no_deps:
        _ensure_deps()

    if not args.app_only:
        if args.force or not (PLAYER_FILE.exists() and TEAM_FILE.exists()):
            print("Building data …")
            _build(force=args.force)
        else:
            print("Data files already exist — skipping build (use --force to rebuild).")

    if not (PLAYER_FILE.exists() and TEAM_FILE.exists()):
        print("ERROR: data files missing. Run without --app-only to build them first.")
        sys.exit(1)

    _start_app()


if __name__ == "__main__":
    main()
