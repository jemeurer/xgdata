#!/usr/bin/env python3

#i sused py 3.11.15 (conda: xgdata)
from __future__ import annotations
import argparse
import subprocess
import sys
import venv
from pathlib import Path

_ROOT = Path(__file__).parent.parent
_VENV = _ROOT / ".venv"
PLAYER_FILE = _ROOT / "data" / "player_stats.parquet"
TEAM_FILE   = _ROOT / "data" / "team_stats.parquet"

# ── venv bootstrap (must be first) ──────────────────────────────────────────
def _venv_python() -> Path:
    """Return the path to the venv's Python executable."""
    return _VENV / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")

def _in_venv() -> bool:
    return Path(sys.executable).resolve() == _venv_python().resolve()

def _bootstrap_venv() -> None:
    """Create venv + install deps, then re-exec this script inside it."""
    if not _VENV.exists():
        print("Creating virtual environment …")
        venv.create(str(_VENV), with_pip=True)

    py = str(_venv_python())
    print("Upgrading pip …")
    subprocess.check_call([py, "-m", "pip", "install", "-q", "--upgrade", "pip"])

    req = _ROOT / "requirements.txt"
    print("Installing / verifying dependencies …")
    subprocess.check_call([py, "-m", "pip", "install", "-q", "-r", str(req)])

    raise SystemExit(subprocess.call([py] + sys.argv))
# ────────────────────────────────────────────────────────────────────────────

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
    parser.add_argument("--force",    action="store_true", help="Force re-fetch data")
    parser.add_argument("--app-only", action="store_true", help="Skip data build")
    args = parser.parse_args()

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