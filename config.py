"""
MLB Home Run Bot — Cloud Configuration
All secrets come from environment variables (set in Railway dashboard).
Never hardcode keys here.
"""

import os
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_DIR  = BASE_DIR / "logs"
EXPORT_DIR = BASE_DIR / "exports"
MODEL_DIR  = BASE_DIR / "models" / "saved"

# ── Database ───────────────────────────────────────────────────────────────
# Railway injects DATABASE_URL automatically when you add a Postgres plugin.
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── API Keys ───────────────────────────────────────────────────────────────
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

# ── Odds API settings ──────────────────────────────────────────────────────
ODDS_API_BASE  = "https://api.the-odds-api.com/v4"
ODDS_SPORT     = "baseball_mlb"
ODDS_MARKETS   = "batter_home_runs"
ODDS_REGIONS   = "us"
ODDS_BOOKMAKERS = ["fanduel", "draftkings", "betmgm", "caesars", "pointsbet"]

# ── MLB Stats API (free, no key) ───────────────────────────────────────────
MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"

# ── Baseball Savant (free, no key) ────────────────────────────────────────
SAVANT_BASE = "https://baseballsavant.mlb.com"

# ── Weather (Open-Meteo, free, no key) ────────────────────────────────────
WEATHER_BASE = "https://api.open-meteo.com/v1/forecast"

# ── Betting strategy ───────────────────────────────────────────────────────
DAILY_BUDGET = 150.0

STAKE_MAP = {
    "A":    30.0,
    "B":    30.0,
    "C":    20.0,
    "D":    20.0,
    "AB":   12.0,
    "AC":   10.0,
    "AD":    8.0,
    "BC":    8.0,
    "ABC":   6.0,
    "ABD":   4.0,
    "ACD":   4.0,
    "ABCD": 10.0,
}

# ── Model thresholds ───────────────────────────────────────────────────────
MIN_EDGE_PCT      = 0.03
MIN_PROJECTED_PA  = 3.8
MAX_SAME_GAME_LEGS = 2

# ── Scoring weights ────────────────────────────────────────────────────────
SCORE_WEIGHT_EDGE       = 0.45
SCORE_WEIGHT_TRUE_PROB  = 0.30
SCORE_WEIGHT_STABILITY  = 0.15
SCORE_WEIGHT_CORR_RISK  = 0.10

# ── Feature windows ────────────────────────────────────────────────────────
ROLLING_WINDOWS = [7, 14, 30]

# ── Dashboard ─────────────────────────────────────────────────────────────
DASHBOARD_HOST = "0.0.0.0"          # must be 0.0.0.0 on Railway
DASHBOARD_PORT = int(os.environ.get("PORT", 8000))
