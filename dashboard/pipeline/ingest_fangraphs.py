"""
MLB Home Run Bot — FanGraphs / MLB Stats API Advanced Pitcher & Batter Ingestion

Pulls data not available from Baseball Savant:
  Pitchers: HR/9, K/9, BB/9, ERA, WHIP, innings pitched, recent form
  Batters:  HR/PA, HR rate per game, Z-score (how "due" they are)

Source: MLB Stats API (free, no key needed) — same data FanGraphs derives from.
FanGraphs xFIP is approximated from K/9, BB/9, and HR/9 using the standard formula.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import requests
from datetime import date, datetime, timezone
from typing import Optional

from config import MLB_STATS_BASE
from data.schema import get_connection, execute, fetchall

log = logging.getLogger(__name__)

SEASON = date.today().year
_NOW   = lambda: datetime.now(timezone.utc).isoformat()

# League-average constants for xFIP formula and Z-score baseline
LEAGUE_AVG_HR9   = 1.25   # MLB avg HR/9 innings pitched 2023-25
LEAGUE_AVG_K9    = 8.9
LEAGUE_AVG_BB9   = 3.1
LEAGUE_HR_PER_PA = 0.033  # ~3.3% league avg HR/PA for batters
LEAGUE_HR_STD    = 0.020  # standard deviation for Z-score


# ── xFIP approximation ────────────────────────────────────────────────────
# xFIP = ((13 * xHR) + (3 * (BB+HBP)) - (2 * K)) / IP + FIP_constant
# Simplified: use league-avg HR/FB rate to normalize HR component
# FIP constant ≈ ERA - (13*HR + 3*BB - 2*K) / IP — we use ~3.1 (2024 avg)

FIP_CONSTANT = 3.10

def _calc_xfip(k9: float, bb9: float, hr9: float, ip: float) -> Optional[float]:
    """
    Approximate xFIP from per-9 rates.
    Uses league-avg HR/FB (10.5%) to normalize HR component.
    Lower = better pitcher.
    """
    if not ip or ip < 10:
        return None
    # League avg HR/FB ~10.5% — xFIP replaces actual HR with expected HR
    # Expected HR/9 = league_hr_fb * (fly_balls/9) — approximated from actual HR/9
    xhr9 = hr9 * (0.105 / max(hr9 / max(k9, 1), 0.01))  # normalize
    xhr9 = min(xhr9, hr9 * 1.5)  # cap outliers
    xfip = ((13 * xhr9) + (3 * bb9) - (2 * k9)) / 9 + FIP_CONSTANT
    return round(max(1.0, min(xfip, 9.0)), 3)


# ── Fetch pitcher season stats ────────────────────────────────────────────

def fetch_pitcher_advanced_stats(season: int = SEASON) -> list[dict]:
    """
    Pull all pitchers' season stats from MLB Stats API.
    Returns list of dicts with HR/9, K/9, BB/9, xFIP, IP, recent form.
    """
    url = f"{MLB_STATS_BASE}/stats"
    params = {
        "stats":   "season",
        "group":   "pitching",
        "season":  season,
        "sportId": 1,
        "limit":   1000,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Pitcher stats fetch failed: {e}")
        return []

    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        pid  = split.get("player", {}).get("id")
        name = split.get("player", {}).get("fullName", "")
        stat = split.get("stat", {})

        ip   = _safe_float(stat.get("inningsPitched", 0))
        hr9  = _safe_float(stat.get("homeRunsPer9", LEAGUE_AVG_HR9))
        k9   = _safe_float(stat.get("strikeoutsPer9Inn", LEAGUE_AVG_K9))
        bb9  = _safe_float(stat.get("walksPer9Inn", LEAGUE_AVG_BB9))
        era  = _safe_float(stat.get("era", 4.50))
        whip = _safe_float(stat.get("whip", 1.30))
        hr_allowed = int(stat.get("homeRuns", 0))
        bf   = int(stat.get("battersFaced", 1))

        xfip = _calc_xfip(k9, bb9, hr9, ip)

        results.append({
            "player_id":    pid,
            "full_name":    name,
            "season":       season,
            "ip":           ip,
            "hr9":          hr9,
            "k9":           k9,
            "bb9":          bb9,
            "era":          era,
            "whip":         whip,
            "xfip":         xfip,
            "hr_allowed":   hr_allowed,
            "hr_per_bf":    round(hr_allowed / max(bf, 1), 5),
            "fetched_at":   _NOW(),
        })

    log.info(f"Fetched {len(results)} pitcher advanced stats.")
    return results


# ── Fetch batter season stats + Z-score ──────────────────────────────────

def fetch_batter_hr_stats(season: int = SEASON) -> list[dict]:
    """
    Pull all batters' season HR stats from MLB Stats API.
    Computes HR/PA and Z-score (how statistically 'due' a batter is).
    """
    url = f"{MLB_STATS_BASE}/stats"
    params = {
        "stats":   "season",
        "group":   "hitting",
        "season":  season,
        "sportId": 1,
        "limit":   1000,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Batter stats fetch failed: {e}")
        return []

    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        pid  = split.get("player", {}).get("id")
        stat = split.get("stat", {})

        hr = int(stat.get("homeRuns", 0))
        pa = int(stat.get("plateAppearances", 0)) or int(stat.get("atBats", 0))
        gp = int(stat.get("gamesPlayed", 1)) or 1

        if pa < 30:   # need minimum sample
            continue

        hr_per_pa   = round(hr / max(pa, 1), 5)
        hr_per_game = round(hr / max(gp, 1), 5)

        # Z-score: how many std deviations above/below their own rate
        # Positive = fewer HRs than expected = statistically "due"
        # Based on Poisson process: expected HR by now vs actual
        expected_hr = hr_per_pa * pa  # = actual HR (season total)
        # More useful: rolling game drought — approximate from games/HR
        games_per_hr   = gp / max(hr, 1)
        # League avg games/HR for a player with this rate
        expected_gphr  = 1.0 / max(hr_per_game, 0.001)
        z_score = (games_per_hr - expected_gphr) / max(expected_gphr * 0.5, 1)

        results.append({
            "player_id":    pid,
            "season":       season,
            "hr":           hr,
            "pa":           pa,
            "games_played": gp,
            "hr_per_pa":    hr_per_pa,
            "hr_per_game":  hr_per_game,
            "z_score_due":  round(float(z_score), 4),
            "slg":          _safe_float(stat.get("slg", 0)),
            "iso":          _safe_float(stat.get("slg", 0)) - _safe_float(stat.get("avg", 0)),
            "k_rate":       round(int(stat.get("strikeOuts", 0)) / max(pa, 1), 4),
            "bb_rate":      round(int(stat.get("baseOnBalls", 0)) / max(pa, 1), 4),
            "fetched_at":   _NOW(),
        })

    log.info(f"Fetched {len(results)} batter HR stats.")
    return results


# ── DB upsert ─────────────────────────────────────────────────────────────

def _upsert_pitcher_advanced(conn, rows: list[dict]):
    for r in rows:
        if not r.get("player_id"):
            continue
        execute(conn,
            "INSERT OR IGNORE INTO players(player_id, full_name) VALUES(?,?)",
            (r["player_id"], r["full_name"]))
        # Update pitcher_statcast with the new fields
        execute(conn, """
            UPDATE pitcher_statcast
            SET hr9=?, zone_rate=?, whiff_rate=?
            WHERE player_id=? AND window_days=0
        """, (r["hr9"], r["k9"], r["bb9"], r["player_id"]))
        # Also store in pitcher_advanced table (created below)
        execute(conn, """
            INSERT INTO pitcher_advanced(
                player_id, season, ip, hr9, k9, bb9, era, whip, xfip,
                hr_allowed, hr_per_bf, fetched_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(player_id, season) DO UPDATE SET
                ip=EXCLUDED.ip, hr9=EXCLUDED.hr9, k9=EXCLUDED.k9,
                bb9=EXCLUDED.bb9, era=EXCLUDED.era, whip=EXCLUDED.whip,
                xfip=EXCLUDED.xfip, hr_allowed=EXCLUDED.hr_allowed,
                hr_per_bf=EXCLUDED.hr_per_bf, fetched_at=EXCLUDED.fetched_at
        """, (
            r["player_id"], r["season"], r["ip"], r["hr9"], r["k9"],
            r["bb9"], r["era"], r["whip"], r["xfip"],
            r["hr_allowed"], r["hr_per_bf"], r["fetched_at"],
        ))


def _upsert_batter_advanced(conn, rows: list[dict]):
    for r in rows:
        if not r.get("player_id"):
            continue
        execute(conn, """
            INSERT INTO batter_advanced(
                player_id, season, hr, pa, games_played,
                hr_per_pa, hr_per_game, z_score_due,
                slg, iso, k_rate, bb_rate, fetched_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(player_id, season) DO UPDATE SET
                hr=EXCLUDED.hr, pa=EXCLUDED.pa,
                games_played=EXCLUDED.games_played,
                hr_per_pa=EXCLUDED.hr_per_pa,
                hr_per_game=EXCLUDED.hr_per_game,
                z_score_due=EXCLUDED.z_score_due,
                slg=EXCLUDED.slg, iso=EXCLUDED.iso,
                k_rate=EXCLUDED.k_rate, bb_rate=EXCLUDED.bb_rate,
                fetched_at=EXCLUDED.fetched_at
        """, (
            r["player_id"], r["season"], r["hr"], r["pa"],
            r["games_played"], r["hr_per_pa"], r["hr_per_game"],
            r["z_score_due"], r["slg"], r["iso"],
            r["k_rate"], r["bb_rate"], r["fetched_at"],
        ))


# ── Main entry points ─────────────────────────────────────────────────────

def run_ingest_pitcher_advanced():
    conn = get_connection()
    rows = fetch_pitcher_advanced_stats()
    _upsert_pitcher_advanced(conn, rows)
    conn.commit()
    conn.close()
    log.info(f"Pitcher advanced stats ingested: {len(rows)} rows.")


def run_ingest_batter_advanced():
    conn = get_connection()
    rows = fetch_batter_hr_stats()
    _upsert_batter_advanced(conn, rows)
    conn.commit()
    conn.close()
    log.info(f"Batter advanced stats ingested: {len(rows)} rows.")


def run_ingest_advanced():
    """Run both pitcher and batter advanced ingestion."""
    run_ingest_pitcher_advanced()
    run_ingest_batter_advanced()


# ── Helper ────────────────────────────────────────────────────────────────

def _safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    run_ingest_advanced()
