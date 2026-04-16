"""
MLB Home Run Bot — Baseball Savant / Statcast Ingestion
Pulls batter and pitcher Statcast leaderboards via Baseball Savant CSV exports.
No API key required.

Verified column names (April 2026):
  expected_statistics: last_name, first_name | player_id | pa | est_slg | est_woba | …
  statcast leaderboard: player_id | avg_hit_speed | avg_hit_angle | brl_percent | ev95percent | …
  bat-tracking:         id | avg_bat_speed | blast_per_bat_contact | whiff_per_swing | …
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import logging
import requests
import pandas as pd
from datetime import date, datetime, timezone, timedelta

from config import SAVANT_BASE, ROLLING_WINDOWS
from data.schema import get_connection, execute

log = logging.getLogger(__name__)

_NOW = lambda: datetime.now(timezone.utc).isoformat()

SEASON = date.today().year

# ── Endpoint builders ─────────────────────────────────────────────────────

def _expected_stats_url(player_type: str, min_pa: int = 10,
                        start: str = "", end: str = "") -> str:
    """Baseball Savant expected statistics leaderboard (xwOBA, xSLG, …)."""
    base = (f"{SAVANT_BASE}/leaderboard/expected_statistics"
            f"?type={player_type}&year={SEASON}&position=&team=&min={min_pa}&csv=true")
    if start:
        base += f"&startDate={start}"
    if end:
        base += f"&endDate={end}"
    return base


def _statcast_url(player_type: str, min_bbe: int = 10) -> str:
    """Baseball Savant statcast leaderboard (EV, LA, barrels, …)."""
    return (f"{SAVANT_BASE}/leaderboard/statcast"
            f"?type={player_type}&year={SEASON}&position=&team=&min={min_bbe}&csv=true")


def _bat_tracking_url(min_swings: int = 10) -> str:
    """Baseball Savant bat-tracking leaderboard (bat speed, blast rate, whiff, …)."""
    return (f"{SAVANT_BASE}/leaderboard/bat-tracking"
            f"?type=batter&year={SEASON}&min={min_swings}&csv=true")


# ── pybaseball fallback ───────────────────────────────────────────────────
# Baseball Savant changes CSV column names ~2x per season without warning.
# If Savant returns an empty or malformed DataFrame, we fall back to
# pybaseball which maintains a stable API against the same data source.

def _pybaseball_batter_season() -> pd.DataFrame:
    """Fallback: pull Statcast leaderboard via pybaseball."""
    try:
        import pybaseball  # optional dependency
        log.info("Falling back to pybaseball for batter Statcast data...")
        df = pybaseball.statcast_batter_exitvelo_barrels(SEASON, minBBE=10)
        if df is not None and not df.empty:
            # Normalize column names to match our expected schema
            rename = {
                "player_id":        "player_id",
                "last_name":        "last_name",
                "first_name":       "first_name",
                "avg_hit_speed":    "avg_hit_speed",
                "avg_hit_angle":    "avg_hit_angle",
                "brl_percent":      "brl_percent",
                "ev95percent":      "ev95percent",
                "anglesweetspotpercent": "anglesweetspotpercent",
            }
            df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
            log.info(f"pybaseball fallback: {len(df)} rows")
            return df
    except ImportError:
        log.warning("pybaseball not installed — cannot use fallback.")
    except Exception as e:
        log.warning(f"pybaseball fallback failed: {e}")
    return pd.DataFrame()


def _pybaseball_pitcher_season() -> pd.DataFrame:
    """Fallback: pull pitcher Statcast data via pybaseball."""
    try:
        import pybaseball
        log.info("Falling back to pybaseball for pitcher Statcast data...")
        df = pybaseball.statcast_pitcher_exitvelo_barrels(SEASON, minBBE=5)
        if df is not None and not df.empty:
            return df
    except ImportError:
        pass
    except Exception as e:
        log.warning(f"pybaseball pitcher fallback failed: {e}")
    return pd.DataFrame()


# ── Generic fetch ─────────────────────────────────────────────────────────

SAVANT_MIN_ROWS = 50   # if Savant returns fewer rows than this, assume broken

def _fetch(url: str, label: str) -> pd.DataFrame:
    try:
        resp = requests.get(url, timeout=30,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        # Sanity check: Savant sometimes returns a 200 with an error HTML page
        if df.empty or len(df.columns) < 3:
            log.warning(f"{label}: suspiciously small response — may be broken")
            return pd.DataFrame()
        log.info(f"{label}: {len(df)} rows, cols={list(df.columns)}")
        return df
    except Exception as e:
        log.warning(f"{label} fetch failed: {e}")
        return pd.DataFrame()


# ── Column helpers ────────────────────────────────────────────────────────

def _g(row: pd.Series, *cols, default=None):
    """Return first non-null value among candidate column names."""
    for c in cols:
        if c in row.index:
            v = row[c]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return v
    return default


def _pid(row: pd.Series) -> int | None:
    """Extract player_id regardless of column name variant."""
    for c in ("player_id", "mlbam_id", "batter", "pitcher", "id"):
        if c in row.index:
            v = row[c]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                try:
                    return int(v)
                except (ValueError, TypeError):
                    pass
    return None


def _player_name(row: pd.Series) -> str:
    for c in ("last_name, first_name", "player_name", "name"):
        if c in row.index:
            v = row[c]
            if v and str(v) != "nan":
                return str(v)
    return ""


# ── Batter row mapper ─────────────────────────────────────────────────────

def _map_batter_row(row: pd.Series, ev_row: pd.Series | None,
                    bat_row: pd.Series | None,
                    window_days: int, pitcher_hand: str) -> dict:
    """
    Merge up to three source rows (expected-stats, statcast-EV, bat-tracking)
    into one batter_statcast record using verified column names.
    """
    g = lambda *cols, **kw: _g(row, *cols, **kw)
    ev = lambda *cols, **kw: (_g(ev_row, *cols, **kw) if ev_row is not None else None)
    bt = lambda *cols, **kw: (_g(bat_row, *cols, **kw) if bat_row is not None else None)

    return {
        "player_id":         _pid(row),
        "season":            SEASON,
        "game_date":         date.today().isoformat(),
        # Exit velocity & launch angle — from statcast leaderboard
        "avg_exit_velocity": ev("avg_hit_speed"),
        "avg_launch_angle":  ev("avg_hit_angle"),
        # Barrel rate — statcast leaderboard uses brl_percent
        "barrel_rate":       ev("brl_percent"),
        # Hard-hit — ev95percent (% of BBE at 95+ mph)
        "hard_hit_rate":     ev("ev95percent"),
        # Sweet spot — anglesweetspotpercent (8–32° launch angle)
        "sweet_spot_rate":   ev("anglesweetspotpercent"),
        # Bat tracking
        "bat_speed_avg":     bt("avg_bat_speed"),
        "blast_rate":        bt("blast_per_bat_contact"),
        # Flyball proxy: fbld (fly balls + line drives %)
        "flyball_rate":      ev("fbld"),
        "groundball_rate":   ev("gb"),
        # Pull/spray: not available in these endpoints
        "pull_rate_airball": None,
        # Plate discipline — bat-tracking whiff_per_swing
        "strikeout_rate":    None,
        "walk_rate":         None,
        "chase_rate":        None,
        "whiff_rate":        bt("whiff_per_swing"),
        "contact_rate":      None,
        # Expected stats — from expected_statistics endpoint
        "xslg":              g("est_slg"),
        "xwoba":             g("est_woba"),
        "iso":               None,
        "hr_per_pa":         None,
        "pitcher_hand":      pitcher_hand,
        "window_days":       window_days,
        "pa_count":          g("pa", "attempts"),
        "fetched_at":        _NOW(),
    }


# ── DB upsert ─────────────────────────────────────────────────────────────

def _upsert_batter_statcast(conn, rows: list[dict]):
    for r in rows:
        if not r.get("player_id"):
            continue
        execute(conn, """
            INSERT OR IGNORE INTO players(player_id, full_name) VALUES(?,?)
        """, (r["player_id"], ""))
        execute(conn, """
            INSERT INTO batter_statcast(
                player_id,season,game_date,avg_exit_velocity,avg_launch_angle,barrel_rate,
                hard_hit_rate,sweet_spot_rate,bat_speed_avg,blast_rate,flyball_rate,
                groundball_rate,pull_rate_airball,strikeout_rate,walk_rate,chase_rate,
                whiff_rate,contact_rate,xslg,xwoba,iso,hr_per_pa,pitcher_hand,
                window_days,pa_count,fetched_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT DO NOTHING
        """, (
            r["player_id"], r["season"], r["game_date"],
            r["avg_exit_velocity"], r["avg_launch_angle"], r["barrel_rate"],
            r["hard_hit_rate"], r["sweet_spot_rate"], r["bat_speed_avg"],
            r["blast_rate"], r["flyball_rate"], r["groundball_rate"],
            r["pull_rate_airball"], r["strikeout_rate"], r["walk_rate"],
            r["chase_rate"], r["whiff_rate"], r["contact_rate"],
            r["xslg"], r["xwoba"], r["iso"], r["hr_per_pa"],
            r["pitcher_hand"], r["window_days"], r["pa_count"], r["fetched_at"],
        ))


# ── Main batter ingest ────────────────────────────────────────────────────

def run_ingest_batter_statcast():
    """Pull Statcast batter data — season + rolling windows."""
    conn = get_connection()

    # ── Fetch all three season leaderboards (Savant-first, pybaseball fallback) ──
    xstats_df  = _fetch(_expected_stats_url("batter", min_pa=10),  "xStats-batter-season")
    ev_df      = _fetch(_statcast_url("batter", min_bbe=10),        "EV-batter-season")
    bat_df     = _fetch(_bat_tracking_url(min_swings=10),           "BatTracking-batter-season")

    # If EV leaderboard looks broken, fall back to pybaseball
    if len(ev_df) < SAVANT_MIN_ROWS:
        log.warning("Savant EV leaderboard looks broken — trying pybaseball fallback...")
        ev_df = _pybaseball_batter_season()

    # Build player_id → row maps for join
    def _id_map(df: pd.DataFrame) -> dict[int, pd.Series]:
        m = {}
        for _, row in df.iterrows():
            pid = _pid(row)
            if pid:
                m[pid] = row
        return m

    ev_map  = _id_map(ev_df)
    bat_map = _id_map(bat_df)

    rows = []
    for _, row in xstats_df.iterrows():
        pid = _pid(row)
        if not pid:
            continue
        # Update player name from any source
        name = _player_name(row)
        execute(conn,
            "INSERT OR IGNORE INTO players(player_id,full_name) VALUES(?,?)", (pid, name))
        if name:
            execute(conn,
                "UPDATE players SET full_name=? WHERE player_id=?", (name, pid))

        ev_row  = ev_map.get(pid)
        bat_row = bat_map.get(pid)
        rows.append(_map_batter_row(row, ev_row, bat_row, 0, "overall"))

    _upsert_batter_statcast(conn, rows)
    log.info(f"Ingested {len(rows)} batter season rows (Statcast).")

    # ── Rolling windows ──
    for days in ROLLING_WINDOWS:
        end_d   = date.today()
        start_d = end_d - timedelta(days=days)
        roll_df = _fetch(
            _expected_stats_url("batter", min_pa=3,
                                start=start_d.strftime("%Y-%m-%d"),
                                end=end_d.strftime("%Y-%m-%d")),
            f"xStats-batter-{days}d",
        )
        roll_rows = []
        for _, row in roll_df.iterrows():
            pid = _pid(row)
            if not pid:
                continue
            ev_row  = ev_map.get(pid)   # EV from season — best available for rolling
            bat_row = bat_map.get(pid)
            roll_rows.append(_map_batter_row(row, ev_row, bat_row, days, "overall"))
        _upsert_batter_statcast(conn, roll_rows)
        log.info(f"  {days}d window: {len(roll_rows)} rows ingested.")

    conn.close()


# ── Pitcher ingest ────────────────────────────────────────────────────────

def run_ingest_pitcher_statcast():
    """Pull Statcast pitcher data."""
    conn = get_connection()

    xstats_df = _fetch(_expected_stats_url("pitcher", min_pa=5), "xStats-pitcher-season")
    ev_df     = _fetch(_statcast_url("pitcher", min_bbe=10),      "EV-pitcher-season")

    # Fallback if Savant looks broken
    if len(ev_df) < SAVANT_MIN_ROWS:
        log.warning("Savant pitcher EV looks broken — trying pybaseball fallback...")
        ev_df = _pybaseball_pitcher_season()
    ev_map    = {}
    for _, row in ev_df.iterrows():
        pid = _pid(row)
        if pid:
            ev_map[pid] = row

    count = 0
    for _, row in xstats_df.iterrows():
        pid = _pid(row)
        if not pid:
            continue
        name = _player_name(row)
        execute(conn,
            "INSERT OR IGNORE INTO players(player_id,full_name) VALUES(?,?)", (pid, name))

        ev = ev_map.get(pid)
        g  = lambda *cols, **kw: _g(row, *cols, **kw)
        eg = lambda *cols, **kw: (_g(ev, *cols, **kw) if ev is not None else None)

        execute(conn, """
            INSERT INTO pitcher_statcast(
                player_id,season,game_date,barrel_rate_allowed,hard_hit_rate_allowed,
                avg_exit_velocity_allowed,batter_hand,window_days,fetched_at,
                avg_launch_angle_allowed,hr9,hr_per_bf,zone_rate,whiff_rate
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT DO NOTHING
        """, (
            pid, SEASON, date.today().isoformat(),
            eg("brl_percent"),
            eg("ev95percent"),
            eg("avg_hit_speed"),
            "overall", 0, _NOW(),
            eg("avg_hit_angle"),
            None, None,
            None,  # zone_rate not in these endpoints
            None,  # whiff not available for pitchers here
        ))
        count += 1

    conn.close()
    log.info(f"Ingested {count} pitcher statcast rows.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_ingest_batter_statcast()
    run_ingest_pitcher_statcast()
