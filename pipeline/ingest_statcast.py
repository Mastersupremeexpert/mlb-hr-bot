"""
MLB Home Run Bot — Baseball Savant / Statcast Ingestion
Pulls batter and pitcher Statcast leaderboards via Baseball Savant CSV exports.
No API key required.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import io
import logging
import requests
import pandas as pd
from datetime import date, datetime, timezone

from config import SAVANT_BASE, ROLLING_WINDOWS
from data.schema import get_connection

log = logging.getLogger(__name__)

_NOW = lambda: datetime.now(timezone.utc).isoformat()

SEASON = date.today().year

# ── Baseball Savant CSV URLs ──────────────────────────────────────────────

def _savant_batter_url(player_type: str, min_pa: int, start_date: str, end_date: str) -> str:
    """Build Baseball Savant leaderboard URL."""
    return (
        f"{SAVANT_BASE}/statcast_search/csv"
        f"?all=true&hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea={SEASON}%7C"
        f"&hfSit=&player_type={player_type}&hfOuts=&opponent=&pitcher_throws=&batter_stands=&"
        f"hfSA=&game_date_gt={start_date}&game_date_lt={end_date}&hfInfield=&team=&position=&"
        f"hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&"
        f"min_results=0&group_by=name&sort_col=xwoba&player_event_sort=api_p_release_speed&"
        f"sort_order=desc&min_pas={min_pa}&type=details&player_id="
    )


def _fetch_savant_leaderboard(player_type: str, start_date: str, end_date: str, min_pa: int = 5) -> pd.DataFrame:
    """Fetch Statcast leaderboard as DataFrame."""
    # Use the statcast leaderboard endpoint
    url = (
        f"{SAVANT_BASE}/leaderboard/expected_statistics"
        f"?type={player_type}&year={SEASON}&position=&team=&min={min_pa}&csv=true"
    )
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        return df
    except Exception as e:
        log.warning(f"Savant leaderboard fetch failed ({player_type}): {e}")
        return pd.DataFrame()


def _fetch_statcast_rolling(player_type: str, days_back: int) -> pd.DataFrame:
    """
    Pull rolling window stats from Baseball Savant expected stats leaderboard.
    Falls back to season data split by date range.
    """
    end = date.today()
    from datetime import timedelta
    start = end - timedelta(days=days_back)

    url = (
        f"{SAVANT_BASE}/leaderboard/expected_statistics"
        f"?type={player_type}&year={SEASON}&position=&team=&min=3"
        f"&startDate={start.strftime('%Y-%m-%d')}&endDate={end.strftime('%Y-%m-%d')}&csv=true"
    )
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        return df
    except Exception as e:
        log.warning(f"Savant rolling fetch ({player_type}, {days_back}d) failed: {e}")
        return pd.DataFrame()


def _fetch_statcast_batter_detail(player_type: str) -> pd.DataFrame:
    """Fetch barrel/exit velocity leaderboard."""
    url = (
        f"{SAVANT_BASE}/leaderboard/statcast"
        f"?type={player_type}&year={SEASON}&position=&team=&min=10&csv=true"
    )
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        return df
    except Exception as e:
        log.warning(f"Statcast detail fetch failed ({player_type}): {e}")
        return pd.DataFrame()


def _map_batter_row(row: pd.Series, player_id: int, window_days: int, pitcher_hand: str) -> dict:
    def g(col, default=None):
        val = row.get(col, default)
        if pd.isna(val) if val is not None else False:
            return default
        return val

    return {
        "player_id": player_id,
        "season": SEASON,
        "game_date": date.today().isoformat(),
        "avg_exit_velocity": g("avg_hit_speed") or g("exit_velocity_avg"),
        "avg_launch_angle": g("avg_hit_angle") or g("launch_angle_avg"),
        "barrel_rate": g("brl_percent") or g("barrel_batted_rate"),
        "hard_hit_rate": g("hard_hit_percent"),
        "sweet_spot_rate": g("sweet_spot_percent") or g("sweetspot_percent"),
        "bat_speed_avg": g("avg_bat_speed"),
        "blast_rate": g("blast_rate") or g("blasts_percent"),
        "flyball_rate": g("ev95percent") or None,  # proxy; real FB% from FG
        "groundball_rate": None,
        "pull_rate_airball": None,
        "strikeout_rate": g("k_percent"),
        "walk_rate": g("bb_percent"),
        "chase_rate": None,
        "whiff_rate": g("whiff_percent"),
        "contact_rate": None,
        "xslg": g("xslg"),
        "xwoba": g("xwoba") or g("est_woba"),
        "iso": g("iso") or None,
        "hr_per_pa": None,
        "pitcher_hand": pitcher_hand,
        "window_days": window_days,
        "pa_count": g("pa") or g("attempts"),
        "fetched_at": _NOW(),
    }


def _upsert_batter_statcast(conn, rows: list[dict]):
    cur = conn.cursor()
    for r in rows:
        # First ensure player exists
        cur.execute("""
            INSERT OR IGNORE INTO players(player_id, full_name) VALUES(?,?)
        """, (r["player_id"], ""))

        cur.execute("""
            INSERT INTO batter_statcast(
                player_id,season,game_date,avg_exit_velocity,avg_launch_angle,barrel_rate,
                hard_hit_rate,sweet_spot_rate,bat_speed_avg,blast_rate,flyball_rate,
                groundball_rate,pull_rate_airball,strikeout_rate,walk_rate,chase_rate,
                whiff_rate,contact_rate,xslg,xwoba,iso,hr_per_pa,pitcher_hand,
                window_days,pa_count,fetched_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            r["player_id"],r["season"],r["game_date"],r["avg_exit_velocity"],
            r["avg_launch_angle"],r["barrel_rate"],r["hard_hit_rate"],r["sweet_spot_rate"],
            r["bat_speed_avg"],r["blast_rate"],r["flyball_rate"],r["groundball_rate"],
            r["pull_rate_airball"],r["strikeout_rate"],r["walk_rate"],r["chase_rate"],
            r["whiff_rate"],r["contact_rate"],r["xslg"],r["xwoba"],r["iso"],r["hr_per_pa"],
            r["pitcher_hand"],r["window_days"],r["pa_count"],r["fetched_at"],
        ))
    conn.commit()


def _get_player_id_from_name(name: str, conn) -> int | None:
    cur = conn.cursor()
    row = cur.execute("SELECT player_id FROM players WHERE full_name=?", (name,)).fetchone()
    return row["player_id"] if row else None


def run_ingest_batter_statcast():
    """Pull Statcast batter data for season and rolling windows."""
    conn = get_connection()
    log.info("Fetching Statcast batter leaderboard (season)...")

    season_df = _fetch_savant_leaderboard("batter", "", "", 10)
    if season_df.empty:
        season_df = _fetch_statcast_batter_detail("batter")

    rows = []
    if not season_df.empty:
        for _, row in season_df.iterrows():
            pid_col = next((c for c in ["player_id","mlbam_id","pitcher","batter"] if c in row.index), None)
            if not pid_col:
                continue
            pid = int(row[pid_col]) if not pd.isna(row[pid_col]) else None
            if not pid:
                continue
            # Update player name
            name_col = next((c for c in ["last_name, first_name","player_name","name"] if c in row.index), None)
            name = str(row[name_col]) if name_col else ""
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO players(player_id,full_name) VALUES(?,?)", (pid, name))
            cur.execute("UPDATE players SET full_name=? WHERE player_id=?", (name, pid))
            rows.append(_map_batter_row(row, pid, 0, "overall"))

    _upsert_batter_statcast(conn, rows)
    log.info(f"Ingested {len(rows)} batter season rows.")

    # Rolling windows
    for days in ROLLING_WINDOWS:
        log.info(f"Fetching Statcast batter rolling {days}d...")
        df = _fetch_statcast_rolling("batter", days)
        roll_rows = []
        if not df.empty:
            for _, row in df.iterrows():
                pid_col = next((c for c in ["player_id","mlbam_id","batter"] if c in row.index), None)
                if not pid_col:
                    continue
                pid = int(row[pid_col]) if not pd.isna(row.get(pid_col, None)) else None
                if not pid:
                    continue
                roll_rows.append(_map_batter_row(row, pid, days, "overall"))
        _upsert_batter_statcast(conn, roll_rows)
        log.info(f"  Ingested {len(roll_rows)} rows for {days}d window.")

    conn.close()


def run_ingest_pitcher_statcast():
    """Pull Statcast pitcher data."""
    conn = get_connection()
    log.info("Fetching Statcast pitcher leaderboard...")
    df = _fetch_savant_leaderboard("pitcher", "", "", 5)
    if df.empty:
        df = _fetch_statcast_batter_detail("pitcher")

    cur = conn.cursor()
    count = 0
    if not df.empty:
        for _, row in df.iterrows():
            pid_col = next((c for c in ["player_id","mlbam_id","pitcher"] if c in row.index), None)
            if not pid_col:
                continue
            pid_val = row.get(pid_col)
            if pid_val is None or (isinstance(pid_val, float) and pd.isna(pid_val)):
                continue
            pid = int(pid_val)

            name_col = next((c for c in ["last_name, first_name","player_name","name"] if c in row.index), None)
            name = str(row[name_col]) if name_col else ""
            cur.execute("INSERT OR IGNORE INTO players(player_id,full_name) VALUES(?,?)", (pid, name))

            def g(col):
                v = row.get(col)
                return None if (v is None or (isinstance(v, float) and pd.isna(v))) else v

            cur.execute("""
                INSERT INTO pitcher_statcast(
                    player_id,season,game_date,barrel_rate_allowed,hard_hit_rate_allowed,
                    avg_exit_velocity_allowed,batter_hand,window_days,fetched_at,
                    avg_launch_angle_allowed,hr9,hr_per_bf,zone_rate,whiff_rate
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                pid, SEASON, date.today().isoformat(),
                g("brl_percent") or g("barrel_batted_rate"),
                g("hard_hit_percent"),
                g("avg_hit_speed") or g("exit_velocity_avg"),
                "overall", 0, _NOW(),
                g("avg_hit_angle") or g("launch_angle_avg"),
                None, None,
                g("zone_percent") or g("z_percent"),
                g("whiff_percent"),
            ))
            count += 1

    conn.commit()
    conn.close()
    log.info(f"Ingested {count} pitcher statcast rows.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_ingest_batter_statcast()
    run_ingest_pitcher_statcast()
