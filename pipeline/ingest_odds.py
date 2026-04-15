"""
MLB Home Run Bot — Sportsbook Odds Ingestion
Uses The Odds API (https://the-odds-api.com) — free tier: 500 req/month.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import requests
from datetime import datetime, timezone, date
from typing import Optional

from config import ODDS_API_KEY, ODDS_API_BASE, ODDS_SPORT, ODDS_MARKETS, ODDS_REGIONS, ODDS_BOOKMAKERS
from data.schema import get_connection

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def american_to_implied(american_odds: int) -> float:
    """Convert American odds to implied probability (without vig)."""
    if american_odds >= 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def _find_player_id(conn, name: str) -> Optional[int]:
    """Try to match a player name to a player_id in the database."""
    cur = conn.cursor()
    # Exact match
    row = cur.execute("SELECT player_id FROM players WHERE full_name=?", (name,)).fetchone()
    if row:
        return row["player_id"]
    # Fuzzy: last name match
    parts = name.split()
    if len(parts) >= 2:
        last = parts[-1]
        rows = cur.execute(
            "SELECT player_id, full_name FROM players WHERE full_name LIKE ?",
            (f"%{last}%",)
        ).fetchall()
        if len(rows) == 1:
            return rows[0]["player_id"]
    return None


def _resolve_game_pk(conn, home_team: str, away_team: str, game_date: str) -> Optional[int]:
    """Try to match game by team name / abbr."""
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT g.game_pk
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE g.game_date=?
          AND (ht.name LIKE ? OR ht.abbr LIKE ? OR ht.name LIKE ? OR at.name LIKE ? OR at.abbr LIKE ?)
        LIMIT 1
    """, (game_date, f"%{home_team}%", f"%{home_team}%", f"%{away_team}%",
          f"%{away_team}%", f"%{away_team}%")).fetchall()
    if rows:
        return rows[0]["game_pk"]
    return None


def fetch_and_store_odds(snapshot_type: str = "pre_lineup", game_date: Optional[date] = None):
    """Pull HR props from The Odds API and store in sportsbook_odds."""
    if ODDS_API_KEY == "YOUR_ODDS_API_KEY_HERE":
        log.warning("Odds API key not set. Skipping odds ingestion.")
        return

    if game_date is None:
        game_date = date.today()

    conn = get_connection()
    cur = conn.cursor()

    # Fetch events first (to know game IDs)
    events_url = f"{ODDS_API_BASE}/sports/{ODDS_SPORT}/events"
    try:
        events_resp = requests.get(events_url, params={
            "apiKey": ODDS_API_KEY,
            "dateFormat": "iso",
        }, timeout=20)
        events_resp.raise_for_status()
        events = events_resp.json()
    except Exception as e:
        log.error(f"Events fetch failed: {e}")
        conn.close()
        return

    game_date_str = game_date.strftime("%Y-%m-%d")
    today_events = [
        e for e in events
        if e.get("commence_time","")[:10] == game_date_str
    ]

    log.info(f"Found {len(today_events)} events for {game_date_str}")

    for event in today_events:
        event_id = event["id"]
        home_team = event.get("home_team","")
        away_team = event.get("away_team","")
        game_pk = _resolve_game_pk(conn, home_team, away_team, game_date_str)

        # Fetch player props for this event
        props_url = f"{ODDS_API_BASE}/sports/{ODDS_SPORT}/events/{event_id}/odds"
        try:
            props_resp = requests.get(props_url, params={
                "apiKey": ODDS_API_KEY,
                "regions": ODDS_REGIONS,
                "markets": ODDS_MARKETS,
                "oddsFormat": "american",
                "bookmakers": ",".join(ODDS_BOOKMAKERS),
            }, timeout=20)
            props_resp.raise_for_status()
            props_data = props_resp.json()
        except Exception as e:
            log.warning(f"Props fetch failed for event {event_id}: {e}")
            continue

        now = _now_utc()
        count = 0
        for bookmaker in props_data.get("bookmakers", []):
            book_name = bookmaker["key"]
            for market in bookmaker.get("markets", []):
                if market.get("key") != "batter_home_runs":
                    continue
                for outcome in market.get("outcomes", []):
                    player_name = outcome.get("description") or outcome.get("name","")
                    odds_val = outcome.get("price")
                    if not player_name or odds_val is None:
                        continue

                    player_id = _find_player_id(conn, player_name)
                    if not player_id:
                        # Insert unknown player as placeholder
                        cur.execute(
                            "INSERT OR IGNORE INTO players(player_id, full_name) VALUES(?,?)",
                            (hash(player_name) % 9000000 + 1000000, player_name)
                        )
                        player_id = _find_player_id(conn, player_name)

                    implied = american_to_implied(int(odds_val))
                    cur.execute("""
                        INSERT INTO sportsbook_odds(
                            game_pk,player_id,bookmaker,market,american_odds,
                            implied_prob,fetched_at,snapshot_type
                        ) VALUES(?,?,?,?,?,?,?,?)
                    """, (
                        game_pk, player_id, book_name,
                        "batter_home_runs", int(odds_val), implied, now, snapshot_type
                    ))
                    count += 1

        log.info(f"  {home_team} vs {away_team}: stored {count} odds entries.")

    conn.commit()
    conn.close()


def get_best_odds_for_player(player_id: int, game_pk: Optional[int] = None,
                              snapshot_type: str = "pre_lineup") -> dict:
    """Return best (highest implied prob) odds for a player."""
    conn = get_connection()
    cur = conn.cursor()
    if game_pk:
        row = cur.execute("""
            SELECT bookmaker, american_odds, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND game_pk=? AND snapshot_type=?
            ORDER BY american_odds ASC
            LIMIT 1
        """, (player_id, game_pk, snapshot_type)).fetchone()
    else:
        row = cur.execute("""
            SELECT bookmaker, american_odds, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND snapshot_type=?
            ORDER BY american_odds ASC
            LIMIT 1
        """, (player_id, snapshot_type)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_and_store_odds(snapshot_type="morning")
