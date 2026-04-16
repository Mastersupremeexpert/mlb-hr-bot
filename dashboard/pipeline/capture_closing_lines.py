"""
MLB Home Run Bot — Closing Line Capture
Runs ~10 minutes before first pitch each day.
Snapshots the current odds for every player in today's bet_recommendations
so we can calculate Closing Line Value (CLV) in the backtest.

CLV = closing_implied_prob - our_locked_implied_prob
Positive CLV = we got better odds than the market closed at = we have edge.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import requests
from datetime import date, datetime, timezone
from typing import Optional

from config import ODDS_API_KEY, ODDS_API_BASE
from data.schema import get_connection, execute, fetchall

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _american_to_implied(odds: int) -> float:
    if odds >= 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def fetch_closing_snapshot(game_date: Optional[date] = None) -> list[dict]:
    """
    Pull current HR prop odds from Odds API for all players
    who have a recommendation today. Stores as closing_lines.
    """
    if game_date is None:
        game_date = date.today()

    if not ODDS_API_KEY:
        log.warning("ODDS_API_KEY not set — skipping closing line capture.")
        return []

    conn = get_connection()
    date_str = game_date.strftime("%Y-%m-%d")

    # Get all player_ids with recommendations today
    recs = fetchall(conn, """
        SELECT DISTINCT rec.legs, rec.bet_type
        FROM bet_recommendations rec
        WHERE rec.bet_date = ?
    """, (date_str,))

    player_ids: set[int] = set()
    for rec in recs:
        legs = json.loads(rec.get("legs", "[]"))
        player_ids.update(legs)

    if not player_ids:
        log.info(f"No recommendations for {game_date} — nothing to capture.")
        conn.close()
        return []

    log.info(f"Capturing closing lines for {len(player_ids)} players on {game_date}...")

    # Fetch current odds from Odds API
    url = f"{ODDS_API_BASE}/sports/baseball_mlb/events"
    params = {
        "apiKey":  ODDS_API_KEY,
        "markets": "batter_home_runs",
        "regions": "us",
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        log.error(f"Odds API closing line fetch failed: {e}")
        conn.close()
        return []

    captured = []
    for event in events:
        for bookmaker in event.get("bookmakers", []):
            book_key = bookmaker.get("key", "")
            for market in bookmaker.get("markets", []):
                if market.get("key") != "batter_home_runs":
                    continue
                for outcome in market.get("outcomes", []):
                    pid = outcome.get("description_id") or outcome.get("player_id")
                    if not pid:
                        continue
                    try:
                        pid = int(pid)
                    except (ValueError, TypeError):
                        continue
                    if pid not in player_ids:
                        continue

                    odds = outcome.get("price")
                    if not odds:
                        continue
                    try:
                        odds = int(odds)
                    except (ValueError, TypeError):
                        continue

                    implied = round(_american_to_implied(odds), 6)
                    game_pk = event.get("id")  # event ID as proxy

                    execute(conn, """
                        INSERT INTO closing_lines(
                            game_pk, player_id, bookmaker,
                            closing_odds, closing_implied_prob, locked_at
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT DO NOTHING
                    """, (game_pk, pid, book_key, odds, implied, _now_utc()))

                    captured.append({
                        "player_id": pid,
                        "bookmaker": book_key,
                        "odds":      odds,
                        "implied":   implied,
                    })

    conn.commit()
    conn.close()
    log.info(f"Captured {len(captured)} closing line entries.")
    return captured


def run_closing_line_capture(game_date: Optional[date] = None):
    """Entry point — call this ~10 min before first pitch."""
    if game_date is None:
        game_date = date.today()
    log.info(f"=== CLOSING LINE CAPTURE: {game_date} ===")
    results = fetch_closing_snapshot(game_date)
    return {"date": game_date.strftime("%Y-%m-%d"), "captured": len(results)}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()
    d = date.fromisoformat(args.date) if args.date else None
    print(json.dumps(run_closing_line_capture(d), indent=2))
