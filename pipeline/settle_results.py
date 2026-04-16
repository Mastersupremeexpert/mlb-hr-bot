"""
MLB Home Run Bot — Auto Result Settlement
Runs after games end (typically 11 PM ET). Pulls yesterday's HR log from
MLB Stats API and auto-settles bet_results for any open recommendations.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import requests
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from config import MLB_STATS_BASE
from data.schema import get_connection, execute, fetchall, fetchone

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_hr_log(game_date: date) -> set[int]:
    """
    Pull all player_ids who hit at least one HR on game_date.
    Uses MLB Stats API game feed for each completed game.
    Returns a set of player_ids.
    """
    url = f"{MLB_STATS_BASE}/schedule"
    params = {
        "sportId": 1,
        "date": game_date.strftime("%Y-%m-%d"),
        "hydrate": "scoringPlays,decisions",
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Schedule fetch failed for {game_date}: {e}")
        return set()

    game_pks = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            if g.get("status", {}).get("abstractGameState") == "Final":
                game_pks.append(g["gamePk"])

    if not game_pks:
        log.warning(f"No final games found for {game_date}")
        return set()

    hr_players: set[int] = set()

    for gk in game_pks:
        try:
            feed_url = f"{MLB_STATS_BASE}/game/{gk}/feed/live"
            feed = requests.get(feed_url, timeout=20).json()
            plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
            for play in plays:
                result = play.get("result", {})
                if result.get("eventType") == "home_run":
                    batter = play.get("matchup", {}).get("batter", {})
                    pid = batter.get("id")
                    if pid:
                        hr_players.add(int(pid))
        except Exception as e:
            log.warning(f"Game feed failed for {gk}: {e}")
            continue

    log.info(f"HR players on {game_date}: {hr_players}")
    return hr_players


def settle_single(conn, rec: dict, hr_players: set[int]) -> bool:
    """
    Settle a single-leg recommendation.
    Returns True if settled, False if already settled or skipped.
    """
    legs = json.loads(rec.get("legs", "[]"))
    if len(legs) != 1:
        return False
    player_id = legs[0]
    won = player_id in hr_players

    # Calculate payout
    stake = rec.get("stake", 0.0) or 0.0
    odds  = rec.get("american_odds")
    if won and odds:
        from models.optimizer import american_to_decimal
        dec    = american_to_decimal(int(odds))
        payout = round(stake * dec, 2)
        profit = round(payout - stake, 2)
    else:
        payout = 0.0
        profit = round(-stake, 2)

    execute(conn, """
        INSERT INTO bet_results(recommendation_id, bet_date, settled_at, won, payout, profit)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, (rec["id"], rec["bet_date"], _now_utc(), 1 if won else 0, payout, profit))

    return True


def settle_parlay(conn, rec: dict, hr_players: set[int]) -> bool:
    """
    Settle a multi-leg parlay — all legs must win.
    """
    legs = json.loads(rec.get("legs", "[]"))
    if not legs:
        return False
    won = all(pid in hr_players for pid in legs)

    stake = rec.get("stake", 0.0) or 0.0
    odds  = rec.get("american_odds")
    if won and odds:
        from models.optimizer import american_to_decimal
        dec    = american_to_decimal(int(odds))
        payout = round(stake * dec, 2)
        profit = round(payout - stake, 2)
    else:
        payout = 0.0
        profit = round(-stake, 2)

    execute(conn, """
        INSERT INTO bet_results(recommendation_id, bet_date, settled_at, won, payout, profit)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, (rec["id"], rec["bet_date"], _now_utc(), 1 if won else 0, payout, profit))

    return True


def run_auto_settle(game_date: Optional[date] = None):
    """
    Main settlement job. Call this after games finish (11 PM ET).
    Defaults to yesterday's date since games are already complete.
    """
    if game_date is None:
        game_date = date.today() - timedelta(days=1)

    log.info(f"=== AUTO SETTLE: {game_date} ===")

    # Fetch who homered
    hr_players = fetch_hr_log(game_date)
    if not hr_players and game_date != date.today():
        log.warning("No HR data returned — check MLB API or game date.")

    conn = get_connection()

    # Find all unsettled recommendations for this date
    date_str = game_date.strftime("%Y-%m-%d")
    unsettled = fetchall(conn, """
        SELECT rec.*
        FROM bet_recommendations rec
        LEFT JOIN bet_results br ON br.recommendation_id = rec.id
        WHERE rec.bet_date = ? AND br.id IS NULL
    """, (date_str,))

    if not unsettled:
        log.info(f"No unsettled bets for {game_date}.")
        conn.close()
        return {"date": date_str, "settled": 0, "hr_players": list(hr_players)}

    settled_count = 0
    wins = 0

    for rec in unsettled:
        bet_type = rec.get("bet_type", "single")
        if bet_type == "single":
            ok = settle_single(conn, rec, hr_players)
        else:
            ok = settle_parlay(conn, rec, hr_players)

        if ok:
            settled_count += 1
            # Check if won for summary
            legs = json.loads(rec.get("legs", "[]"))
            if bet_type == "single" and legs and legs[0] in hr_players:
                wins += 1
            elif bet_type != "single" and all(p in hr_players for p in legs):
                wins += 1

    conn.commit()
    conn.close()

    log.info(f"Settled {settled_count} bets ({wins} wins) for {game_date}.")
    return {
        "date":        date_str,
        "settled":     settled_count,
        "wins":        wins,
        "hr_players":  list(hr_players),
    }


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args()
    d = date.fromisoformat(args.date) if args.date else None
    result = run_auto_settle(d)
    print(json.dumps(result, indent=2))
