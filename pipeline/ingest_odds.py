"""
MLB Home Run Bot — Sportsbook Odds Ingestion
Uses The Odds API (https://the-odds-api.com) — free tier: 500 req/month.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import unicodedata
import requests
from datetime import datetime, timezone, date
from typing import Optional

from config import ODDS_API_KEY, ODDS_API_BASE, ODDS_SPORT, ODDS_MARKETS, ODDS_REGIONS, ODDS_BOOKMAKERS
from data.schema import get_connection, execute, fetchone, fetchall

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def american_to_implied(american_odds: int) -> float:
    """Convert American odds to implied probability (without vig)."""
    if american_odds >= 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def _normalize_name(name: str) -> str:
    """Strip accents, lowercase, remove punctuation for fuzzy matching."""
    # Normalize unicode (e.g. é → e, ñ → n, ü → u)
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase and strip non-alpha chars except spaces
    return " ".join(ascii_name.lower().split())


def _find_player_id(conn, name: str) -> Optional[int]:
    """
    Match a player name from the Odds API to a player_id in the DB.
    Tries in order:
      1. Exact match
      2. Accent-normalized exact match
      3. Last name + first initial match
      4. Last name only (if unique)
    """
    # 1. Exact match
    row = fetchone(conn, "SELECT player_id FROM players WHERE full_name=?", (name,))
    if row:
        return row["player_id"]

    # 2. Accent-normalized match — compare normalized versions
    norm_input = _normalize_name(name)
    all_players = fetchall(conn, "SELECT player_id, full_name FROM players", ())
    for p in all_players:
        if _normalize_name(p["full_name"]) == norm_input:
            return p["player_id"]

    # 3. Last name + first initial match (handles "R. Acuña Jr." vs "Ronald Acuna Jr.")
    parts = name.split()
    if len(parts) >= 2:
        last = _normalize_name(parts[-1])
        first_initial = _normalize_name(parts[0])[0] if parts[0] else ""
        matches = []
        for p in all_players:
            p_parts = p["full_name"].split()
            if len(p_parts) >= 2:
                p_last = _normalize_name(p_parts[-1])
                p_first = _normalize_name(p_parts[0])[0] if p_parts[0] else ""
                if p_last == last and p_first == first_initial:
                    matches.append(p["player_id"])
        if len(matches) == 1:
            return matches[0]

        # 4. Last name only (if unique)
        last_matches = [
            p["player_id"] for p in all_players
            if _normalize_name(p["full_name"].split()[-1]) == last
        ]
        if len(last_matches) == 1:
            return last_matches[0]

    return None


def _resolve_game_pk(conn, home_team: str, away_team: str, game_date: str) -> Optional[int]:
    """Try to match game by team name / abbr."""
    rows = fetchall(conn, """
        SELECT g.game_pk
        FROM games g
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE g.game_date=?
          AND (ht.name LIKE ? OR ht.abbr LIKE ? OR at.name LIKE ? OR at.abbr LIKE ?)
        LIMIT 1
    """, (game_date, f"%{home_team.split()[-1]}%", f"%{home_team}%",
          f"%{away_team.split()[-1]}%", f"%{away_team}%"))
    if rows:
        return rows[0]["game_pk"]
    return None


def fetch_and_store_odds(snapshot_type: str = "pre_lineup", game_date: Optional[date] = None):
    """Pull HR props from The Odds API and store in sportsbook_odds."""
    if not ODDS_API_KEY or ODDS_API_KEY == "YOUR_ODDS_API_KEY_HERE":
        log.warning("Odds API key not set. Skipping odds ingestion.")
        return

    if game_date is None:
        game_date = date.today()

    conn = get_connection()

    # Fetch events
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
        if e.get("commence_time", "")[:10] == game_date_str
    ]
    log.info(f"Found {len(today_events)} events for {game_date_str}")

    total_stored = 0
    total_matched = 0
    total_inserted = 0

    for event in today_events:
        event_id = event["id"]
        home_team = event.get("home_team", "")
        away_team = event.get("away_team", "")
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
                    player_name = outcome.get("description") or outcome.get("name", "")
                    odds_val = outcome.get("price")
                    if not player_name or odds_val is None:
                        continue

                    total_stored += 1
                    player_id = _find_player_id(conn, player_name)

                    if not player_id:
                        # Insert as placeholder so we at least store the odds
                        fake_id = abs(hash(player_name)) % 9000000 + 1000000
                        try:
                            execute(conn, """
                                INSERT INTO players(player_id, full_name)
                                VALUES(?, ?)
                                ON CONFLICT DO NOTHING
                            """, (fake_id, player_name))
                            conn.commit()
                        except Exception:
                            pass
                        player_id = fake_id
                        log.debug(f"  No DB match for '{player_name}' — inserted as placeholder")
                    else:
                        total_matched += 1

                    implied = american_to_implied(int(odds_val))
                    try:
                        execute(conn, """
                            INSERT INTO sportsbook_odds(
                                game_pk, player_id, bookmaker, market, american_odds,
                                implied_prob, fetched_at, snapshot_type
                            ) VALUES(?,?,?,?,?,?,?,?)
                        """, (
                            game_pk, player_id, book_name,
                            "batter_home_runs", int(odds_val), implied, now, snapshot_type
                        ))
                        total_inserted += 1
                        count += 1
                    except Exception as e:
                        log.warning(f"Failed to insert odds for {player_name}: {e}")

        conn.commit()
        log.info(f"  {away_team} @ {home_team}: stored {count} odds entries.")

    conn.close()
    log.info(f"Odds ingestion complete: {total_stored} players seen, {total_matched} matched to DB, {total_inserted} rows inserted.")


def get_best_odds_for_player(player_id: int, game_pk: Optional[int] = None,
                              snapshot_type: str = "pre_lineup") -> dict:
    """Return best (highest payout = lowest american_odds for favorites, highest for dogs) odds."""
    conn = get_connection()
    if game_pk:
        row = fetchone(conn, """
            SELECT bookmaker, american_odds, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND (game_pk=? OR game_pk IS NULL) AND snapshot_type=?
            ORDER BY american_odds DESC
            LIMIT 1
        """, (player_id, game_pk, snapshot_type))
    else:
        row = fetchone(conn, """
            SELECT bookmaker, american_odds, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND snapshot_type=?
            ORDER BY american_odds DESC
            LIMIT 1
        """, (player_id, snapshot_type))
    conn.close()
    return row if row else {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_and_store_odds(snapshot_type="morning")
