"""
MLB Home Run Bot — Sportsbook Odds Ingestion
Uses The Odds API (https://the-odds-api.com) — free tier: 500 req/month.

De-vig method: Additive normalization (Shin approximation).
For each player we collect YES (over) and NO (under) lines from the same book,
compute raw implieds, then divide each by their sum to strip the book's hold.
Only the vig-free YES implied probability is stored.
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


def american_to_implied_raw(american_odds: int) -> float:
    """Raw implied probability — still contains vig."""
    if american_odds >= 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def devig_implied(yes_odds: int, no_odds: int) -> float:
    """
    Additive de-vig (Shin method approximation).
    Given the Yes (over) and No (under) American odds for the same market,
    returns the vig-free probability for the Yes side.

    Raw implieds sum to >1.0 (the overround = book's hold).
    Dividing each by their sum removes the hold proportionally.
    """
    p_yes_raw = american_to_implied_raw(yes_odds)
    p_no_raw  = american_to_implied_raw(no_odds)
    total = p_yes_raw + p_no_raw
    if total <= 0:
        return p_yes_raw  # fallback
    return p_yes_raw / total


def devig_single(yes_odds: int) -> float:
    """
    When we only have the YES side, estimate vig-free prob by applying
    a conservative 10% hold reduction (typical for HR props on major books).
    This is less accurate than two-sided de-vig but better than no de-vig.
    """
    raw = american_to_implied_raw(yes_odds)
    # HR props typically have 10-15% hold; 12% is conservative middle
    return raw / 1.12


def _normalize_name(name: str) -> str:
    """Strip accents, lowercase, remove punctuation for fuzzy matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
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
    row = fetchone(conn, "SELECT player_id FROM players WHERE full_name=?", (name,))
    if row:
        return row["player_id"]

    norm_input = _normalize_name(name)
    all_players = fetchall(conn, "SELECT player_id, full_name FROM players", ())

    for p in all_players:
        if _normalize_name(p["full_name"]) == norm_input:
            return p["player_id"]

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

        last_matches = [
            p["player_id"] for p in all_players
            if _normalize_name(p["full_name"].split()[-1]) == last
        ]
        if len(last_matches) == 1:
            return last_matches[0]

    return None


def _resolve_game_pk(conn, home_team: str, away_team: str, game_date: str) -> Optional[int]:
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
    """Pull HR props from The Odds API and store de-vigged implied probabilities."""
    if not ODDS_API_KEY or ODDS_API_KEY == "YOUR_ODDS_API_KEY_HERE":
        log.warning("Odds API key not set. Skipping odds ingestion.")
        return

    if game_date is None:
        game_date = date.today()

    conn = get_connection()

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

                # Build YES/NO lookup per player for two-sided de-vig
                # Outcomes typically have "Over" (yes) and "Under" (no) sides
                yes_odds_map: dict[str, int] = {}
                no_odds_map:  dict[str, int] = {}

                for outcome in market.get("outcomes", []):
                    player_name = outcome.get("description") or outcome.get("name", "")
                    odds_val = outcome.get("price")
                    side = (outcome.get("name") or "").lower()
                    point = outcome.get("point")
                    if not player_name or odds_val is None:
                        continue
                    # Only use 0.5 line (to HR or not) — most liquid standard bet
                    # Fall back to 1.5 if no 0.5 available
                    if point is not None and point not in (0.5, 1.5):
                        continue
                    # Prefer 0.5 line — skip 1.5 if we already have 0.5 for this player
                    if point == 1.5 and player_name in yes_odds_map:
                        continue
                    if "over" in side or side == player_name.lower():
                        yes_odds_map[player_name] = int(odds_val)
                    elif "under" in side or "no" in side:
                        no_odds_map[player_name] = int(odds_val)
                    else:
                        # HR props sometimes only list YES side
                        yes_odds_map[player_name] = int(odds_val)

                # Now store with de-vigged implied prob
                for player_name, yes_odds in yes_odds_map.items():
                    total_stored += 1

                    # De-vig
                    if player_name in no_odds_map:
                        implied = devig_implied(yes_odds, no_odds_map[player_name])
                        log.debug(f"  Two-sided de-vig {player_name}: raw={american_to_implied_raw(yes_odds):.3f} -> fair={implied:.3f}")
                    else:
                        implied = devig_single(yes_odds)
                        log.debug(f"  Single-sided de-vig {player_name}: raw={american_to_implied_raw(yes_odds):.3f} -> fair={implied:.3f}")

                    player_id = _find_player_id(conn, player_name)
                    if not player_id:
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
                    else:
                        total_matched += 1

                    try:
                        execute(conn, """
                            INSERT INTO sportsbook_odds(
                                game_pk, player_id, bookmaker, market, american_odds,
                                implied_prob, fetched_at, snapshot_type
                            ) VALUES(?,?,?,?,?,?,?,?)
                        """, (
                            game_pk, player_id, book_name,
                            "batter_home_runs", yes_odds, implied, now, snapshot_type
                        ))
                        total_inserted += 1
                        count += 1
                    except Exception as e:
                        log.warning(f"Failed to insert odds for {player_name}: {e}")

        conn.commit()
        log.info(f"  {away_team} @ {home_team}: stored {count} de-vigged odds entries.")

    conn.close()
    log.info(f"Odds done: {total_stored} seen, {total_matched} DB-matched, {total_inserted} inserted.")


def get_best_odds_for_player(player_id: int, game_pk: Optional[int] = None,
                              snapshot_type: str = "pre_lineup") -> dict:
    conn = get_connection()
    if game_pk:
        row = fetchone(conn, """
            SELECT bookmaker, american_odds, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND (game_pk=? OR game_pk IS NULL) AND snapshot_type=?
            ORDER BY american_odds DESC LIMIT 1
        """, (player_id, game_pk, snapshot_type))
    else:
        row = fetchone(conn, """
            SELECT bookmaker, american_odds, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND snapshot_type=?
            ORDER BY american_odds DESC LIMIT 1
        """, (player_id, snapshot_type))
    conn.close()
    return row if row else {}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetch_and_store_odds(snapshot_type="morning")
