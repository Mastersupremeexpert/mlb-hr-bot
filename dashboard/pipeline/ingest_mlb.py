"""
MLB Home Run Bot — MLB Stats API Ingestion (Cloud Edition)
Works with both PostgreSQL (cloud) and SQLite (local).
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import logging
from datetime import date, datetime, timezone
from typing import Optional

from config import MLB_STATS_BASE
from data.schema import get_connection, execute, fetchall, fetchone

log = logging.getLogger(__name__)

VENUE_META = {
    3:    {"lat": 40.8296, "lon": -73.9262, "alt_ft": 55,   "roof": "open",        "lf": 318, "cf": 408, "rf": 314, "hr_rhb": 1.10, "hr_lhb": 1.12},
    4:    {"lat": 41.4962, "lon": -81.6852, "alt_ft": 660,  "roof": "open",        "lf": 325, "cf": 405, "rf": 325, "hr_rhb": 1.05, "hr_lhb": 1.03},
    5:    {"lat": 39.0840, "lon": -84.5073, "alt_ft": 550,  "roof": "open",        "lf": 328, "cf": 404, "rf": 325, "hr_rhb": 0.97, "hr_lhb": 0.98},
    12:   {"lat": 29.7573, "lon": -95.3555, "alt_ft": 42,   "roof": "retractable", "lf": 315, "cf": 409, "rf": 326, "hr_rhb": 0.98, "hr_lhb": 0.97},
    14:   {"lat": 34.0739, "lon": -118.2400,"alt_ft": 300,  "roof": "open",        "lf": 330, "cf": 400, "rf": 330, "hr_rhb": 0.94, "hr_lhb": 0.95},
    15:   {"lat": 39.7561, "lon": -104.9942,"alt_ft": 5200, "roof": "retractable", "lf": 347, "cf": 415, "rf": 350, "hr_rhb": 1.25, "hr_lhb": 1.20},
    17:   {"lat": 38.9622, "lon": -77.0082, "alt_ft": 58,   "roof": "open",        "lf": 336, "cf": 402, "rf": 335, "hr_rhb": 0.95, "hr_lhb": 0.97},
    19:   {"lat": 41.8299, "lon": -87.6338, "alt_ft": 600,  "roof": "open",        "lf": 355, "cf": 400, "rf": 353, "hr_rhb": 0.93, "hr_lhb": 0.94},
    22:   {"lat": 42.3467, "lon": -71.0972, "alt_ft": 20,   "roof": "open",        "lf": 310, "cf": 420, "rf": 302, "hr_rhb": 1.15, "hr_lhb": 0.90},
    31:   {"lat": 37.7786, "lon": -122.3893,"alt_ft": 0,    "roof": "open",        "lf": 339, "cf": 399, "rf": 309, "hr_rhb": 0.85, "hr_lhb": 0.88},
    2392: {"lat": 33.4453, "lon": -112.0667,"alt_ft": 1082, "roof": "retractable", "lf": 328, "cf": 407, "rf": 335, "hr_rhb": 1.05, "hr_lhb": 1.05},
    2394: {"lat": 33.8003, "lon": -117.8827,"alt_ft": 160,  "roof": "open",        "lf": 333, "cf": 400, "rf": 330, "hr_rhb": 1.00, "hr_lhb": 1.00},
    2395: {"lat": 32.7073, "lon": -117.1570,"alt_ft": 20,   "roof": "open",        "lf": 336, "cf": 396, "rf": 322, "hr_rhb": 0.90, "hr_lhb": 0.93},
    680:  {"lat": 37.3285, "lon": -121.9006,"alt_ft": 87,   "roof": "open",        "lf": 335, "cf": 399, "rf": 335, "hr_rhb": 0.92, "hr_lhb": 0.93},
    2889: {"lat": 47.5914, "lon": -122.3325,"alt_ft": 0,    "roof": "retractable", "lf": 331, "cf": 401, "rf": 326, "hr_rhb": 0.89, "hr_lhb": 0.91},
    4705: {"lat": 29.6857, "lon": -98.4946, "alt_ft": 650,  "roof": "retractable", "lf": 315, "cf": 404, "rf": 322, "hr_rhb": 1.08, "hr_lhb": 1.06},
    2626: {"lat": 25.7781, "lon": -80.2197, "alt_ft": 8,    "roof": "retractable", "lf": 344, "cf": 416, "rf": 335, "hr_rhb": 0.88, "hr_lhb": 0.89},
    5325: {"lat": 27.7683, "lon": -82.6534, "alt_ft": 12,   "roof": "retractable", "lf": 315, "cf": 404, "rf": 322, "hr_rhb": 0.96, "hr_lhb": 0.95},
    2757: {"lat": 43.6414, "lon": -79.3892, "alt_ft": 300,  "roof": "retractable", "lf": 328, "cf": 400, "rf": 328, "hr_rhb": 1.01, "hr_lhb": 1.01},
    1:    {"lat": 40.7571, "lon": -73.8458, "alt_ft": 20,   "roof": "open",        "lf": 335, "cf": 408, "rf": 330, "hr_rhb": 1.03, "hr_lhb": 1.05},
    2593: {"lat": 38.6226, "lon": -90.1928, "alt_ft": 440,  "roof": "open",        "lf": 336, "cf": 400, "rf": 335, "hr_rhb": 0.97, "hr_lhb": 0.99},
    2500: {"lat": 42.3467, "lon": -83.0490, "alt_ft": 600,  "roof": "retractable", "lf": 345, "cf": 420, "rf": 330, "hr_rhb": 0.88, "hr_lhb": 0.89},
    2503: {"lat": 39.9512, "lon": -75.1656, "alt_ft": 20,   "roof": "open",        "lf": 329, "cf": 401, "rf": 330, "hr_rhb": 1.00, "hr_lhb": 1.00},
    4169: {"lat": 39.2785, "lon": -76.6216, "alt_ft": 20,   "roof": "open",        "lf": 333, "cf": 400, "rf": 318, "hr_rhb": 1.00, "hr_lhb": 1.00},
    2406: {"lat": 44.9817, "lon": -93.2778, "alt_ft": 830,  "roof": "retractable", "lf": 339, "cf": 404, "rf": 328, "hr_rhb": 0.97, "hr_lhb": 0.96},
    2616: {"lat": 40.4468, "lon": -80.0058, "alt_ft": 730,  "roof": "open",        "lf": 325, "cf": 399, "rf": 320, "hr_rhb": 1.00, "hr_lhb": 1.00},
    32:   {"lat": 44.5013, "lon": -88.0622, "alt_ft": 594,  "roof": "open",        "lf": 335, "cf": 400, "rf": 335, "hr_rhb": 0.98, "hr_lhb": 0.98},
    2380: {"lat": 30.3232, "lon": -97.7397, "alt_ft": 489,  "roof": "retractable", "lf": 329, "cf": 407, "rf": 326, "hr_rhb": 1.02, "hr_lhb": 1.01},
}


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_schedule(game_date: Optional[date] = None) -> list:
    if game_date is None:
        game_date = date.today()
    url = f"{MLB_STATS_BASE}/schedule"
    params = {
        "sportId": 1,
        "date": game_date.strftime("%Y-%m-%d"),
        "hydrate": "team,venue,probablePitcher(note),lineScore",
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    games = []
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            games.append(g)
    return games


def _upsert_player(conn, person: dict):
    execute(conn, """
        INSERT INTO players(player_id, full_name, bats, throws, primary_position, updated_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(player_id) DO UPDATE SET full_name=EXCLUDED.full_name
    """, (
        person.get("id"),
        person.get("fullName", person.get("name", "")),
        person.get("batSide", {}).get("code", "") if isinstance(person.get("batSide"), dict) else "",
        person.get("pitchHand", {}).get("code", "") if isinstance(person.get("pitchHand"), dict) else "",
        person.get("primaryPosition", {}).get("abbreviation", "") if isinstance(person.get("primaryPosition"), dict) else "",
        _now_utc(),
    ))


def run_ingest_schedule(game_date: Optional[date] = None):
    if game_date is None:
        game_date = date.today()
    log.info(f"Ingesting schedule for {game_date}")
    games = fetch_schedule(game_date)
    if not games:
        log.warning("No games found.")
        return []

    conn = get_connection()
    try:
        for g in games:
            v = g.get("venue", {})
            vid = v.get("id")
            if vid:
                meta = VENUE_META.get(vid, {})
                execute(conn, """
                    INSERT INTO venues(venue_id,name,city,state,latitude,longitude,altitude_ft,
                                       roof_type,lf_dist,cf_dist,rf_dist,hr_factor_rhb,hr_factor_lhb)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(venue_id) DO UPDATE SET name=EXCLUDED.name
                """, (
                    vid, v.get("name",""), v.get("location",{}).get("city",""),
                    v.get("location",{}).get("state",""),
                    meta.get("lat"), meta.get("lon"), meta.get("alt_ft"),
                    meta.get("roof","open"), meta.get("lf"), meta.get("cf"), meta.get("rf"),
                    meta.get("hr_rhb",1.0), meta.get("hr_lhb",1.0),
                ))

            home = g.get("teams",{}).get("home",{}).get("team",{})
            away = g.get("teams",{}).get("away",{}).get("team",{})
            for team in [home, away]:
                if team.get("id"):
                    execute(conn, """
                        INSERT INTO teams(team_id,name,abbr,venue_id,league)
                        VALUES(?,?,?,?,?)
                        ON CONFLICT(team_id) DO UPDATE SET name=EXCLUDED.name
                    """, (team["id"], team.get("name",""), team.get("abbreviation",""),
                          vid, team.get("league",{}).get("name","")))

            execute(conn, """
                INSERT INTO games(game_pk,game_date,game_time_utc,home_team_id,away_team_id,venue_id,status)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(game_pk) DO UPDATE SET status=EXCLUDED.status
            """, (
                g["gamePk"], game_date.strftime("%Y-%m-%d"), g.get("gameDate",""),
                home.get("id"), away.get("id"), vid,
                g.get("status",{}).get("detailedState",""),
            ))

            for side in ["home","away"]:
                pp = g.get("teams",{}).get(side,{}).get("probablePitcher",{})
                if pp.get("id"):
                    _upsert_player(conn, pp)
                    execute(conn, """
                        INSERT INTO probable_pitchers(game_pk,pitcher_id,side,confirmed,fetched_at)
                        VALUES(?,?,?,?,?)
                        ON CONFLICT DO NOTHING
                    """, (g["gamePk"], pp["id"], side, 1, _now_utc()))

        conn.commit()
        log.info(f"Ingested {len(games)} games.")
    finally:
        conn.close()
    return games


def run_ingest_lineups(game_date: Optional[date] = None):
    if game_date is None:
        game_date = date.today()
    conn = get_connection()
    try:
        rows = fetchall(conn, "SELECT game_pk FROM games WHERE game_date=?",
                        (game_date.strftime("%Y-%m-%d"),))
        for row in rows:
            _fetch_lineups(conn, row["game_pk"])
        conn.commit()
        log.info(f"Lineups ingested for {len(rows)} games.")
    finally:
        conn.close()


def _fetch_lineups(conn, game_pk: int):
    url = f"{MLB_STATS_BASE}/game/{game_pk}/boxscore"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"Lineup fetch failed for {game_pk}: {e}")
        return
    data = resp.json()
    now = _now_utc()
    for side in ["home","away"]:
        team_data = data.get("teams",{}).get(side,{})
        team_id = team_data.get("team",{}).get("id")
        batters = team_data.get("battingOrder",[])
        players = team_data.get("players",{})
        for slot, pid in enumerate(batters, 1):
            pkey = f"ID{pid}"
            pdata = players.get(pkey, {})
            person = pdata.get("person",{})
            pos = pdata.get("position",{}).get("abbreviation","")
            if person.get("id"):
                _upsert_player(conn, person)
                execute(conn, """
                    INSERT INTO lineups(game_pk,player_id,team_id,batting_order,position,confirmed,fetched_at)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT DO NOTHING
                """, (game_pk, person["id"], team_id, slot, pos, 1, now))
