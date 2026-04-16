"""
MLB Home Run Bot — Feature Engineering
Builds the feature vector for each batter in each game.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import json
from datetime import date
from typing import Optional
import sqlite3

from data.schema import get_connection, execute, fetchall

log = logging.getLogger(__name__)


def _get(row, key, default=None):
    try:
        v = row[key]
        return default if v is None else v
    except (IndexError, KeyError):
        return default


def get_batter_features(conn, player_id: int) -> dict:
    """Aggregate Statcast features for a batter across rolling windows."""
    feats = {}

    for window in [0, 7, 14, 30]:  # 0 = season
        label = f"season" if window == 0 else f"last_{window}d"
        rows = fetchall(conn, """
            SELECT * FROM batter_statcast
            WHERE player_id=? AND window_days=? AND pitcher_hand='overall'
            ORDER BY fetched_at DESC LIMIT 1
        """, (player_id, window))
        row = rows[0] if rows else None

        if row:
            feats[f"barrel_rate_{label}"]    = _get(row, "barrel_rate")
            feats[f"avg_ev_{label}"]          = _get(row, "avg_exit_velocity")
            feats[f"launch_angle_{label}"]    = _get(row, "avg_launch_angle")
            feats[f"hard_hit_{label}"]        = _get(row, "hard_hit_rate")
            feats[f"sweet_spot_{label}"]      = _get(row, "sweet_spot_rate")
            feats[f"xslg_{label}"]            = _get(row, "xslg")
            feats[f"xwoba_{label}"]           = _get(row, "xwoba")
            feats[f"k_rate_{label}"]          = _get(row, "strikeout_rate")
            feats[f"bb_rate_{label}"]         = _get(row, "walk_rate")
            feats[f"whiff_rate_{label}"]      = _get(row, "whiff_rate")
            feats[f"hr_per_pa_{label}"]       = _get(row, "hr_per_pa")
            feats[f"pa_count_{label}"]        = _get(row, "pa_count")
            feats[f"bat_speed_{label}"]       = _get(row, "bat_speed_avg")
            feats[f"blast_rate_{label}"]      = _get(row, "blast_rate")
            feats[f"flyball_rate_{label}"]    = _get(row, "flyball_rate")
            feats[f"iso_{label}"]             = _get(row, "iso")
        else:
            # Fill with None for missing windows
            for col in ["barrel_rate","avg_ev","launch_angle","hard_hit","sweet_spot",
                        "xslg","xwoba","k_rate","bb_rate","whiff_rate","hr_per_pa",
                        "pa_count","bat_speed","blast_rate","flyball_rate","iso"]:
                feats[f"{col}_{label}"] = None

    # Advanced batter stats: HR/PA, HR/game, Z-score (from ingest_fangraphs)
    adv_rows = fetchall(conn, """
        SELECT * FROM batter_advanced
        WHERE player_id=?
        ORDER BY season DESC LIMIT 1
    """, (player_id,))
    adv = adv_rows[0] if adv_rows else None

    if adv:
        feats["hr_per_pa_season"]  = _get(adv, "hr_per_pa")
        feats["hr_per_game_season"]= _get(adv, "hr_per_game")
        feats["z_score_due"]       = _get(adv, "z_score_due")
        feats["season_hr"]         = _get(adv, "hr")
        feats["season_pa"]         = _get(adv, "pa")
        feats["season_games"]      = _get(adv, "games_played")
        # Fill ISO/k_rate from advanced if not already set from Statcast
        if not feats.get("iso_season"):
            feats["iso_season"]    = _get(adv, "iso")
        if not feats.get("k_rate_season"):
            feats["k_rate_season"] = _get(adv, "k_rate")
    else:
        for k in ["hr_per_pa_season","hr_per_game_season","z_score_due",
                  "season_hr","season_pa","season_games"]:
            feats[k] = None

    return feats


def get_pitcher_features(conn, pitcher_id: int) -> dict:
    """Get pitcher vulnerability features — Statcast + advanced (HR/9, xFIP, K/9, BB/9)."""
    feats = {}

    # Statcast EV/barrel data
    rows = fetchall(conn, """
        SELECT * FROM pitcher_statcast
        WHERE player_id=? AND window_days=0
        ORDER BY fetched_at DESC LIMIT 1
    """, (pitcher_id,))
    row = rows[0] if rows else None

    if row:
        feats["p_barrel_rate_allowed"] = _get(row, "barrel_rate_allowed")
        feats["p_hard_hit_allowed"]    = _get(row, "hard_hit_rate_allowed")
        feats["p_avg_ev_allowed"]      = _get(row, "avg_exit_velocity_allowed")
        feats["p_launch_angle_allowed"]= _get(row, "avg_launch_angle_allowed")
        feats["p_ff_usage"]            = _get(row, "ff_usage_pct")
        feats["p_sl_usage"]            = _get(row, "sl_usage_pct")
        feats["p_ch_usage"]            = _get(row, "ch_usage_pct")
        feats["p_avg_fb_velo"]         = _get(row, "avg_fb_velo")
        feats["p_days_rest"]           = _get(row, "days_rest")
    else:
        for k in ["p_barrel_rate_allowed","p_hard_hit_allowed","p_avg_ev_allowed",
                  "p_launch_angle_allowed","p_ff_usage","p_sl_usage","p_ch_usage",
                  "p_avg_fb_velo","p_days_rest"]:
            feats[k] = None

    # Advanced pitcher stats: HR/9, xFIP, K/9, BB/9 (from ingest_fangraphs)
    adv_rows = fetchall(conn, """
        SELECT * FROM pitcher_advanced
        WHERE player_id=?
        ORDER BY season DESC LIMIT 1
    """, (pitcher_id,))
    adv = adv_rows[0] if adv_rows else None

    if adv:
        feats["p_hr9"]      = _get(adv, "hr9")
        feats["p_xfip"]     = _get(adv, "xfip")
        feats["p_k9"]       = _get(adv, "k9")
        feats["p_bb9"]      = _get(adv, "bb9")
        feats["p_era"]      = _get(adv, "era")
        feats["p_whip"]     = _get(adv, "whip")
        feats["p_hr_per_bf"]= _get(adv, "hr_per_bf")
        feats["p_ip"]       = _get(adv, "ip")
    else:
        for k in ["p_hr9","p_xfip","p_k9","p_bb9","p_era","p_whip","p_hr_per_bf","p_ip"]:
            feats[k] = None

    return feats


def get_environment_features(conn, game_pk: int, batter_hand: str = "R") -> dict:
    """Get park + weather features."""
    feats = {}

    env_rows = fetchall(conn, """
        SELECT e.*, v.hr_factor_rhb, v.hr_factor_lhb, v.altitude_ft
        FROM environment_features e
        LEFT JOIN games g ON e.game_pk = g.game_pk
        LEFT JOIN venues v ON e.venue_id = v.venue_id
        WHERE e.game_pk=?
        ORDER BY e.fetched_at DESC LIMIT 1
    """, (game_pk,))
    env = env_rows[0] if env_rows else None

    if env:
        feats["temp_f"]           = _get(env, "temperature_f", 72.0)
        feats["humidity_pct"]     = _get(env, "humidity_pct", 50.0)
        feats["wind_speed_mph"]   = _get(env, "wind_speed_mph", 0.0)
        feats["wind_dir_label"]   = _get(env, "wind_dir_label", "calm")
        feats["roof_open"]        = _get(env, "roof_open", 1)
        feats["air_density_idx"]  = _get(env, "air_density_idx", 1.0)
        feats["altitude_ft"]      = _get(env, "altitude_ft", 0.0)
        hr_factor = _get(env, "hr_factor_rhb" if batter_hand != "L" else "hr_factor_lhb", 1.0)
        feats["park_hr_factor"]   = hr_factor
    else:
        feats.update({
            "temp_f": 72.0, "humidity_pct": 50.0, "wind_speed_mph": 0.0,
            "wind_dir_label": "calm", "roof_open": 1, "air_density_idx": 1.0,
            "altitude_ft": 0.0, "park_hr_factor": 1.0,
        })

    # Wind bonus: out_to_cf = positive, in_from_cf = negative
    wl = feats["wind_dir_label"]
    ws = feats["wind_speed_mph"]
    if wl == "out_to_cf":
        feats["wind_hr_bonus"] = round(ws * 0.004, 4)
    elif wl == "in_from_cf":
        feats["wind_hr_bonus"] = round(-ws * 0.003, 4)
    else:
        feats["wind_hr_bonus"] = 0.0

    # Temperature bonus (warm air = less dense)
    feats["temp_hr_bonus"] = round((feats["temp_f"] - 72) * 0.0008, 4)

    return feats


def get_opportunity_features(conn, player_id: int, game_pk: int) -> dict:
    """Lineup slot, PA projection, team totals."""
    feats = {}

    lineup_rows = fetchall(conn, """
        SELECT batting_order, confirmed, position
        FROM lineups WHERE player_id=? AND game_pk=?
        ORDER BY confirmed DESC LIMIT 1
    """, (player_id, game_pk))
    lineup = lineup_rows[0] if lineup_rows else None

    if lineup:
        slot = _get(lineup, "batting_order", 5)
        feats["batting_order"] = slot
        feats["confirmed_lineup"] = _get(lineup, "confirmed", 0)
        # PA projection: slots 1-3 ~4.3, 4-6 ~4.0, 7-9 ~3.7
        if slot <= 3:
            feats["projected_pa"] = 4.3
        elif slot <= 6:
            feats["projected_pa"] = 4.0
        else:
            feats["projected_pa"] = 3.7
    else:
        feats["batting_order"] = 5
        feats["confirmed_lineup"] = 0
        feats["projected_pa"] = 3.5

    # Pinch-hit risk (simplified: DH slot or very late order)
    feats["pinch_hit_risk"] = 1 if feats["batting_order"] >= 9 else 0

    return feats


def get_line_movement_feature(conn, player_id: int, game_pk: int) -> float:
    """
    Returns line_move_since_open: the change in implied probability from
    the earliest morning snapshot to the most recent pre_lineup snapshot.
    Positive = line moved toward YES (sharp/public money backing HR).
    Returns 0.0 if insufficient data.
    """
    opening_rows = fetchall(conn, """
        SELECT implied_prob FROM sportsbook_odds
        WHERE player_id=? AND (game_pk=? OR game_pk IS NULL)
          AND snapshot_type='morning'
        ORDER BY fetched_at ASC LIMIT 1
    """, (player_id, game_pk))

    current_rows = fetchall(conn, """
        SELECT implied_prob FROM sportsbook_odds
        WHERE player_id=? AND (game_pk=? OR game_pk IS NULL)
          AND snapshot_type IN ('pre_lineup', 'closing')
        ORDER BY fetched_at DESC LIMIT 1
    """, (player_id, game_pk))

    if opening_rows and current_rows:
        opening_prob = opening_rows[0]["implied_prob"]
        current_prob = current_rows[0]["implied_prob"]
        if opening_prob and current_prob:
            return round(float(current_prob) - float(opening_prob), 4)
    return 0.0


def build_feature_vector(conn: sqlite3.Connection, player_id: int, game_pk: int,
                          pitcher_id: Optional[int] = None, batter_hand: str = "R") -> dict:
    """Combine all feature groups into one dict for the model."""
    feats = {"player_id": player_id, "game_pk": game_pk}
    feats.update(get_batter_features(conn, player_id))
    if pitcher_id:
        feats.update(get_pitcher_features(conn, pitcher_id))
    feats.update(get_environment_features(conn, game_pk, batter_hand))
    feats.update(get_opportunity_features(conn, player_id, game_pk))
    feats["line_move_since_open"] = get_line_movement_feature(conn, player_id, game_pk)
    return feats


def build_all_feature_vectors(game_date: date | None = None) -> list[dict]:
    """
    Build feature vectors for every confirmed lineup batter on a given date.
    Returns list of feature dicts.
    """
    if game_date is None:
        game_date = date.today()

    conn = get_connection()

    lineups = fetchall(conn, """
        SELECT l.player_id, l.game_pk, l.batting_order, p.bats,
               pp.pitcher_id
        FROM lineups l
        JOIN games g ON l.game_pk = g.game_pk
        JOIN players p ON l.player_id = p.player_id
        LEFT JOIN probable_pitchers pp ON (pp.game_pk = l.game_pk
             AND pp.side != CASE WHEN l.team_id = g.home_team_id THEN 'home' ELSE 'away' END)
        WHERE g.game_date=?
        ORDER BY l.game_pk, l.batting_order
    """, (game_date.strftime("%Y-%m-%d"),))

    vectors = []
    for row in lineups:
        pid = row["player_id"]
        gp = row["game_pk"]
        pitcher_id = row["pitcher_id"]
        bhand = row["bats"] or "R"
        fv = build_feature_vector(conn, pid, gp, pitcher_id, bhand)
        vectors.append(fv)

    conn.close()
    log.info(f"Built {len(vectors)} feature vectors for {game_date}")
    return vectors
