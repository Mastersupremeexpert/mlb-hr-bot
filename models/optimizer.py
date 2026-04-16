"""
MLB Home Run Bot — Parlay Combo Optimizer
Builds best 2-leg, 3-leg, 4-leg parlays from A/B/C/D candidates.
Only builds combos from already-approved singles.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from itertools import combinations
from datetime import date, datetime, timezone
from typing import Optional

from config import STAKE_MAP, MAX_SAME_GAME_LEGS, DAILY_BUDGET
from data.schema import get_connection, execute

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def american_to_decimal(american_odds: int) -> float:
    if american_odds >= 0:
        return (american_odds / 100) + 1.0
    else:
        return (100 / abs(american_odds)) + 1.0


def parlay_american_odds(legs: list[dict]) -> int:
    """Combine multiple legs into parlay American odds."""
    decimal = 1.0
    for leg in legs:
        odds = leg.get("best_odds")
        if not odds:
            # Use implied prob as fallback
            ip = leg.get("implied_prob", 0.15)
            dec = 1.0 / max(ip, 0.01)
        else:
            dec = american_to_decimal(int(odds))
        decimal *= dec
    # Convert combined decimal back to American
    if decimal >= 2.0:
        return int((decimal - 1.0) * 100)
    else:
        return int(-100 / (decimal - 1.0))


def combo_score(legs: list[dict]) -> float:
    """
    Score a parlay combination.
    combo_score = sum(player_scores) + price_bonus - corr_penalty - volatility_penalty
    """
    base = sum(leg.get("score", 0.0) for leg in legs)

    # Price bonus: better-than-consensus odds
    price_bonus = 0.0
    for leg in legs:
        edge = leg.get("edge", 0.0)
        if edge > 0.05:
            price_bonus += 0.02
        elif edge > 0.03:
            price_bonus += 0.01

    # Correlation penalty: same game
    game_pks = [leg.get("game_pk") for leg in legs]
    same_game_count = len(game_pks) - len(set(game_pks))
    corr_penalty = same_game_count * 0.10

    # Volatility penalty: low stability scores
    avg_stab = sum(leg.get("stability", 0.5) for leg in legs) / len(legs)
    vol_penalty = max(0.0, (0.6 - avg_stab) * 0.15)

    return round(base + price_bonus - corr_penalty - vol_penalty, 4)


def _same_game_count(legs: list[dict]) -> int:
    games = [l.get("game_pk") for l in legs]
    return len(games) - len(set(games))


def kelly_stake(cal_prob: float, american_odds: int,
                kelly_frac: float = 0.25, max_stake: float = 50.0,
                min_stake: float = 5.0) -> float:
    """
    Fractional Kelly criterion stake sizing.
    kelly_frac=0.25 = quarter Kelly (conservative, reduces variance).
    Formula: f* = (b*p - q) / b  where b = decimal_odds - 1, p = win_prob, q = 1 - p
    """
    dec = american_to_decimal(american_odds)
    b = dec - 1.0
    if b <= 0:
        return min_stake
    p = cal_prob
    q = 1.0 - p
    full_kelly = (b * p - q) / b
    if full_kelly <= 0:
        return 0.0  # negative Kelly = no bet
    frac_kelly = full_kelly * kelly_frac
    # Convert fraction to dollar amount (% of DAILY_BUDGET)
    stake = frac_kelly * DAILY_BUDGET
    return round(max(min_stake, min(max_stake, stake)), 2)


def build_parlays(candidates: list[dict]) -> dict:
    """
    Given A/B/C/D candidates, build ranked 2-leg, 3-leg, and 4-leg parlays.
    Only plays legs that passed the single bet filter.

    candidates: list of ranked dicts (A=candidates[0], B=candidates[1], etc.)
    Returns dict with keys: singles, parlay_2, parlay_3, parlay_4, bets
    """
    if not candidates:
        return {"singles": [], "parlay_2": [], "parlay_3": [], "parlay_4": []}

    labels = ["A", "B", "C", "D"]
    labeled = {c["rank_label"]: c for c in candidates if c.get("rank_label") in labels}

    # Singles
    singles = []
    for label in labels:
        if label in labeled:
            c = labeled[label]
            odds = c.get("best_odds")
            if odds:
                dec = american_to_decimal(odds)
                # Kelly sizing replaces hardcoded STAKE_MAP for singles
                stake = kelly_stake(c["cal_prob"], odds)
                if stake == 0.0:
                    continue  # negative Kelly = skip
                exp_payout = stake * dec
                ev = (c["cal_prob"] * exp_payout) - stake
                if ev <= 0:
                    continue  # only recommend positive EV singles
                singles.append({
                    "label": label,
                    "player_id": c["player_id"],
                    "player_name": c.get("player_name",""),
                    "game_pk": c.get("game_pk"),
                    "cal_prob": c["cal_prob"],
                    "implied_prob": c["implied_prob"],
                    "edge": c["edge"],
                    "best_odds": odds,
                    "best_book": c.get("best_book",""),
                    "stake": stake,
                    "expected_payout": round(exp_payout, 2),
                    "expected_value": round(ev, 2),
                    "score": c["score"],
                    "reasons": c.get("reasons", []),
                })

    # 2-leg parlays — all pairs, ranked by combo_score
    all_candidates_list = [labeled[l] for l in labels if l in labeled]
    parlay_2_candidates = []

    for leg_a, leg_b in combinations(all_candidates_list, 2):
        legs = [leg_a, leg_b]
        if _same_game_count(legs) > MAX_SAME_GAME_LEGS - 1:
            continue
        cs = combo_score(legs)
        odds = parlay_american_odds(legs)
        dec = american_to_decimal(odds)
        label_key = leg_a["rank_label"] + leg_b["rank_label"]
        stake = STAKE_MAP.get(label_key, 8.0)
        combined_prob = leg_a["cal_prob"] * leg_b["cal_prob"]
        ev = (combined_prob * stake * dec) - stake

        parlay_2_candidates.append({
            "legs": [leg_a["player_id"], leg_b["player_id"]],
            "leg_labels": label_key,
            "leg_names": [leg_a.get("player_name",""), leg_b.get("player_name","")],
            "combo_score": cs,
            "combined_prob": round(combined_prob, 4),
            "american_odds": odds,
            "stake": stake,
            "expected_payout": round(stake * dec, 2),
            "expected_value": round(ev, 2),
        })

    parlay_2_candidates.sort(key=lambda x: x["combo_score"], reverse=True)
    # Only keep top 3 2-leg parlays, and only if EV > 0
    parlay_2 = [p for p in parlay_2_candidates[:3] if p["expected_value"] > 0]

    # 3-leg parlays
    parlay_3_candidates = []
    for combo in combinations(all_candidates_list, 3):
        legs = list(combo)
        if _same_game_count(legs) > MAX_SAME_GAME_LEGS:
            continue
        cs = combo_score(legs)
        odds = parlay_american_odds(legs)
        dec = american_to_decimal(odds)
        label_key = "".join(l["rank_label"] for l in legs)
        stake = STAKE_MAP.get(label_key, 4.0)
        combined_prob = 1.0
        for l in legs:
            combined_prob *= l["cal_prob"]
        ev = (combined_prob * stake * dec) - stake

        parlay_3_candidates.append({
            "legs": [l["player_id"] for l in legs],
            "leg_labels": label_key,
            "leg_names": [l.get("player_name","") for l in legs],
            "combo_score": cs,
            "combined_prob": round(combined_prob, 4),
            "american_odds": odds,
            "stake": stake,
            "expected_payout": round(stake * dec, 2),
            "expected_value": round(ev, 2),
        })

    parlay_3_candidates.sort(key=lambda x: x["combo_score"], reverse=True)
    parlay_3 = [p for p in parlay_3_candidates[:2] if p["expected_value"] > 0]

    # 4-leg parlay — only include if EV > 0 (no lottery exception)
    parlay_4 = []
    if len(all_candidates_list) == 4:
        legs = all_candidates_list
        if _same_game_count(legs) <= MAX_SAME_GAME_LEGS:
            cs = combo_score(legs)
            odds = parlay_american_odds(legs)
            dec = american_to_decimal(odds)
            stake = STAKE_MAP.get("ABCD", 10.0)
            combined_prob = 1.0
            for l in legs:
                combined_prob *= l["cal_prob"]
            ev = (combined_prob * stake * dec) - stake
            # EV gate applies to 4-leg too — no free lottery passes
            if ev > 0:
                parlay_4 = [{
                    "legs": [l["player_id"] for l in legs],
                    "leg_labels": "ABCD",
                    "leg_names": [l.get("player_name","") for l in legs],
                    "combo_score": cs,
                    "combined_prob": round(combined_prob, 4),
                    "american_odds": odds,
                    "stake": stake,
                    "expected_payout": round(stake * dec, 2),
                    "expected_value": round(ev, 2),
                }]
            else:
                log.info(f"4-leg ABCD parlay EV={ev:.2f} < 0 — excluded.")

    return {
        "singles": singles,
        "parlay_2": parlay_2,
        "parlay_3": parlay_3,
        "parlay_4": parlay_4,
    }


def save_recommendations(card: dict, game_date: Optional[date] = None):
    """Persist bet recommendations to DB."""
    if game_date is None:
        game_date = date.today()

    conn = get_connection()
    cur = conn.cursor()
    now = _now_utc()
    date_str = game_date.strftime("%Y-%m-%d")

    for s in card.get("singles", []):
        cur.execute("""
            INSERT INTO bet_recommendations(
                bet_date,bet_type,legs,leg_labels,combo_score,stake,
                expected_payout,expected_value,american_odds,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (
            date_str, "single",
            json.dumps([s["player_id"]]), s["label"],
            s["score"], s["stake"], s["expected_payout"],
            s["expected_value"], s["best_odds"], now,
        ))

    for parlay_list, bet_type in [
        (card.get("parlay_2",[]), "parlay_2"),
        (card.get("parlay_3",[]), "parlay_3"),
        (card.get("parlay_4",[]), "parlay_4"),
    ]:
        for p in parlay_list:
            cur.execute("""
                INSERT INTO bet_recommendations(
                    bet_date,bet_type,legs,leg_labels,combo_score,stake,
                    expected_payout,expected_value,american_odds,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (
                date_str, bet_type,
                json.dumps(p["legs"]), p["leg_labels"],
                p["combo_score"], p["stake"], p["expected_payout"],
                p["expected_value"], p["american_odds"], now,
            ))

    conn.commit()
    conn.close()
    log.info("Recommendations saved.")
