"""
MLB Home Run Bot — Parlay Combo Optimizer
Builds best 2-leg, 3-leg, 4-leg parlays from A/B/C/D candidates.

Kelly sizing: scales bet size by edge strength — never blocks a pick.
Thin-edge bets get a warning flag so the user can decide.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from itertools import combinations
from datetime import date, datetime, timezone
from typing import Optional

from config import DAILY_BUDGET, MAX_SAME_GAME_LEGS
from data.schema import get_connection, execute

log = logging.getLogger(__name__)

# Kelly parameters
KELLY_FRAC   = 0.25   # quarter Kelly — conservative
MAX_STAKE    = 50.0   # hard cap per single bet
MIN_STAKE    = 5.0    # floor for display (never skip based on this)
THIN_EDGE_THRESHOLD = 0.02   # edge below this gets a warning flag


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def american_to_decimal(american_odds: int) -> float:
    if american_odds >= 0:
        return (american_odds / 100) + 1.0
    else:
        return (100 / abs(american_odds)) + 1.0


def kelly_stake(cal_prob: float, american_odds: int) -> tuple[float, bool]:
    """
    Fractional Kelly sizing. NEVER blocks — always returns a stake >= MIN_STAKE.
    Returns (stake, is_thin_edge).

    is_thin_edge = True when Kelly is negative or very small — flags the pick
    for the user's attention without removing it from the card.
    """
    dec = american_to_decimal(american_odds)
    b = dec - 1.0
    if b <= 0:
        return MIN_STAKE, True

    p = cal_prob
    q = 1.0 - p
    full_kelly = (b * p - q) / b

    if full_kelly <= 0:
        # Negative Kelly = book has edge — flag it but still show the pick
        return MIN_STAKE, True

    frac_kelly = full_kelly * KELLY_FRAC
    raw_stake  = frac_kelly * DAILY_BUDGET
    stake      = round(max(MIN_STAKE, min(MAX_STAKE, raw_stake)), 2)
    is_thin    = (raw_stake < 3.0) or (full_kelly < THIN_EDGE_THRESHOLD)
    return stake, is_thin


def kelly_stake_parlay(combined_prob: float, american_odds: int) -> tuple[float, bool]:
    """Kelly sizing for parlays — same logic, lower natural stakes due to low prob."""
    return kelly_stake(combined_prob, american_odds)


def parlay_american_odds(legs: list[dict]) -> int:
    """Combine multiple legs into parlay American odds."""
    decimal = 1.0
    for leg in legs:
        odds = leg.get("best_odds")
        if not odds:
            ip = leg.get("implied_prob", 0.15)
            dec = 1.0 / max(ip, 0.01)
        else:
            dec = american_to_decimal(int(odds))
        decimal *= dec
    if decimal >= 2.0:
        return int((decimal - 1.0) * 100)
    else:
        return int(-100 / (decimal - 1.0))


def combo_score(legs: list[dict]) -> float:
    """Score a parlay combination."""
    base = sum(leg.get("score", 0.0) for leg in legs)

    price_bonus = 0.0
    for leg in legs:
        edge = leg.get("edge", 0.0)
        if edge > 0.05:
            price_bonus += 0.02
        elif edge > 0.03:
            price_bonus += 0.01

    game_pks = [leg.get("game_pk") for leg in legs]
    same_game_count = len(game_pks) - len(set(game_pks))
    corr_penalty = same_game_count * 0.10

    avg_stab = sum(leg.get("stability", 0.5) for leg in legs) / len(legs)
    vol_penalty = max(0.0, (0.6 - avg_stab) * 0.15)

    return round(base + price_bonus - corr_penalty - vol_penalty, 4)


def _same_game_count(legs: list[dict]) -> int:
    games = [l.get("game_pk") for l in legs]
    return len(games) - len(set(games))


def build_parlays(candidates: list[dict]) -> dict:
    """
    Given A/B/C/D candidates, build ranked 2-leg, 3-leg, and 4-leg parlays.
    Always produces a full card. Kelly sizes stakes but never removes picks.
    Thin-edge picks are flagged with 'thin_edge': True for dashboard display.
    """
    if not candidates:
        return {"singles": [], "parlay_2": [], "parlay_3": [], "parlay_4": [],
                "no_bets_today": True, "reason": "No candidates scored today."}

    labels = ["A", "B", "C", "D"]
    labeled = {c["rank_label"]: c for c in candidates if c.get("rank_label") in labels}

    # ── Singles ──────────────────────────────────────────────────────────
    singles = []
    for label in labels:
        if label not in labeled:
            continue
        c = labeled[label]
        odds = c.get("best_odds")
        if not odds:
            continue
        dec = american_to_decimal(odds)
        stake, thin = kelly_stake(c["cal_prob"], odds)
        exp_payout = round(stake * dec, 2)
        ev = round((c["cal_prob"] * exp_payout) - stake, 2)

        singles.append({
            "label":           label,
            "player_id":       c["player_id"],
            "player_name":     c.get("player_name", ""),
            "game_pk":         c.get("game_pk"),
            "cal_prob":        c["cal_prob"],
            "implied_prob":    c["implied_prob"],
            "edge":            c["edge"],
            "best_odds":       odds,
            "best_book":       c.get("best_book", ""),
            "stake":           stake,
            "expected_payout": exp_payout,
            "expected_value":  ev,
            "score":           c["score"],
            "reasons":         c.get("reasons", []),
            "thin_edge":       thin,   # ← flag for dashboard warning
        })

    all_candidates_list = [labeled[l] for l in labels if l in labeled]

    # ── 2-leg parlays ─────────────────────────────────────────────────────
    parlay_2_candidates = []
    for leg_a, leg_b in combinations(all_candidates_list, 2):
        legs = [leg_a, leg_b]
        if _same_game_count(legs) > MAX_SAME_GAME_LEGS - 1:
            continue
        cs           = combo_score(legs)
        odds         = parlay_american_odds(legs)
        dec          = american_to_decimal(odds)
        combined_prob = leg_a["cal_prob"] * leg_b["cal_prob"]
        stake, thin  = kelly_stake_parlay(combined_prob, odds)
        ev           = round((combined_prob * stake * dec) - stake, 2)

        parlay_2_candidates.append({
            "legs":          [leg_a["player_id"], leg_b["player_id"]],
            "leg_labels":    leg_a["rank_label"] + leg_b["rank_label"],
            "leg_names":     [leg_a.get("player_name", ""), leg_b.get("player_name", "")],
            "combo_score":   cs,
            "combined_prob": round(combined_prob, 4),
            "american_odds": odds,
            "stake":         stake,
            "expected_payout": round(stake * dec, 2),
            "expected_value": ev,
            "thin_edge":     thin,
        })

    parlay_2_candidates.sort(key=lambda x: x["combo_score"], reverse=True)
    parlay_2 = parlay_2_candidates[:1]   # best 2-leg combo

    # ── 3-leg parlays ─────────────────────────────────────────────────────
    parlay_3_candidates = []
    for combo in combinations(all_candidates_list, 3):
        legs = list(combo)
        if _same_game_count(legs) > MAX_SAME_GAME_LEGS:
            continue
        cs            = combo_score(legs)
        odds          = parlay_american_odds(legs)
        dec           = american_to_decimal(odds)
        combined_prob = 1.0
        for l in legs:
            combined_prob *= l["cal_prob"]
        stake, thin   = kelly_stake_parlay(combined_prob, odds)
        ev            = round((combined_prob * stake * dec) - stake, 2)

        parlay_3_candidates.append({
            "legs":          [l["player_id"] for l in legs],
            "leg_labels":    "".join(l["rank_label"] for l in legs),
            "leg_names":     [l.get("player_name", "") for l in legs],
            "combo_score":   cs,
            "combined_prob": round(combined_prob, 4),
            "american_odds": odds,
            "stake":         stake,
            "expected_payout": round(stake * dec, 2),
            "expected_value": ev,
            "thin_edge":     thin,
        })

    parlay_3_candidates.sort(key=lambda x: x["combo_score"], reverse=True)
    parlay_3 = parlay_3_candidates[:1]   # best 3-leg combo

    # ── 4-leg parlay ──────────────────────────────────────────────────────
    parlay_4 = []
    if len(all_candidates_list) == 4:
        legs = all_candidates_list
        if _same_game_count(legs) <= MAX_SAME_GAME_LEGS:
            cs            = combo_score(legs)
            odds          = parlay_american_odds(legs)
            dec           = american_to_decimal(odds)
            combined_prob = 1.0
            for l in legs:
                combined_prob *= l["cal_prob"]
            stake, thin   = kelly_stake_parlay(combined_prob, odds)
            ev            = round((combined_prob * stake * dec) - stake, 2)

            parlay_4 = [{
                "legs":          [l["player_id"] for l in legs],
                "leg_labels":    "ABCD",
                "leg_names":     [l.get("player_name", "") for l in legs],
                "combo_score":   cs,
                "combined_prob": round(combined_prob, 4),
                "american_odds": odds,
                "stake":         stake,
                "expected_payout": round(stake * dec, 2),
                "expected_value": ev,
                "thin_edge":     thin,
            }]
            if ev <= 0:
                log.info(f"4-leg ABCD EV={ev:.2f} < 0 — flagged thin, still shown.")

    # ── No-bets check ─────────────────────────────────────────────────────
    # Even if all picks are thin, the card is returned — user decides.
    all_thin = all(s.get("thin_edge", False) for s in singles)
    no_bets_msg = None
    if not singles:
        no_bets_msg = "No ranked candidates today — check odds ingestion."
    elif all_thin:
        no_bets_msg = "All picks are thin-edge today. Consider skipping or sizing down."

    return {
        "singles":       singles,
        "parlay_2":      parlay_2,
        "parlay_3":      parlay_3,
        "parlay_4":      parlay_4,
        "no_bets_today": all_thin or not singles,
        "no_bets_reason": no_bets_msg or "",
    }


def save_recommendations(card: dict, game_date: Optional[date] = None):
    """Persist bet recommendations to DB."""
    if game_date is None:
        game_date = date.today()

    conn = get_connection()
    now      = _now_utc()
    date_str = game_date.strftime("%Y-%m-%d")

    for s in card.get("singles", []):
        execute(conn, """
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
        (card.get("parlay_2", []), "parlay_2"),
        (card.get("parlay_3", []), "parlay_3"),
        (card.get("parlay_4", []), "parlay_4"),
    ]:
        for p in parlay_list:
            execute(conn, """
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
