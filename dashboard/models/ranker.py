"""
MLB Home Run Bot — Edge Calculator and A/B/C/D Ranker
Scores every batter, assigns A/B/C/D labels, generates reason codes.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from datetime import date, datetime, timezone
from typing import Optional

from config import (
    MIN_EDGE_PCT, MIN_PROJECTED_PA,
    SCORE_WEIGHT_EDGE, SCORE_WEIGHT_TRUE_PROB,
    SCORE_WEIGHT_STABILITY, SCORE_WEIGHT_CORR_RISK,
)
from data.schema import get_connection, execute, fetchone, fetchall
from pipeline.features import build_all_feature_vectors
from models.train import predict_proba
from models.openrouter import analyze_full_card

log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def implied_probability(american_odds: int) -> float:
    if american_odds >= 0:
        return 100 / (american_odds + 100)
    return abs(american_odds) / (abs(american_odds) + 100)


def stability_score(feats: dict) -> float:
    """
    How consistent is this hitter's contact quality across windows?
    Higher = more stable (less variance across 7/14/30 day windows).
    """
    br_vals = [feats.get(f"barrel_rate_last_{d}d") for d in [7, 14, 30]]
    br_vals = [v for v in br_vals if v is not None]
    if len(br_vals) < 2:
        return 0.50  # default moderate stability

    import statistics
    try:
        cv = statistics.stdev(br_vals) / (statistics.mean(br_vals) + 0.001)
        # cv=0 → stability=1.0; cv=0.5 → ~0.5
        stab = max(0.0, min(1.0, 1.0 - cv * 2.0))
    except Exception:
        stab = 0.50

    # Bonus if pa_count is high (more reliable sample)
    pa = feats.get("pa_count_season") or 0
    if pa > 200:
        stab = min(1.0, stab + 0.10)
    elif pa < 50:
        stab = max(0.0, stab - 0.15)

    return round(stab, 4)


def correlation_risk(feats: dict, existing_picks: list[dict]) -> float:
    """
    Returns a correlation penalty (0.0–1.0).
    Higher = more correlated with existing picks (worse for parlay diversification).
    """
    risk = 0.0
    my_game = feats.get("game_pk")

    for pick in existing_picks:
        if pick.get("game_pk") == my_game:
            risk += 0.40   # same game = high correlation
        elif pick.get("pitcher_id") and pick.get("pitcher_id") == feats.get("pitcher_id"):
            risk += 0.20   # same opposing pitcher

    return min(1.0, risk)


def generate_reason_codes(feats: dict, cal_prob: float, implied_prob: float) -> list[str]:
    """Generate human-readable reason codes for a pick."""
    reasons = []

    br = feats.get("barrel_rate_last_14d") or feats.get("barrel_rate_season") or 0
    if br >= 0.12:
        reasons.append(f"Elite barrel rate ({br:.1%}) over 14d")
    elif br >= 0.10:
        reasons.append(f"Strong barrel rate ({br:.1%}) over 14d")

    ev = feats.get("avg_ev_last_14d") or feats.get("avg_ev_season") or 0
    if ev >= 92:
        reasons.append(f"High exit velocity ({ev:.1f} mph)")

    xslg = feats.get("xslg_last_14d") or feats.get("xslg_season") or 0
    if xslg >= 0.500:
        reasons.append(f"Premium xSLG ({xslg:.3f})")

    p_br = feats.get("p_barrel_rate_allowed") or 0
    if p_br >= 0.10:
        reasons.append(f"Pitcher allows high barrel rate ({p_br:.1%})")

    p_hr9 = feats.get("p_hr9") or 0
    if p_hr9 >= 1.3:
        reasons.append(f"Pitcher vulnerable (HR/9: {p_hr9:.2f})")

    wind = feats.get("wind_dir_label","")
    ws = feats.get("wind_speed_mph", 0)
    if wind == "out_to_cf" and ws >= 8:
        reasons.append(f"Wind blowing out to CF ({ws:.0f} mph)")

    pf = feats.get("park_hr_factor") or 1.0
    if pf >= 1.10:
        reasons.append(f"Hitter-friendly park (HR factor {pf:.2f})")
    elif pf <= 0.90:
        reasons.append(f"Pitcher-friendly park (HR factor {pf:.2f})")

    temp = feats.get("temp_f") or 72
    if temp >= 82:
        reasons.append(f"Warm conditions favor carry ({temp:.0f}°F)")

    alt = feats.get("altitude_ft") or 0
    if alt >= 3000:
        reasons.append(f"High altitude benefits fly balls ({alt:.0f} ft)")

    edge = cal_prob - implied_prob
    if edge >= 0.06:
        reasons.append(f"Strong price edge ({edge:.1%} above implied)")
    elif edge >= 0.03:
        reasons.append(f"Positive price edge ({edge:.1%})")

    if not reasons:
        reasons.append("Marginally positive expected value")

    return reasons


def player_score(edge: float, cal_prob: float, stab: float, corr_risk: float) -> float:
    """Master scoring formula."""
    return (
        SCORE_WEIGHT_EDGE      * edge +
        SCORE_WEIGHT_TRUE_PROB * cal_prob +
        SCORE_WEIGHT_STABILITY * stab -
        SCORE_WEIGHT_CORR_RISK * corr_risk
    )


def run_ranking(game_date: Optional[date] = None, run_stage: str = "final") -> list[dict]:
    """
    Full scoring pipeline:
    1. Build feature vectors for all lineup batters.
    2. Predict HR probability.
    3. Compare to best available odds.
    4. Score and rank.
    5. Label A/B/C/D.
    6. Save to model_predictions.
    Returns ranked list of prediction dicts.
    """
    if game_date is None:
        game_date = date.today()

    log.info(f"Running ranking for {game_date} ({run_stage})")

    # Build features
    vectors = build_all_feature_vectors(game_date)
    if not vectors:
        log.warning("No feature vectors found. Check lineups and game data.")
        return []

    conn = get_connection()
    now = _now_utc()

    results = []
    for feats in vectors:
        pid = feats["player_id"]
        gpk = feats["game_pk"]

        # Get probability
        _, cal_prob = predict_proba(feats)

        # Get best odds (highest american_odds = best payout for HR dogs)
        odds_row = fetchone(conn, """
            SELECT american_odds as best_odds, bookmaker, implied_prob
            FROM sportsbook_odds
            WHERE player_id=? AND (game_pk=? OR game_pk IS NULL)
            ORDER BY american_odds DESC LIMIT 1
        """, (pid, gpk))

        if odds_row and odds_row["best_odds"]:
            best_odds = odds_row["best_odds"]
            best_book = odds_row["bookmaker"]
            impl_prob = implied_probability(best_odds)
        else:
            # No odds found — skip this player for betting, but still score
            best_odds = None
            best_book = None
            impl_prob = 0.15  # league average placeholder

        edge = cal_prob - impl_prob

        # Stability
        stab = stability_score(feats)

        # Correlation risk vs already-seen picks
        corr = correlation_risk(feats, results)

        # Score
        score = player_score(edge, cal_prob, stab, corr)

        # Opportunity checks
        confirmed = feats.get("confirmed_lineup", 0)
        proj_pa = feats.get("projected_pa", 3.8)

        # Reason codes
        reasons = generate_reason_codes(feats, cal_prob, impl_prob)

        rec = {
            "player_id": pid,
            "game_pk": gpk,
            "cal_prob": round(cal_prob, 4),
            "implied_prob": round(impl_prob, 4),
            "edge": round(edge, 4),
            "stability": round(stab, 4),
            "corr_risk": round(corr, 4),
            "score": round(score, 4),
            "best_odds": best_odds,
            "best_book": best_book,
            "proj_pa": proj_pa,
            "batting_order": feats.get("batting_order"),
            "confirmed": confirmed,
            "reasons": reasons,
            "pitcher_id": feats.get("pitcher_id"),
            "rank_label": None,
        }
        results.append(rec)

    # Filter: must be confirmed lineup, meet PA minimum, have positive edge if odds available
    playable = [
        r for r in results
        if r["confirmed"] == 1
        and r["proj_pa"] >= MIN_PROJECTED_PA
        and (r["best_odds"] is None or r["edge"] >= MIN_EDGE_PCT)
    ]

    # Sort by score desc
    playable.sort(key=lambda x: x["score"], reverse=True)

    # Label A/B/C/D
    labels = ["A", "B", "C", "D"]
    labeled = []
    for i, rec in enumerate(playable[:4]):
        rec["rank_label"] = labels[i]
        labeled.append(rec)

    # Save to DB
    all_ranked = labeled + [r for r in playable[4:]] + [r for r in results if r not in playable]
    for rec in all_ranked:
        execute(conn, """
            INSERT INTO model_predictions(
                game_pk,player_id,run_timestamp,run_stage,model_prob,calibrated_prob,
                pre_ai_prob,
                best_implied_prob,best_odds,best_bookmaker,edge,stability_score,
                player_score,rank_label,reason_codes,projected_pa,batting_order,confirmed_lineup
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            rec["game_pk"], rec["player_id"], now, run_stage,
            rec["cal_prob"], rec["cal_prob"],
            rec["cal_prob"],   # pre_ai_prob = model prob before any AI adjustment
            rec["implied_prob"], rec["best_odds"], rec["best_book"],
            rec["edge"], rec["stability"], rec["score"],
            rec["rank_label"], json.dumps(rec["reasons"]),
            rec["proj_pa"], rec["batting_order"], rec["confirmed"],
        ))

    conn.commit()

    # Fetch player names for output
    named = []
    for rec in labeled:
        row = fetchone(conn, "SELECT full_name FROM players WHERE player_id=?", (rec["player_id"],))
        rec["player_name"] = row["full_name"] if row else f"Player {rec['player_id']}"
        named.append(rec)

    conn.close()
    log.info(f"Ranked {len(playable)} playable hitters. Top 4 labeled.")

    # AI enrichment — only on final run stage to save API calls
    if run_stage == "final" and named:
        try:
            log.info("Running AI analysis on top picks...")
            named, card_summary = analyze_full_card(named, game_date.strftime("%Y-%m-%d"))
            # Store summary on first pick
            if named and card_summary:
                named[0]["card_summary"] = card_summary
            # Save AI data back to DB
            conn2 = get_connection()
            for rec in named:
                ai_blob = json.dumps({
                    "ai_verdict":        rec.get("ai_verdict", ""),
                    "ai_grade":          rec.get("ai_grade", ""),
                    "ai_one_liner":      rec.get("ai_one_liner", ""),
                    "ai_bull":           rec.get("ai_bull", ""),
                    "ai_bear":           rec.get("ai_bear", ""),
                    "ai_sharp":          rec.get("ai_sharp", ""),
                    "ai_confidence_adj": rec.get("ai_confidence_adj", 0.0),
                    "card_summary":      rec.get("card_summary", ""),
                })
                execute(conn2, """
                    UPDATE model_predictions
                    SET ai_analysis = ?
                    WHERE player_id = ? AND run_timestamp = ? AND run_stage = ?
                """, (ai_blob, rec["player_id"], now, run_stage))
            conn2.commit()
            conn2.close()
        except Exception as e:
            log.warning(f"AI analysis failed (non-fatal): {e}")

    return named
