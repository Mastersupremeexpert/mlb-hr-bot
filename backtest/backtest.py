"""
MLB Home Run Bot — Backtesting Module
Evaluates historical prediction performance: Brier score, calibration, ROI by bet type.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from datetime import date
from collections import defaultdict

import numpy as np

from data.schema import get_connection, execute, fetchall

log = logging.getLogger(__name__)


def compute_brier_score(probs: list[float], outcomes: list[int]) -> float:
    """Lower is better. Perfect = 0, random = 0.25."""
    if not probs:
        return None
    return float(np.mean((np.array(probs) - np.array(outcomes)) ** 2))


def compute_calibration_curve(probs: list[float], outcomes: list[int], n_bins: int = 10):
    if not probs:
        return []
    bins = np.linspace(0, 1, n_bins + 1)
    curve = []
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        in_bin_p = [p for p in probs if lo <= p < hi]
        in_bin_y = [y for p, y in zip(probs, outcomes) if lo <= p < hi]
        if in_bin_p:
            curve.append({
                "bin_center":    round((lo + hi) / 2, 2),
                "mean_pred":     round(float(np.mean(in_bin_p)), 4),
                "fraction_pos":  round(float(np.mean(in_bin_y)), 4),
                "count":         len(in_bin_p),
            })
    return curve


def compute_clv(predictions: list[dict]) -> dict:
    """
    Closing Line Value: did we beat the closing number?
    CLV > 0 means we got better odds than the market closed at.
    """
    clv_total = 0.0
    clv_count = 0
    for p in predictions:
        locked_implied  = p.get("best_implied_prob")
        closing_implied = p.get("closing_implied_prob")
        if locked_implied and closing_implied:
            clv_total += closing_implied - locked_implied
            clv_count += 1
    if clv_count == 0:
        return {"avg_clv": None, "count": 0}
    return {"avg_clv": round(clv_total / clv_count, 4), "count": clv_count}


def compute_ai_impact(predictions: list[dict]) -> dict:
    """
    Compare pre_ai_prob vs calibrated_prob vs outcomes.
    Measures whether the AI adjustment helped or hurt calibration.
    """
    pairs = [
        (p["pre_ai_prob"], p["calibrated_prob"], p["won"])
        for p in predictions
        if p.get("pre_ai_prob") and p.get("calibrated_prob") and p.get("won") is not None
    ]
    if not pairs:
        return {"ai_impact": "insufficient_data", "count": 0}

    pre_probs  = [x[0] for x in pairs]
    post_probs = [x[1] for x in pairs]
    outcomes   = [x[2] for x in pairs]

    pre_brier  = float(np.mean((np.array(pre_probs) - np.array(outcomes)) ** 2))
    post_brier = float(np.mean((np.array(post_probs) - np.array(outcomes)) ** 2))
    delta      = post_brier - pre_brier  # negative = AI improved calibration

    return {
        "pre_ai_brier":  round(pre_brier, 5),
        "post_ai_brier": round(post_brier, 5),
        "delta":         round(delta, 5),
        "ai_helped":     delta < 0,
        "count":         len(pairs),
    }


def run_backtest(
    start_date: str | None = None,
    end_date:   str | None = None,
    bet_types:  list[str] | None = None,
) -> dict:
    """Full backtest report for a date range."""
    if bet_types is None:
        bet_types = ["single", "parlay_2", "parlay_3", "parlay_4"]

    conn = get_connection()

    where_clauses = ["br.won IS NOT NULL"]
    params = []
    if start_date:
        where_clauses.append("rec.bet_date >= ?")
        params.append(start_date)
    if end_date:
        where_clauses.append("rec.bet_date <= ?")
        params.append(end_date)
    where = " AND ".join(where_clauses)

    rows = fetchall(conn, f"""
        SELECT rec.*, br.won, br.payout, br.profit, br.settled_at
        FROM bet_recommendations rec
        JOIN bet_results br ON br.recommendation_id = rec.id
        WHERE {where}
        ORDER BY rec.bet_date
    """, params if params else None)

    pred_rows = fetchall(conn, """
        SELECT mp.calibrated_prob,
               mp.pre_ai_prob,
               mp.best_implied_prob,
               cl.closing_implied_prob,
               br.won
        FROM model_predictions mp
        JOIN bet_recommendations rec ON (
            rec.legs = json_array(mp.player_id)
            AND rec.bet_date = substr(mp.run_timestamp, 1, 10)
            AND rec.bet_type = 'single'
        )
        JOIN bet_results br ON br.recommendation_id = rec.id
        LEFT JOIN closing_lines cl ON (
            cl.player_id = mp.player_id
            AND cl.game_pk = mp.game_pk
        )
        WHERE br.won IS NOT NULL
    """, None)
    conn.close()

    all_probs    = [r["calibrated_prob"] for r in pred_rows if r["calibrated_prob"]]
    all_outcomes = [r["won"]             for r in pred_rows if r["calibrated_prob"]]
    all_dicts    = [dict(r)              for r in pred_rows]

    brier             = compute_brier_score(all_probs, all_outcomes)
    calibration_curve = compute_calibration_curve(all_probs, all_outcomes)
    clv               = compute_clv(all_dicts)
    ai_impact         = compute_ai_impact(all_dicts)

    by_type = defaultdict(lambda: {
        "bets": 0, "wins": 0, "staked": 0.0, "profit": 0.0,
    })
    for r in rows:
        bt = r["bet_type"]
        by_type[bt]["bets"]   += 1
        by_type[bt]["wins"]   += int(r["won"])
        by_type[bt]["staked"] += r["stake"]  or 0.0
        by_type[bt]["profit"] += r["profit"] or 0.0

    roi_by_type = {}
    for bt, d in by_type.items():
        roi_by_type[bt] = {
            "bets":          d["bets"],
            "wins":          d["wins"],
            "hit_rate_pct":  round((d["wins"] / max(d["bets"], 1)) * 100, 2),
            "total_staked":  round(d["staked"], 2),
            "total_profit":  round(d["profit"], 2),
            "roi_pct":       round((d["profit"] / max(d["staked"], 0.01)) * 100, 2),
        }

    daily = defaultdict(float)
    for r in rows:
        daily[r["bet_date"]] += r["profit"] or 0.0
    daily_sorted = [{"date": k, "profit": round(v, 2)} for k, v in sorted(daily.items())]

    result = {
        "period":            {"start": start_date, "end": end_date},
        "total_bets":        len(rows),
        "brier_score":       round(brier, 5) if brier else None,
        "calibration_curve": calibration_curve,
        "clv":               clv,
        "ai_impact":         ai_impact,
        "roi_by_type":       roi_by_type,
        "daily_pnl":         daily_sorted,
    }

    log.info("=== BACKTEST RESULTS ===")
    log.info(f"  Period: {start_date} → {end_date} | Total bets: {result['total_bets']}")
    log.info(f"  Brier: {result['brier_score']} | CLV: {clv} | AI impact: {ai_impact}")
    for bt, roi in roi_by_type.items():
        log.info(
            f"  [{bt}] Bets={roi['bets']} | Win%={roi['hit_rate_pct']:.1f}% | "
            f"Profit=${roi['total_profit']:.2f} | ROI={roi['roi_pct']:.1f}%"
        )
    return result


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end",   type=str, default=None)
    args = parser.parse_args()
    print(json.dumps(run_backtest(args.start, args.end), indent=2))
