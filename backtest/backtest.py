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

from data.schema import get_connection

log = logging.getLogger(__name__)


def compute_brier_score(probs: list[float], outcomes: list[int]) -> float:
    """Lower is better. Perfect = 0, random = 0.25."""
    if not probs:
        return None
    arr_p = np.array(probs)
    arr_y = np.array(outcomes)
    return float(np.mean((arr_p - arr_y) ** 2))


def compute_calibration_curve(probs: list[float], outcomes: list[int], n_bins: int = 10):
    """
    Returns list of (mean_pred_prob, fraction_positive, count) per bin.
    Use this to plot or inspect calibration.
    """
    if not probs:
        return []
    bins = np.linspace(0, 1, n_bins + 1)
    curve = []
    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = [(lo <= p < hi) for p in probs]
        in_bin_p = [p for p, m in zip(probs, mask) if m]
        in_bin_y = [y for y, m in zip(outcomes, mask) if m]
        if in_bin_p:
            curve.append({
                "bin_center": round((lo + hi) / 2, 2),
                "mean_pred": round(float(np.mean(in_bin_p)), 4),
                "fraction_pos": round(float(np.mean(in_bin_y)), 4),
                "count": len(in_bin_p),
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
        locked_implied = p.get("best_implied_prob")
        closing_implied = p.get("closing_implied_prob")
        if locked_implied and closing_implied:
            clv = closing_implied - locked_implied  # positive = we got better price
            clv_total += clv
            clv_count += 1
    if clv_count == 0:
        return {"avg_clv": None, "count": 0}
    return {"avg_clv": round(clv_total / clv_count, 4), "count": clv_count}


def run_backtest(
    start_date: str | None = None,
    end_date: str | None = None,
    bet_types: list[str] | None = None,
) -> dict:
    """
    Full backtest report for a date range.
    Returns metrics by bet_type.
    """
    if bet_types is None:
        bet_types = ["single", "parlay_2", "parlay_3", "parlay_4"]

    conn = get_connection()
    cur = conn.cursor()

    where_clauses = ["br.won IS NOT NULL"]
    params = []
    if start_date:
        where_clauses.append("rec.bet_date >= ?")
        params.append(start_date)
    if end_date:
        where_clauses.append("rec.bet_date <= ?")
        params.append(end_date)
    where = " AND ".join(where_clauses)

    rows = cur.execute(f"""
        SELECT rec.*, br.won, br.payout, br.profit, br.settled_at
        FROM bet_recommendations rec
        JOIN bet_results br ON br.recommendation_id = rec.id
        WHERE {where}
        ORDER BY rec.bet_date
    """, params).fetchall()

    # Get predictions for calibration
    pred_rows = cur.execute("""
        SELECT mp.calibrated_prob, mp.best_implied_prob,
               cl.closing_implied_prob,
               br.won
        FROM model_predictions mp
        JOIN bet_recommendations rec ON (
            rec.legs LIKE '%' || mp.player_id || '%'
            AND rec.bet_date = substr(mp.run_timestamp,1,10)
            AND rec.bet_type = 'single'
        )
        JOIN bet_results br ON br.recommendation_id = rec.id
        LEFT JOIN closing_lines cl ON (cl.player_id = mp.player_id AND cl.game_pk = mp.game_pk)
        WHERE br.won IS NOT NULL
    """).fetchall()
    conn.close()

    all_probs = [r["calibrated_prob"] for r in pred_rows if r["calibrated_prob"]]
    all_outcomes = [r["won"] for r in pred_rows if r["calibrated_prob"]]
    all_pred_dicts = [dict(r) for r in pred_rows]

    # Overall calibration
    brier = compute_brier_score(all_probs, all_outcomes)
    calibration_curve = compute_calibration_curve(all_probs, all_outcomes)
    clv = compute_clv(all_pred_dicts)

    # ROI by bet type
    by_type = defaultdict(lambda: {
        "bets": 0, "wins": 0, "staked": 0.0, "profit": 0.0, "payouts": []
    })

    for r in rows:
        bt = r["bet_type"]
        by_type[bt]["bets"] += 1
        by_type[bt]["wins"] += int(r["won"])
        by_type[bt]["staked"] += r["stake"] or 0.0
        by_type[bt]["profit"] += r["profit"] or 0.0
        by_type[bt]["payouts"].append(r["payout"] or 0.0)

    roi_by_type = {}
    for bt, d in by_type.items():
        roi_pct = (d["profit"] / max(d["staked"], 0.01)) * 100
        hit_rate = (d["wins"] / max(d["bets"], 1)) * 100
        roi_by_type[bt] = {
            "bets": d["bets"],
            "wins": d["wins"],
            "hit_rate_pct": round(hit_rate, 2),
            "total_staked": round(d["staked"], 2),
            "total_profit": round(d["profit"], 2),
            "roi_pct": round(roi_pct, 2),
        }

    # Daily P&L
    daily = defaultdict(float)
    for r in rows:
        daily[r["bet_date"]] += r["profit"] or 0.0
    daily_sorted = [{"date": k, "profit": round(v, 2)} for k, v in sorted(daily.items())]

    result = {
        "period": {"start": start_date, "end": end_date},
        "total_bets": len(rows),
        "brier_score": round(brier, 5) if brier else None,
        "calibration_curve": calibration_curve,
        "clv": clv,
        "roi_by_type": roi_by_type,
        "daily_pnl": daily_sorted,
    }

    # Print summary
    log.info("=== BACKTEST RESULTS ===")
    log.info(f"  Period: {start_date} → {end_date}")
    log.info(f"  Total bets: {result['total_bets']}")
    log.info(f"  Brier score: {result['brier_score']}")
    log.info(f"  CLV: {clv}")
    for bt, roi in roi_by_type.items():
        log.info(f"  [{bt}] Bets={roi['bets']} | Win%={roi['hit_rate_pct']:.1f}% | "
                 f"Profit=${roi['total_profit']:.2f} | ROI={roi['roi_pct']:.1f}%")

    return result


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    args = parser.parse_args()
    result = run_backtest(args.start, args.end)
    print(json.dumps(result, indent=2))
