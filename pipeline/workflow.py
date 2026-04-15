"""
MLB Home Run Bot — Daily Workflow Orchestrator
Runs the full pipeline: ingest → features → rank → optimize → export.
Can be run manually or scheduled via Windows Task Scheduler.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import csv
from datetime import date, datetime, timezone
from pathlib import Path

from config import EXPORT_DIR, LOG_DIR
from data.schema import init_db, get_connection
from pipeline.ingest_mlb import run_ingest_schedule, run_ingest_lineups
from pipeline.ingest_statcast import run_ingest_batter_statcast, run_ingest_pitcher_statcast
from pipeline.ingest_weather import run_ingest_weather
from pipeline.ingest_odds import fetch_and_store_odds
from models.ranker import run_ranking
from models.optimizer import build_parlays, save_recommendations

# Setup logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"workflow_{date.today().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def export_card(card: dict, game_date: date):
    """Export daily card as JSON and CSV."""
    date_str = game_date.strftime("%Y-%m-%d")

    # JSON export
    json_path = EXPORT_DIR / f"card_{date_str}.json"
    with open(json_path, "w") as f:
        json.dump(card, f, indent=2, default=str)
    log.info(f"Exported JSON: {json_path}")

    # CSV export — flat singles + parlays
    csv_path = EXPORT_DIR / f"card_{date_str}.csv"
    rows = []
    for s in card.get("singles", []):
        rows.append({
            "date": date_str, "type": "single", "label": s["label"],
            "player": s["player_name"], "odds": s["best_odds"],
            "book": s["best_book"], "model_prob": s["cal_prob"],
            "implied_prob": s["implied_prob"], "edge": s["edge"],
            "stake": s["stake"], "ev": s["expected_value"],
            "reasons": "; ".join(s.get("reasons",[])),
        })
    for plist, ptype in [
        (card.get("parlay_2",[]), "parlay_2"),
        (card.get("parlay_3",[]), "parlay_3"),
        (card.get("parlay_4",[]), "parlay_4"),
    ]:
        for p in plist:
            rows.append({
                "date": date_str, "type": ptype, "label": p["leg_labels"],
                "player": " + ".join(p["leg_names"]),
                "odds": p["american_odds"], "book": "",
                "model_prob": p["combined_prob"], "implied_prob": "",
                "edge": "", "stake": p["stake"], "ev": p["expected_value"],
                "reasons": "",
            })

    if rows:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"Exported CSV: {csv_path}")

    return json_path, csv_path


def run_morning(game_date: date | None = None):
    """Morning run: schedule, Statcast, early odds, weather."""
    if game_date is None:
        game_date = date.today()
    log.info(f"=== MORNING RUN: {game_date} ===")

    init_db()
    log.info("Ingesting schedule...")
    run_ingest_schedule(game_date)

    log.info("Ingesting Statcast batters...")
    run_ingest_batter_statcast()

    log.info("Ingesting Statcast pitchers...")
    run_ingest_pitcher_statcast()

    log.info("Ingesting weather...")
    run_ingest_weather(game_date)

    log.info("Ingesting early odds...")
    fetch_and_store_odds(snapshot_type="morning", game_date=game_date)

    log.info("Running preliminary ranking...")
    ranked = run_ranking(game_date, run_stage="morning")
    card = build_parlays(ranked)
    log.info(f"Morning card: {len(card['singles'])} singles, {len(card['parlay_2'])} 2-legs")
    return card


def run_post_lineup(game_date: date | None = None):
    """
    Post-lineup run: confirm orders, refresh odds, produce final card.
    This is the most important run — call it ~30 min before first pitch.
    """
    if game_date is None:
        game_date = date.today()
    log.info(f"=== POST-LINEUP RUN: {game_date} ===")

    log.info("Confirming lineups...")
    run_ingest_lineups(game_date)

    log.info("Refreshing odds (pre_lineup snapshot)...")
    fetch_and_store_odds(snapshot_type="pre_lineup", game_date=game_date)

    log.info("Running final ranking...")
    ranked = run_ranking(game_date, run_stage="final")
    card = build_parlays(ranked)
    save_recommendations(card, game_date)

    # Export
    export_card(card, game_date)

    log.info("=== FINAL CARD ===")
    for s in card.get("singles", []):
        log.info(
            f"  [{s['label']}] {s['player_name']} | "
            f"Model: {s['cal_prob']:.1%} | Implied: {s['implied_prob']:.1%} | "
            f"Edge: {s['edge']:+.1%} | Odds: {s['best_odds']:+d} ({s['best_book']}) | "
            f"Stake: ${s['stake']:.0f} | EV: ${s['expected_value']:.2f}"
        )
    for p in card.get("parlay_2", []):
        log.info(f"  [2-LEG {p['leg_labels']}] {' + '.join(p['leg_names'])} | "
                 f"Odds: +{p['american_odds']} | Stake: ${p['stake']:.0f} | EV: ${p['expected_value']:.2f}")
    for p in card.get("parlay_3", []):
        log.info(f"  [3-LEG {p['leg_labels']}] {' + '.join(p['leg_names'])} | "
                 f"Odds: +{p['american_odds']} | Stake: ${p['stake']:.0f}")
    for p in card.get("parlay_4", []):
        log.info(f"  [4-LEG ABCD] {' + '.join(p['leg_names'])} | "
                 f"Odds: +{p['american_odds']} | Stake: ${p['stake']:.0f}")

    return card


def run_full_day(game_date: date | None = None):
    """Run morning + post-lineup in one shot (for manual/testing use)."""
    if game_date is None:
        game_date = date.today()
    run_morning(game_date)
    return run_post_lineup(game_date)


def record_result(recommendation_id: int, won: bool, payout: float, settled_at: str | None = None):
    """Record actual outcome of a bet."""
    conn = get_connection()
    stake_row = conn.execute(
        "SELECT stake FROM bet_recommendations WHERE id=?", (recommendation_id,)
    ).fetchone()
    stake = stake_row["stake"] if stake_row else 0.0
    profit = (payout - stake) if won else -stake

    conn.execute("""
        INSERT INTO bet_results(recommendation_id,bet_date,settled_at,won,payout,profit)
        VALUES(?,?,?,?,?,?)
    """, (
        recommendation_id,
        date.today().strftime("%Y-%m-%d"),
        settled_at or _now_utc(),
        1 if won else 0,
        payout if won else 0.0,
        profit,
    ))
    conn.commit()
    conn.close()
    log.info(f"Result recorded: rec_id={recommendation_id} won={won} profit={profit:.2f}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MLB HR Bot Workflow")
    parser.add_argument("--stage", choices=["morning","post_lineup","full"], default="full")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else date.today()

    if args.stage == "morning":
        run_morning(run_date)
    elif args.stage == "post_lineup":
        run_post_lineup(run_date)
    else:
        run_full_day(run_date)
