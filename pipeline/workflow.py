"""
MLB Home Run Bot — Daily Workflow Orchestrator
Runs the full pipeline: ingest → features → rank → optimize → export.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import csv
from datetime import date, datetime, timezone
from pathlib import Path

from config import EXPORT_DIR, LOG_DIR
from data.schema import init_db, get_connection, execute

log = logging.getLogger(__name__)


def _ensure_dirs():
    """Create runtime directories — called lazily, not at import time."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _setup_logging():
    _ensure_dirs()
    if not logging.getLogger().handlers:
        handlers = [logging.StreamHandler(sys.stdout)]
        try:
            log_file = LOG_DIR / f"workflow_{date.today().strftime('%Y%m%d')}.log"
            handlers.append(logging.FileHandler(log_file))
        except Exception:
            pass
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            handlers=handlers,
        )


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def export_card(card: dict, game_date: date):
    """Export daily card as JSON and CSV."""
    _ensure_dirs()
    date_str = game_date.strftime("%Y-%m-%d")

    # JSON export
    try:
        json_path = EXPORT_DIR / f"card_{date_str}.json"
        with open(json_path, "w") as f:
            json.dump(card, f, indent=2, default=str)
        log.info(f"Exported JSON: {json_path}")
    except Exception as e:
        log.warning(f"Could not export JSON: {e}")
        json_path = None

    # CSV export — flat singles + parlays
    try:
        csv_path = EXPORT_DIR / f"card_{date_str}.csv"
        rows = []
        for s in card.get("singles", []):
            rows.append({
                "date": date_str, "type": "single", "label": s.get("label", ""),
                "player": s.get("player_name", ""), "odds": s.get("best_odds", ""),
                "book": s.get("best_book", ""), "model_prob": s.get("cal_prob", ""),
                "implied_prob": s.get("implied_prob", ""), "edge": s.get("edge", ""),
                "stake": s.get("stake", ""), "ev": s.get("expected_value", ""),
                "reasons": "; ".join(s.get("reasons", [])),
            })
        for plist, ptype in [
            (card.get("parlay_2", []), "parlay_2"),
            (card.get("parlay_3", []), "parlay_3"),
            (card.get("parlay_4", []), "parlay_4"),
        ]:
            for p in plist:
                rows.append({
                    "date": date_str, "type": ptype,
                    "label": p.get("leg_labels", ""),
                    "player": " + ".join(p.get("leg_names", [])),
                    "odds": p.get("american_odds", ""), "book": "",
                    "model_prob": p.get("combined_prob", ""), "implied_prob": "",
                    "edge": "", "stake": p.get("stake", ""),
                    "ev": p.get("expected_value", ""), "reasons": "",
                })
        if rows:
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            log.info(f"Exported CSV: {csv_path}")
    except Exception as e:
        log.warning(f"Could not export CSV: {e}")
        csv_path = None

    return json_path, csv_path


def _remap_fake_player_ids(conn):
    """
    Cleanup pass: find sportsbook_odds rows attached to fake hash IDs
    (player_id >= 10,000,000) and remap them to the real MLBAM ID by
    matching on full_name. Deletes any rows that can't be remapped.
    Run this right before ranking so picks always get real odds.
    """
    from data.schema import fetchall, execute as db_execute
    try:
        fake_rows = fetchall(conn, """
            SELECT DISTINCT so.player_id, p.full_name
            FROM sportsbook_odds so
            JOIN players p ON p.player_id = so.player_id
            WHERE so.player_id >= 10000000
        """, ())

        remapped = 0
        deleted = 0
        for row in fake_rows:
            fake_id  = row["player_id"]
            name     = row["full_name"]
            # Find the real MLBAM player with the same name
            real_row = fetchall(conn, """
                SELECT player_id FROM players
                WHERE full_name = ? AND player_id < 10000000
                ORDER BY player_id ASC LIMIT 1
            """, (name,))
            if real_row:
                real_id = real_row[0]["player_id"]
                db_execute(conn, """
                    UPDATE sportsbook_odds SET player_id = ?
                    WHERE player_id = ?
                """, (real_id, fake_id))
                remapped += 1
                log.info(f"  Remapped odds: '{name}' {fake_id} -> {real_id}")
            else:
                # No real player found — drop these orphan odds rows
                db_execute(conn, "DELETE FROM sportsbook_odds WHERE player_id = ?", (fake_id,))
                deleted += 1
                log.debug(f"  Deleted unresolvable odds for '{name}' (fake id {fake_id})")

        if remapped or deleted:
            conn.commit()
            log.info(f"ID cleanup: {remapped} remapped, {deleted} deleted.")
        else:
            log.info("ID cleanup: no fake IDs found.")
    except Exception as e:
        log.warning(f"ID cleanup failed (non-fatal): {e}")


def run_morning(game_date: date | None = None):
    """Morning run: schedule, Statcast, early odds, weather."""
    _setup_logging()
    if game_date is None:
        game_date = date.today()
    log.info(f"=== MORNING RUN: {game_date} ===")

    from pipeline.ingest_mlb import run_ingest_schedule, run_ingest_lineups
    from pipeline.ingest_statcast import run_ingest_batter_statcast, run_ingest_pitcher_statcast
    from pipeline.ingest_weather import run_ingest_weather
    from pipeline.ingest_odds import fetch_and_store_odds
    from models.ranker import run_ranking
    from models.optimizer import build_parlays

    init_db()
    log.info("Ingesting schedule...")
    run_ingest_schedule(game_date)

    log.info("Ingesting Statcast batters...")
    run_ingest_batter_statcast()

    log.info("Ingesting Statcast pitchers...")
    run_ingest_pitcher_statcast()

    log.info("Ingesting advanced pitcher/batter stats (HR/9, xFIP, Z-score)...")
    from pipeline.ingest_fangraphs import run_ingest_advanced
    run_ingest_advanced()

    log.info("Ingesting weather...")
    run_ingest_weather(game_date)

    log.info("Ingesting early odds...")
    fetch_and_store_odds(snapshot_type="morning", game_date=game_date)

    log.info("Remapping any fake player IDs to real MLBAM IDs...")
    _conn = get_connection()
    _remap_fake_player_ids(_conn)
    _conn.close()

    log.info("Running preliminary ranking...")
    ranked = run_ranking(game_date, run_stage="morning")
    card = build_parlays(ranked)
    log.info(f"Morning card: {len(card.get('singles',[]))} singles")
    return card


def run_post_lineup(game_date: date | None = None):
    """Post-lineup run: confirm lineups, refresh odds, produce final card."""
    _setup_logging()
    if game_date is None:
        game_date = date.today()
    log.info(f"=== POST-LINEUP RUN: {game_date} ===")

    from pipeline.ingest_mlb import run_ingest_schedule, run_ingest_lineups
    from pipeline.ingest_odds import fetch_and_store_odds
    from models.ranker import run_ranking
    from models.optimizer import build_parlays, save_recommendations

    log.info("Confirming lineups...")
    run_ingest_lineups(game_date)

    log.info("Refreshing odds...")
    fetch_and_store_odds(snapshot_type="pre_lineup", game_date=game_date)

    log.info("Remapping any fake player IDs to real MLBAM IDs...")
    _conn = get_connection()
    _remap_fake_player_ids(_conn)
    _conn.close()

    log.info("Running final ranking...")
    ranked = run_ranking(game_date, run_stage="final")
    card = build_parlays(ranked)
    save_recommendations(card, game_date)
    export_card(card, game_date)

    log.info("=== FINAL CARD ===")
    for s in card.get("singles", []):
        log.info(
            f"  [{s.get('label')}] {s.get('player_name')} | "
            f"Edge: {s.get('edge', 0):+.1%} | Odds: {s.get('best_odds', 0):+d}"
        )
    return card


def run_pre_game_snapshot(game_date: date | None = None):
    """Capture closing lines ~10 min before first pitch."""
    _setup_logging()
    if game_date is None:
        game_date = date.today()
    log.info(f"=== PRE-GAME SNAPSHOT: {game_date} ===")
    from pipeline.capture_closing_lines import run_closing_line_capture
    return run_closing_line_capture(game_date)


def run_post_game_settle(game_date: date | None = None):
    """
    Auto-settle results after games finish (~11 PM ET).
    Also triggers ML retrain if enough new labeled samples have accumulated.
    """
    _setup_logging()
    if game_date is None:
        from datetime import timedelta
        game_date = date.today() - timedelta(days=1)
    log.info(f"=== POST-GAME SETTLE: {game_date} ===")
    from pipeline.settle_results import run_auto_settle
    result = run_auto_settle(game_date)

    # ── Auto-retrain check ───────────────────────────────────────────────
    # After settlement, check if we have enough labeled samples to retrain.
    # Threshold: 100 samples to train, then retrain every 50 new samples.
    _maybe_retrain()

    return result


def _maybe_retrain():
    """
    Retrain XGBoost model if enough new labeled samples exist.
    - First train: 100 samples
    - Subsequent retrains: every 50 new samples
    Runs synchronously (takes ~10-30 seconds), safe after game settlement.
    """
    try:
        conn = get_connection()
        from data.schema import fetchone as db_fetchone

        # Count total labeled samples
        total = db_fetchone(conn, """
            SELECT COUNT(*) as cnt
            FROM model_predictions mp
            JOIN bet_recommendations rec ON (
                rec.bet_date = substr(mp.run_timestamp,1,10)
                AND rec.bet_type = 'single'
                AND rec.legs = json_array(mp.player_id)
            )
            JOIN bet_results br ON br.recommendation_id = rec.id
            WHERE br.won IS NOT NULL
        """)
        total_samples = total["cnt"] if total else 0

        # Count samples since last retrain
        last_train = db_fetchone(conn,
            "SELECT num_samples FROM training_log ORDER BY trained_at DESC LIMIT 1")
        last_n = last_train["num_samples"] if last_train else 0
        conn.close()

        new_samples = total_samples - last_n
        FIRST_TRAIN_THRESHOLD  = 100
        RETRAIN_INTERVAL       = 50

        should_train = (
            (last_n == 0 and total_samples >= FIRST_TRAIN_THRESHOLD) or
            (last_n > 0  and new_samples  >= RETRAIN_INTERVAL)
        )

        if should_train:
            log.info(f"Auto-retraining: {total_samples} total samples ({new_samples} new)...")
            from models.train import train_model
            train_model()
            log.info("Auto-retrain complete.")
        else:
            log.info(
                f"Retrain check: {total_samples} samples total, "
                f"{new_samples} since last train. "
                f"Need {FIRST_TRAIN_THRESHOLD if last_n==0 else RETRAIN_INTERVAL} to trigger."
            )
    except Exception as e:
        log.warning(f"Auto-retrain check failed (non-fatal): {e}")


def run_full_day(game_date: date | None = None):
    """Run morning + post-lineup in one shot."""
    _setup_logging()
    if game_date is None:
        game_date = date.today()
    run_morning(game_date)
    return run_post_lineup(game_date)


def record_result(recommendation_id: int, won: bool, payout: float, settled_at: str | None = None):
    """Record actual outcome of a bet."""
    conn = get_connection()
    try:
        stake_row_data = None
        try:
            from data.schema import fetchone
            stake_row_data = fetchone(conn, "SELECT stake FROM bet_recommendations WHERE id=?", (recommendation_id,))
        except Exception:
            pass
        stake = stake_row_data["stake"] if stake_row_data else 0.0
        profit = (payout - stake) if won else -stake

        execute(conn, """
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
        log.info(f"Result recorded: rec_id={recommendation_id} won={won} profit={profit:.2f}")
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MLB HR Bot Workflow")
    parser.add_argument("--stage",
        choices=["morning", "post_lineup", "full", "closing", "settle"],
        default="full")
    parser.add_argument("--date", type=str, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    run_date = date.fromisoformat(args.date) if args.date else date.today()
    if args.stage == "morning":
        run_morning(run_date)
    elif args.stage == "post_lineup":
        run_post_lineup(run_date)
    elif args.stage == "closing":
        run_pre_game_snapshot(run_date)
    elif args.stage == "settle":
        run_post_game_settle(run_date)
    else:
        run_full_day(run_date)
