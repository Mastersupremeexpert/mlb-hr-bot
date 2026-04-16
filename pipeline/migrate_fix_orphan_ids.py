"""
MLB Home Run Bot — One-time Migration: Fix Orphaned Hash IDs

Your existing sportsbook_odds table has rows pointing to hash-generated
player IDs (>= 1,000,000) that never matched the real MLBAM IDs used by
the lineups/statcast ingests. This script heals that by:

  1. Finding every hash-ID in `players` that has a matching real MLBAM ID
     (same full_name, player_id < 1,000,000).
  2. Re-pointing all sportsbook_odds, closing_lines, model_predictions,
     and bet_recommendations rows to the real MLBAM ID.
  3. Deleting the now-orphaned hash-ID rows from `players`.

Safe to run multiple times — only acts on rows still pointing to hash IDs.

USAGE (from Railway shell or locally):
    python -m pipeline.migrate_fix_orphan_ids
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging

from data.schema import get_connection, execute, fetchall, _is_postgres

log = logging.getLogger(__name__)


def find_hash_to_real_mapping(conn) -> dict[int, int]:
    """
    Find every hash player_id that shares a full_name with a real MLBAM ID.
    Returns {hash_id: real_mlbam_id}.
    """
    rows = fetchall(conn, """
        SELECT h.player_id AS hash_id, r.player_id AS real_id, h.full_name
        FROM players h
        JOIN players r ON r.full_name = h.full_name AND r.player_id < 1000000
        WHERE h.player_id >= 1000000
    """)
    mapping = {}
    for r in rows:
        mapping[int(r["hash_id"])] = int(r["real_id"])
    return mapping


def repoint_table(conn, table: str, mapping: dict[int, int]) -> int:
    """Update player_id columns in a table to real MLBAM IDs."""
    if not mapping:
        return 0

    moved = 0
    for hash_id, real_id in mapping.items():
        try:
            # Some rows might already exist for real_id — just overwrite
            cur = execute(
                conn,
                f"UPDATE {table} SET player_id = ? WHERE player_id = ?",
                (real_id, hash_id),
            )
            # rowcount isn't always available across drivers — estimate from
            # before/after counts if needed
            if hasattr(cur, "rowcount") and cur.rowcount and cur.rowcount > 0:
                moved += cur.rowcount
        except Exception as e:
            log.warning(f"  Could not repoint {table} {hash_id}->{real_id}: {e}")

    return moved


def repoint_bet_recommendations(conn, mapping: dict[int, int]) -> int:
    """
    bet_recommendations.legs is a JSON array of player_ids — update the JSON.
    """
    if not mapping:
        return 0

    rows = fetchall(
        conn,
        "SELECT id, legs FROM bet_recommendations WHERE legs IS NOT NULL",
    )
    updated = 0
    for r in rows:
        try:
            legs = json.loads(r["legs"]) if isinstance(r["legs"], str) else r["legs"]
            if not isinstance(legs, list):
                continue
            new_legs = [mapping.get(int(pid), int(pid)) for pid in legs]
            if new_legs != legs:
                execute(
                    conn,
                    "UPDATE bet_recommendations SET legs = ? WHERE id = ?",
                    (json.dumps(new_legs), r["id"]),
                )
                updated += 1
        except Exception as e:
            log.debug(f"  Skipping recommendation {r.get('id')}: {e}")
    return updated


def delete_orphan_hash_players(conn, mapping: dict[int, int]) -> int:
    """Remove the hash-ID player rows after their data was re-pointed."""
    if not mapping:
        return 0
    deleted = 0
    for hash_id in mapping.keys():
        try:
            execute(
                conn,
                "DELETE FROM players WHERE player_id = ?",
                (hash_id,),
            )
            deleted += 1
        except Exception as e:
            log.warning(f"  Could not delete hash player {hash_id}: {e}")
    return deleted


def run_migration():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log.info("=" * 70)
    log.info("MIGRATION: Repoint orphaned hash IDs to real MLBAM IDs")
    log.info("=" * 70)
    log.info(f"Database: {'PostgreSQL (Railway)' if _is_postgres() else 'SQLite (local)'}")

    conn = get_connection()

    mapping = find_hash_to_real_mapping(conn)
    if not mapping:
        log.info("Nothing to migrate. No hash-ID duplicates found. ✅")
        conn.close()
        return

    log.info(f"Found {len(mapping)} hash IDs with matching real MLBAM IDs.")
    for hid, rid in list(mapping.items())[:10]:
        log.info(f"  {hid} -> {rid}")
    if len(mapping) > 10:
        log.info(f"  ... and {len(mapping) - 10} more")

    tables_to_repoint = [
        "sportsbook_odds",
        "closing_lines",
        "model_predictions",
        "batter_statcast",
        "pitcher_statcast",
        "lineups",
        "bet_results",
    ]
    for tbl in tables_to_repoint:
        try:
            log.info(f"Repointing {tbl}...")
            repoint_table(conn, tbl, mapping)
        except Exception as e:
            log.warning(f"  {tbl}: {e}")

    log.info("Repointing bet_recommendations (JSON legs)...")
    updated = repoint_bet_recommendations(conn, mapping)
    log.info(f"  Updated {updated} recommendations.")

    log.info("Deleting orphan hash-ID player rows...")
    deleted = delete_orphan_hash_players(conn, mapping)
    log.info(f"  Deleted {deleted} hash-ID player rows.")

    conn.commit()
    conn.close()
    log.info("=" * 70)
    log.info("Migration complete. ✅ Re-run the pipeline to verify edges populate.")
    log.info("=" * 70)


if __name__ == "__main__":
    run_migration()
