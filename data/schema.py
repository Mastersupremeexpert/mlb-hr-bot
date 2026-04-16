"""
MLB Home Run Bot — Database Schema (PostgreSQL)
Uses psycopg2 with the DATABASE_URL injected by Railway.
Falls back to SQLite for local development if DATABASE_URL is not set.
"""

import os
import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── Connection factory ────────────────────────────────────────────────────

def get_connection():
    """
    Returns a database connection.
    - Cloud (Railway): PostgreSQL via DATABASE_URL
    - Local fallback: SQLite
    """
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        # Railway sometimes uses postgres:// — psycopg2 needs postgresql://
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
        conn.autocommit = False
        return conn
    else:
        # Local SQLite fallback
        import sqlite3
        from pathlib import Path
        db_path = Path(__file__).parent / "mlb_hr_bot.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn


def _is_postgres() -> bool:
    return bool(DATABASE_URL)


def _placeholder() -> str:
    """SQL placeholder — %s for Postgres, ? for SQLite."""
    return "%s" if _is_postgres() else "?"


def execute(conn, sql: str, params=None):
    """
    Unified execute that works for both psycopg2 and sqlite3.
    Replaces ? with %s automatically for Postgres.
    """
    if _is_postgres():
        sql = sql.replace("?", "%s")
        # Postgres uses SERIAL / RETURNING instead of lastrowid
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        # Convert SQLite INSERT OR IGNORE → Postgres INSERT ... ON CONFLICT DO NOTHING
        import re
        sql = re.sub(
            r'INSERT\s+OR\s+IGNORE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)(?!\s*ON CONFLICT)',
            r'INSERT INTO \1 (\2) VALUES (\3) ON CONFLICT DO NOTHING',
            sql, flags=re.IGNORECASE
        )
        # Also handle any remaining INSERT OR IGNORE without ON CONFLICT (belt+suspenders)
        sql = re.sub(r'INSERT\s+OR\s+IGNORE\s+INTO', 'INSERT INTO', sql, flags=re.IGNORECASE)
        sql = sql.replace("INSERT OR REPLACE INTO", "INSERT INTO")
        # SQLite json_array(x) → Postgres cast('[' || x || ']' as text) for legs TEXT matching
        # legs is stored as json.dumps([player_id]) = '[12345]' (a TEXT column)
        # So we compare legs = '[' || player_id::text || ']'
        sql = re.sub(
            r'\bjson_array\(([^)]+)\)',
            r"('[' || (\1)::text || ']')",
            sql, flags=re.IGNORECASE
        )
        # SQLite AUTOINCREMENT already handled above
    cur = conn.cursor()
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    return cur


def fetchall(conn, sql: str, params=None) -> list:
    cur = execute(conn, sql, params)
    rows = cur.fetchall()
    # Normalize to list of dicts
    if rows and not isinstance(rows[0], dict):
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    return [dict(r) for r in rows]


def fetchone(conn, sql: str, params=None):
    cur = execute(conn, sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    if not isinstance(row, dict):
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    return dict(row)


# ── Schema creation ───────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS players (
    player_id       BIGINT PRIMARY KEY,
    full_name       TEXT NOT NULL,
    bats            TEXT,
    throws          TEXT,
    primary_position TEXT,
    team_id         BIGINT,
    team_abbr       TEXT,
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS teams (
    team_id         BIGINT PRIMARY KEY,
    name            TEXT,
    abbr            TEXT,
    venue_id        BIGINT,
    division        TEXT,
    league          TEXT
);

CREATE TABLE IF NOT EXISTS venues (
    venue_id        BIGINT PRIMARY KEY,
    name            TEXT,
    city            TEXT,
    state           TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    altitude_ft     DOUBLE PRECISION,
    roof_type       TEXT,
    surface         TEXT,
    lf_dist         INTEGER,
    cf_dist         INTEGER,
    rf_dist         INTEGER,
    hr_factor_rhb   DOUBLE PRECISION DEFAULT 1.0,
    hr_factor_lhb   DOUBLE PRECISION DEFAULT 1.0
);

CREATE TABLE IF NOT EXISTS games (
    game_pk         BIGINT PRIMARY KEY,
    game_date       TEXT NOT NULL,
    game_time_utc   TEXT,
    home_team_id    BIGINT,
    away_team_id    BIGINT,
    venue_id        BIGINT,
    status          TEXT,
    home_score      INTEGER,
    away_score      INTEGER
);

CREATE TABLE IF NOT EXISTS probable_pitchers (
    id              SERIAL PRIMARY KEY,
    game_pk         BIGINT,
    pitcher_id      BIGINT,
    side            TEXT,
    confirmed       INTEGER DEFAULT 0,
    fetched_at      TEXT
);

CREATE TABLE IF NOT EXISTS lineups (
    id              SERIAL PRIMARY KEY,
    game_pk         BIGINT,
    player_id       BIGINT,
    team_id         BIGINT,
    batting_order   INTEGER,
    position        TEXT,
    confirmed       INTEGER DEFAULT 0,
    fetched_at      TEXT
);

CREATE TABLE IF NOT EXISTS batter_statcast (
    id              SERIAL PRIMARY KEY,
    player_id       BIGINT,
    season          INTEGER,
    game_date       TEXT,
    avg_exit_velocity   DOUBLE PRECISION,
    avg_launch_angle    DOUBLE PRECISION,
    barrel_rate         DOUBLE PRECISION,
    hard_hit_rate       DOUBLE PRECISION,
    sweet_spot_rate     DOUBLE PRECISION,
    bat_speed_avg       DOUBLE PRECISION,
    blast_rate          DOUBLE PRECISION,
    flyball_rate        DOUBLE PRECISION,
    groundball_rate     DOUBLE PRECISION,
    pull_rate_airball   DOUBLE PRECISION,
    strikeout_rate      DOUBLE PRECISION,
    walk_rate           DOUBLE PRECISION,
    chase_rate          DOUBLE PRECISION,
    whiff_rate          DOUBLE PRECISION,
    contact_rate        DOUBLE PRECISION,
    xslg                DOUBLE PRECISION,
    xwoba               DOUBLE PRECISION,
    iso                 DOUBLE PRECISION,
    hr_per_pa           DOUBLE PRECISION,
    pitcher_hand        TEXT,
    window_days         INTEGER,
    pa_count            INTEGER,
    fetched_at          TEXT
);

CREATE TABLE IF NOT EXISTS pitcher_statcast (
    id              SERIAL PRIMARY KEY,
    player_id       BIGINT,
    season          INTEGER,
    game_date       TEXT,
    hr9             DOUBLE PRECISION,
    hr_per_bf       DOUBLE PRECISION,
    barrel_rate_allowed DOUBLE PRECISION,
    hard_hit_rate_allowed DOUBLE PRECISION,
    avg_launch_angle_allowed DOUBLE PRECISION,
    avg_exit_velocity_allowed DOUBLE PRECISION,
    flyball_rate_allowed DOUBLE PRECISION,
    ff_usage_pct    DOUBLE PRECISION,
    si_usage_pct    DOUBLE PRECISION,
    fc_usage_pct    DOUBLE PRECISION,
    sl_usage_pct    DOUBLE PRECISION,
    cu_usage_pct    DOUBLE PRECISION,
    ch_usage_pct    DOUBLE PRECISION,
    avg_fb_velo     DOUBLE PRECISION,
    avg_fb_ivb      DOUBLE PRECISION,
    hr_per_ff       DOUBLE PRECISION,
    hr_per_sl       DOUBLE PRECISION,
    hr_per_ch       DOUBLE PRECISION,
    zone_rate       DOUBLE PRECISION,
    first_pitch_strike_rate DOUBLE PRECISION,
    putaway_rate    DOUBLE PRECISION,
    batter_hand     TEXT,
    window_days     INTEGER,
    bf_count        INTEGER,
    days_rest       INTEGER,
    fetched_at      TEXT,
    whiff_rate      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS environment_features (
    id              SERIAL PRIMARY KEY,
    game_pk         BIGINT,
    venue_id        BIGINT,
    fetched_at      TEXT,
    game_time_local TEXT,
    temperature_f   DOUBLE PRECISION,
    humidity_pct    DOUBLE PRECISION,
    wind_speed_mph  DOUBLE PRECISION,
    wind_dir_deg    DOUBLE PRECISION,
    wind_dir_label  TEXT,
    roof_open       INTEGER,
    air_density_idx DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS sportsbook_odds (
    id              SERIAL PRIMARY KEY,
    game_pk         BIGINT,
    player_id       BIGINT,
    bookmaker       TEXT,
    market          TEXT DEFAULT 'batter_home_runs',
    american_odds   INTEGER,
    implied_prob    DOUBLE PRECISION,
    fetched_at      TEXT,
    snapshot_type   TEXT
);

CREATE TABLE IF NOT EXISTS model_predictions (
    id              SERIAL PRIMARY KEY,
    game_pk         BIGINT,
    player_id       BIGINT,
    run_timestamp   TEXT,
    run_stage       TEXT,
    model_prob      DOUBLE PRECISION,
    calibrated_prob DOUBLE PRECISION,
    best_implied_prob DOUBLE PRECISION,
    best_odds       INTEGER,
    best_bookmaker  TEXT,
    edge            DOUBLE PRECISION,
    stability_score DOUBLE PRECISION,
    player_score    DOUBLE PRECISION,
    rank_label      TEXT,
    reason_codes    TEXT,
    projected_pa    DOUBLE PRECISION,
    batting_order   INTEGER,
    confirmed_lineup INTEGER DEFAULT 0,
    pre_ai_prob     DOUBLE PRECISION,
    ai_analysis     TEXT
);

CREATE TABLE IF NOT EXISTS bet_recommendations (
    id              SERIAL PRIMARY KEY,
    bet_date        TEXT NOT NULL,
    bet_type        TEXT,
    legs            TEXT,
    leg_labels      TEXT,
    combo_score     DOUBLE PRECISION,
    stake           DOUBLE PRECISION,
    expected_payout DOUBLE PRECISION,
    expected_value  DOUBLE PRECISION,
    american_odds   INTEGER,
    created_at      TEXT
);

CREATE TABLE IF NOT EXISTS bet_results (
    id              SERIAL PRIMARY KEY,
    recommendation_id BIGINT,
    bet_date        TEXT,
    settled_at      TEXT,
    won             INTEGER,
    payout          DOUBLE PRECISION,
    profit          DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS closing_lines (
    id              SERIAL PRIMARY KEY,
    game_pk         BIGINT,
    player_id       BIGINT,
    bookmaker       TEXT,
    closing_odds    INTEGER,
    closing_implied_prob DOUBLE PRECISION,
    locked_at       TEXT
);

CREATE TABLE IF NOT EXISTS pitcher_advanced (
    player_id       BIGINT,
    season          INTEGER,
    ip              DOUBLE PRECISION,
    hr9             DOUBLE PRECISION,
    k9              DOUBLE PRECISION,
    bb9             DOUBLE PRECISION,
    era             DOUBLE PRECISION,
    whip            DOUBLE PRECISION,
    xfip            DOUBLE PRECISION,
    hr_allowed      INTEGER,
    hr_per_bf       DOUBLE PRECISION,
    fetched_at      TEXT,
    PRIMARY KEY (player_id, season)
);

CREATE TABLE IF NOT EXISTS batter_advanced (
    player_id       BIGINT,
    season          INTEGER,
    hr              INTEGER,
    pa              INTEGER,
    games_played    INTEGER,
    hr_per_pa       DOUBLE PRECISION,
    hr_per_game     DOUBLE PRECISION,
    z_score_due     DOUBLE PRECISION,
    slg             DOUBLE PRECISION,
    iso             DOUBLE PRECISION,
    k_rate          DOUBLE PRECISION,
    bb_rate         DOUBLE PRECISION,
    fetched_at      TEXT,
    PRIMARY KEY (player_id, season)
);

CREATE TABLE IF NOT EXISTS training_log (
    id              SERIAL PRIMARY KEY,
    trained_at      TEXT,
    model_version   TEXT,
    num_samples     INTEGER,
    brier_score     DOUBLE PRECISION,
    log_loss        DOUBLE PRECISION,
    auc             DOUBLE PRECISION,
    calibration_json TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS feature_drift_log (
    id              SERIAL PRIMARY KEY,
    logged_at       TEXT,
    feature_name    TEXT,
    mean_train      DOUBLE PRECISION,
    mean_live       DOUBLE PRECISION,
    drift_pct       DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_lineups_game ON lineups(game_pk);
CREATE INDEX IF NOT EXISTS idx_odds_game_player ON sportsbook_odds(game_pk, player_id);
CREATE INDEX IF NOT EXISTS idx_predictions_game_player ON model_predictions(game_pk, player_id);
CREATE INDEX IF NOT EXISTS idx_batter_statcast_player ON batter_statcast(player_id, window_days);
CREATE INDEX IF NOT EXISTS idx_pitcher_statcast_player ON pitcher_statcast(player_id, window_days);
"""


# Migrations: columns added after initial schema deploy.
# Each entry: (table, column, type) — safe to run repeatedly (IF NOT EXISTS)
_MIGRATIONS = [
    ("model_predictions", "pre_ai_prob",         "DOUBLE PRECISION"),
    ("model_predictions", "ai_confidence",        "DOUBLE PRECISION"),
    ("model_predictions", "ai_reasoning",         "TEXT"),
    ("bet_recommendations", "thin_edge",          "BOOLEAN DEFAULT FALSE"),
    ("pitcher_advanced",  "hr9",                  "DOUBLE PRECISION"),
    ("pitcher_advanced",  "xfip",                 "DOUBLE PRECISION"),
    ("pitcher_advanced",  "k9",                   "DOUBLE PRECISION"),
    ("pitcher_advanced",  "bb9",                  "DOUBLE PRECISION"),
    ("pitcher_advanced",  "era",                  "DOUBLE PRECISION"),
    ("pitcher_advanced",  "whip",                 "DOUBLE PRECISION"),
    ("pitcher_advanced",  "hr_per_bf",            "DOUBLE PRECISION"),
    ("pitcher_advanced",  "ip",                   "DOUBLE PRECISION"),
    ("batter_advanced",   "hr_per_pa",            "DOUBLE PRECISION"),
    ("batter_advanced",   "hr_per_game",          "DOUBLE PRECISION"),
    ("batter_advanced",   "z_score_due",          "DOUBLE PRECISION"),
    ("batter_advanced",   "hr",                   "INTEGER"),
    ("batter_advanced",   "pa",                   "INTEGER"),
    ("batter_advanced",   "games_played",         "INTEGER"),
    ("batter_advanced",   "iso",                  "DOUBLE PRECISION"),
    ("batter_advanced",   "k_rate",               "DOUBLE PRECISION"),
]


def _run_migrations(conn):
    """Add any missing columns to existing tables — safe to run on every startup."""
    cur = conn.cursor()
    for table, column, col_type in _MIGRATIONS:
        try:
            if _is_postgres():
                cur.execute(f"""
                    ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}
                """)
            else:
                # SQLite doesn't support IF NOT EXISTS on ALTER TABLE
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} REAL")
                except Exception:
                    pass  # Column already exists
        except Exception as e:
            log.debug(f"Migration {table}.{column}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
    conn.commit()
    log.info("Migrations applied.")


def init_db():
    """Create all tables if they don't exist, then run migrations."""
    conn = get_connection()
    try:
        if _is_postgres():
            cur = conn.cursor()
            # Split on semicolons and run each statement
            statements = [s.strip() for s in _SCHEMA_SQL.split(";") if s.strip()]
            for stmt in statements:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    log.warning(f"Schema statement warning (may already exist): {e}")
                    conn.rollback()
            conn.commit()
        else:
            # SQLite — run as executescript
            conn.executescript(_SCHEMA_SQL.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
                               .replace("DOUBLE PRECISION", "REAL")
                               .replace("BIGINT", "INTEGER"))
            conn.commit()
        # Run column migrations on every startup
        _run_migrations(conn)
        log.info("Database initialized.")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print("DB ready.")
