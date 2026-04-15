"""
MLB Home Run Bot — Model Training
Trains an XGBoost model with calibration on historical predictions + outcomes.
Falls back to a rule-based heuristic when not enough data exists (<100 samples).
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from config import MODEL_DIR
from data.schema import get_connection

log = logging.getLogger(__name__)

MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = MODEL_DIR / "hr_model.pkl"
CALIBRATOR_PATH = MODEL_DIR / "calibrator.pkl"
FEATURE_LIST_PATH = MODEL_DIR / "feature_list.json"

# ── Feature columns the model uses ──────────────────────────────────────────
FEATURE_COLS = [
    # Batter rolling
    "barrel_rate_last_14d", "barrel_rate_last_30d", "barrel_rate_season",
    "avg_ev_last_14d", "avg_ev_last_30d", "avg_ev_season",
    "hard_hit_last_14d", "hard_hit_last_30d",
    "xslg_last_14d", "xslg_season",
    "xwoba_last_14d", "xwoba_season",
    "hr_per_pa_season",
    "flyball_rate_season",
    "k_rate_season", "bb_rate_season",
    "whiff_rate_season",
    "iso_season",
    "bat_speed_season",
    # Pitcher vulnerability
    "p_barrel_rate_allowed", "p_hard_hit_allowed", "p_avg_ev_allowed",
    "p_hr9", "p_hr_per_bf", "p_ff_usage",
    # Environment
    "park_hr_factor", "wind_hr_bonus", "temp_hr_bonus",
    "air_density_idx", "altitude_ft", "roof_open",
    # Opportunity
    "projected_pa", "batting_order",
]


def _load_training_data():
    """Load historical predictions with known outcomes."""
    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT mp.*, br.won
        FROM model_predictions mp
        JOIN bet_results br ON (br.recommendation_id IN (
            SELECT id FROM bet_recommendations
            WHERE legs LIKE '%' || mp.player_id || '%'
              AND bet_date = substr(mp.run_timestamp,1,10)
        ))
        WHERE br.won IS NOT NULL
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _features_to_array(records: list[dict], feature_cols: list[str]) -> np.ndarray:
    """Convert list of dicts to numpy array, filling missing with median."""
    X = []
    for r in records:
        row = [r.get(c) for c in feature_cols]
        X.append(row)
    arr = np.array(X, dtype=float)
    # Fill NaN with column median
    col_medians = np.nanmedian(arr, axis=0)
    inds = np.where(np.isnan(arr))
    arr[inds] = np.take(col_medians, inds[1])
    return arr


def train_model(force_heuristic: bool = False):
    """Train XGBoost + isotonic calibration. Falls back to heuristic if data is sparse."""
    try:
        import xgboost as xgb
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.isotonic import IsotonicRegression
        from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
    except ImportError:
        log.warning("xgboost/sklearn not installed. Using heuristic model only.")
        _save_heuristic_model()
        return

    records = _load_training_data()
    if len(records) < 100 or force_heuristic:
        log.info(f"Only {len(records)} labeled samples. Using heuristic model.")
        _save_heuristic_model()
        return

    X = _features_to_array(records, FEATURE_COLS)
    y = np.array([r["won"] for r in records], dtype=float)

    log.info(f"Training on {len(y)} samples ({int(y.sum())} positives).")

    # XGBoost base model
    from sklearn.model_selection import train_test_split
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.7,
        scale_pos_weight=scale_pos_weight,
        objective="binary:logistic",
        eval_metric="logloss",
        use_label_encoder=False,
        random_state=42,
        early_stopping_rounds=30,
        verbosity=0,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)

    # Calibration with isotonic regression
    raw_probs = model.predict_proba(X_val)[:, 1]
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(raw_probs, y_val)

    # Metrics
    cal_probs = calibrator.predict(raw_probs)
    bs = brier_score_loss(y_val, cal_probs)
    ll = log_loss(y_val, np.clip(cal_probs, 1e-7, 1 - 1e-7))
    auc = roc_auc_score(y_val, cal_probs)
    log.info(f"Val Brier={bs:.4f}  LogLoss={ll:.4f}  AUC={auc:.4f}")

    # Save
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    with open(CALIBRATOR_PATH, "wb") as f:
        pickle.dump(calibrator, f)
    with open(FEATURE_LIST_PATH, "w") as f:
        json.dump(FEATURE_COLS, f)

    # Log to DB
    conn = get_connection()
    conn.execute("""
        INSERT INTO training_log(trained_at,model_version,num_samples,brier_score,log_loss,auc,notes)
        VALUES(?,?,?,?,?,?,?)
    """, (datetime.now(timezone.utc).isoformat(), "xgb_v1", len(records), bs, ll, auc, "auto-trained"))
    conn.commit()
    conn.close()
    log.info("Model saved.")


def _save_heuristic_model():
    """Save a sentinel so predict() knows to use the heuristic."""
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"type": "heuristic"}, f)
    with open(FEATURE_LIST_PATH, "w") as f:
        json.dump(FEATURE_COLS, f)
    log.info("Heuristic model saved.")


def load_model():
    if not MODEL_PATH.exists():
        _save_heuristic_model()
    with open(MODEL_PATH, "rb") as f:
        return pickle.load(f)


def load_calibrator():
    if not CALIBRATOR_PATH.exists():
        return None
    with open(CALIBRATOR_PATH, "rb") as f:
        return pickle.load(f)


# ── Heuristic probability estimator ─────────────────────────────────────────

def heuristic_hr_prob(feats: dict) -> float:
    """
    Rule-based HR probability using weighted Statcast signals.
    Returns estimated probability between 0 and 1.
    Calibrated to roughly match league average HR rate (~4-5% per PA at 3.7 PA/game ≈ 15-18% per game).
    """
    # Base: league average HR/game ~14%
    prob = 0.14

    # Barrel rate boost (league avg ~8%)
    br = feats.get("barrel_rate_last_14d") or feats.get("barrel_rate_season") or 0.08
    prob += (br - 0.08) * 0.8

    # Exit velocity (league avg ~88 mph)
    ev = feats.get("avg_ev_last_14d") or feats.get("avg_ev_season") or 88.0
    prob += (ev - 88.0) * 0.003

    # xSLG (league avg ~.400)
    xslg = feats.get("xslg_last_14d") or feats.get("xslg_season") or 0.400
    prob += (xslg - 0.400) * 0.15

    # Hard-hit rate (league avg ~37%)
    hh = feats.get("hard_hit_last_14d") or feats.get("hard_hit_last_30d") or 0.37
    prob += (hh - 0.37) * 0.10

    # ISO (league avg ~.155)
    iso = feats.get("iso_season") or 0.155
    prob += (iso - 0.155) * 0.20

    # Pitcher barrel rate allowed (league avg ~8%)
    p_br = feats.get("p_barrel_rate_allowed") or 0.08
    prob += (p_br - 0.08) * 0.30

    # Park factor
    pf = feats.get("park_hr_factor") or 1.0
    prob *= pf

    # Wind / temp bonus
    prob += feats.get("wind_hr_bonus", 0.0)
    prob += feats.get("temp_hr_bonus", 0.0)

    # Air density (lower = more carry)
    adi = feats.get("air_density_idx") or 1.0
    prob += (1.0 - adi) * 0.05

    # PA opportunity scaling
    pa = feats.get("projected_pa") or 3.8
    prob *= (pa / 4.0)

    # Batting order: leadoff hitters get slightly more PAs
    slot = feats.get("batting_order") or 5
    if slot <= 2:
        prob *= 1.05
    elif slot >= 8:
        prob *= 0.95

    # K rate penalty (high strikeout = fewer balls in play)
    k = feats.get("k_rate_season") or 0.22
    prob -= max(0, (k - 0.22)) * 0.15

    return float(np.clip(prob, 0.02, 0.60))


def predict_proba(feats: dict) -> tuple[float, float]:
    """
    Returns (raw_prob, calibrated_prob) for a single batter feature dict.
    Uses XGBoost if trained, otherwise heuristic.
    """
    model = load_model()

    if isinstance(model, dict) and model.get("type") == "heuristic":
        prob = heuristic_hr_prob(feats)
        return prob, prob

    # XGBoost path
    feature_cols = FEATURE_COLS
    row = np.array([[feats.get(c) for c in feature_cols]], dtype=float)
    col_medians = np.array([feats.get(c, 0.0) or 0.0 for c in feature_cols])
    row = np.where(np.isnan(row), col_medians, row)

    raw_prob = float(model.predict_proba(row)[:, 1][0])

    calibrator = load_calibrator()
    if calibrator:
        cal_prob = float(calibrator.predict([raw_prob])[0])
    else:
        cal_prob = raw_prob

    return raw_prob, cal_prob
