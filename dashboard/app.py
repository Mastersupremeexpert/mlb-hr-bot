"""
MLB Home Run Bot — Web Dashboard (FastAPI, Cloud Edition)
Runs on Railway. Accessible from any device via your Railway URL.
Protected by a simple password set via the BOT_PASSWORD env variable.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from datetime import date, datetime
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, Request, BackgroundTasks, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
import secrets

from config import DASHBOARD_HOST, DASHBOARD_PORT, EXPORT_DIR
from data.schema import init_db, get_connection, fetchall, fetchone, _is_postgres

log = logging.getLogger(__name__)

app = FastAPI(title="MLB HR Bot", version="2.0")
security = HTTPBasic()

BASE = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE, "templates"))

# Password from env var (set in Railway dashboard)
BOT_PASSWORD = os.environ.get("BOT_PASSWORD", "homeruns2026")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "admin")


def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """Basic auth — browser will prompt for username/password on first visit."""
    ok_user = secrets.compare_digest(credentials.username.encode(), BOT_USERNAME.encode())
    ok_pass = secrets.compare_digest(credentials.password.encode(), BOT_PASSWORD.encode())
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=401,
            detail="Incorrect credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fmt_odds(v):
    if v is None: return "N/A"
    return f"+{v}" if v >= 0 else str(v)

def _pct(v):
    if v is None: return "—"
    return f"{v:.1%}"


def _get_today_card(game_date: str | None = None) -> dict:
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    conn = get_connection()

    # A/B/C/D picks — try final first, fall back to morning
    picks_rows = fetchall(conn, """
        SELECT mp.*, p.full_name
        FROM model_predictions mp
        JOIN players p ON mp.player_id = p.player_id
        WHERE substr(mp.run_timestamp,1,10) = ?
          AND mp.rank_label IN ('A','B','C','D')
          AND mp.run_stage = 'final'
        ORDER BY mp.rank_label
    """, (game_date,))

    if not picks_rows:
        picks_rows = fetchall(conn, """
            SELECT mp.*, p.full_name
            FROM model_predictions mp
            JOIN players p ON mp.player_id = p.player_id
            WHERE substr(mp.run_timestamp,1,10) = ?
              AND mp.rank_label IN ('A','B','C','D')
            ORDER BY mp.rank_label
        """, (game_date,))

    picks_out = []
    for p in picks_rows:
        reasons = []
        try: reasons = json.loads(p.get("reason_codes") or "[]")
        except: pass
        picks_out.append({
            "label": p["rank_label"],
            "name": p["full_name"],
            "model_prob": _pct(p.get("calibrated_prob")),
            "implied_prob": _pct(p.get("best_implied_prob")),
            "edge": _pct(p.get("edge")) if p.get("edge") else "—",
            "edge_raw": p.get("edge") or 0,
            "odds": _fmt_odds(p.get("best_odds")),
            "book": p.get("best_bookmaker") or "—",
            "stability": f"{(p.get('stability_score') or 0):.2f}",
            "score": f"{(p.get('player_score') or 0):.4f}",
            "proj_pa": f"{(p.get('projected_pa') or 0):.1f}",
            "reasons": reasons,
            "confirmed": p.get("confirmed_lineup"),
        })

    # Bet recommendations
    recs = fetchall(conn, """
        SELECT * FROM bet_recommendations
        WHERE bet_date = ?
        ORDER BY bet_type, id
    """, (game_date,))

    singles, parlay2, parlay3, parlay4 = [], [], [], []
    for r in recs:
        legs = json.loads(r.get("legs") or "[]")
        names = []
        for pid in legs:
            row = fetchone(conn, "SELECT full_name FROM players WHERE player_id=?", (pid,))
            names.append(row["full_name"] if row else str(pid))
        rec_dict = {
            "id": r["id"], "type": r["bet_type"], "label": r["leg_labels"],
            "players": " + ".join(names),
            "odds": _fmt_odds(r.get("american_odds")),
            "stake": f"${r.get('stake', 0):.0f}",
            "ev": f"${r.get('expected_value', 0):.2f}",
            "payout": f"${r.get('expected_payout', 0):.2f}",
        }
        if r["bet_type"] == "single": singles.append(rec_dict)
        elif r["bet_type"] == "parlay_2": parlay2.append(rec_dict)
        elif r["bet_type"] == "parlay_3": parlay3.append(rec_dict)
        elif r["bet_type"] == "parlay_4": parlay4.append(rec_dict)

    # Historical ROI
    roi_rows = fetchall(conn, """
        SELECT rec.bet_type,
               COUNT(*) as total_bets,
               SUM(CASE WHEN br.won=1 THEN 1 ELSE 0 END) as wins,
               SUM(br.profit) as total_profit,
               SUM(rec.stake) as total_staked
        FROM bet_recommendations rec
        JOIN bet_results br ON br.recommendation_id = rec.id
        WHERE br.won IS NOT NULL
        GROUP BY rec.bet_type
    """)

    roi_out = []
    for r in roi_rows:
        staked = r.get("total_staked") or 1
        profit = r.get("total_profit") or 0
        roi_pct = (profit / staked) * 100
        hit_rate = ((r.get("wins") or 0) / max(r.get("total_bets",1), 1)) * 100
        roi_out.append({
            "type": r["bet_type"], "bets": r["total_bets"], "wins": r.get("wins",0),
            "hit_rate": f"{hit_rate:.1f}%",
            "profit": f"${profit:.2f}",
            "roi": f"{roi_pct:+.1f}%",
        })

    # Top 20
    top20_rows = fetchall(conn, """
        SELECT mp.*, p.full_name
        FROM model_predictions mp
        JOIN players p ON mp.player_id = p.player_id
        WHERE substr(mp.run_timestamp,1,10) = ?
        ORDER BY mp.player_score DESC LIMIT 20
    """, (game_date,))

    top20_out = []
    for p in top20_rows:
        edge = p.get("edge") or 0
        top20_out.append({
            "rank": len(top20_out) + 1,
            "label": p.get("rank_label") or "—",
            "name": p["full_name"],
            "model_prob": _pct(p.get("calibrated_prob")),
            "implied_prob": _pct(p.get("best_implied_prob")),
            "edge": _pct(edge) if edge else "—",
            "edge_color": "green" if edge > 0 else "red",
            "odds": _fmt_odds(p.get("best_odds")),
            "score": f"{(p.get('player_score') or 0):.4f}",
            "book": p.get("best_bookmaker") or "—",
        })

    conn.close()
    return {
        "date": game_date, "picks": picks_out,
        "singles": singles, "parlay2": parlay2,
        "parlay3": parlay3, "parlay4": parlay4,
        "roi": roi_out, "top20": top20_out,
        "has_data": len(picks_out) > 0,
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    init_db()
    log.info("DB initialized.")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, date: Optional[str] = None,
                    user: str = Depends(require_auth)):
    card = _get_today_card(date)
    return templates.TemplateResponse("dashboard.html", {"request": request, **card})


@app.get("/api/card")
async def api_card(date: Optional[str] = None, user: str = Depends(require_auth)):
    return JSONResponse(_get_today_card(date))


@app.post("/api/run/morning")
async def api_run_morning(background_tasks: BackgroundTasks,
                          user: str = Depends(require_auth)):
    from pipeline.workflow import run_morning
    background_tasks.add_task(run_morning)
    return {"status": "morning run started"}


@app.post("/api/run/post_lineup")
async def api_run_post_lineup(background_tasks: BackgroundTasks,
                              user: str = Depends(require_auth)):
    from pipeline.workflow import run_post_lineup
    background_tasks.add_task(run_post_lineup)
    return {"status": "post-lineup run started"}


@app.post("/api/run/full")
async def api_run_full(background_tasks: BackgroundTasks,
                       user: str = Depends(require_auth)):
    from pipeline.workflow import run_full_day
    background_tasks.add_task(run_full_day)
    return {"status": "full run started"}


@app.post("/api/result")
async def post_result(rec_id: int, won: bool, payout: float = 0.0,
                      user: str = Depends(require_auth)):
    from pipeline.workflow import record_result
    record_result(rec_id, won, payout)
    return {"status": "recorded"}


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat(),
            "db": "postgres" if _is_postgres() else "sqlite"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard.app:app", host=DASHBOARD_HOST, port=DASHBOARD_PORT, reload=False)
