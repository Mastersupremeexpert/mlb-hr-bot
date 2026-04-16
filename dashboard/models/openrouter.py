"""
MLB Home Run Bot — OpenRouter AI Reasoning Layer
Uses Claude Sonnet 4.6 to analyze each pick and produce:
  - Plain-English bet justification
  - Confidence adjustment (+/- up to 3pp)
  - Red flags (injury news, weather, lineup uncertainty)
  - Sharp money signals
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import requests
from datetime import date
from typing import Optional

log = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE    = "https://openrouter.ai/api/v1/chat/completions"
MODEL              = "anthropic/claude-sonnet-4.6"

# Cost guard — max tokens per pick analysis
MAX_TOKENS = 600


def _call_llm(prompt: str) -> Optional[str]:
    """Call OpenRouter API and return the text response."""
    if not OPENROUTER_API_KEY:
        log.warning("OPENROUTER_API_KEY not set — skipping AI analysis.")
        return None
    try:
        resp = requests.post(
            OPENROUTER_BASE,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://mlb-hr-bot-production.up.railway.app",
                "X-Title": "MLB HR Bot",
            },
            json={
                "model": MODEL,
                "max_tokens": MAX_TOKENS,
                "temperature": 0.3,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are an expert MLB home run prop bet analyst. "
                            "You analyze statistical data and give sharp, concise betting insights. "
                            "Always respond in valid JSON only — no markdown, no extra text."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log.warning(f"OpenRouter call failed: {e}")
        return None


def analyze_pick(pick: dict, game_date: str | None = None) -> dict:
    """
    Run AI analysis on a single ranked pick.
    Returns enriched pick dict with AI fields added.
    """
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    player   = pick.get("player_name", "Unknown")
    label    = pick.get("rank_label", "?")
    model_p  = pick.get("cal_prob", 0)
    impl_p   = pick.get("implied_prob", 0.15)
    edge     = pick.get("edge", 0)
    odds     = pick.get("best_odds", "N/A")
    book     = pick.get("best_book", "N/A")
    reasons  = pick.get("reasons", [])
    proj_pa  = pick.get("proj_pa", 3.8)
    batting  = pick.get("batting_order", "?")
    confirmed = pick.get("confirmed", 0)

    prompt = f"""Analyze this MLB home run prop bet for {game_date}:

PLAYER: {player} (Rank: {label})
MODEL PROBABILITY: {model_p:.1%}
BOOK IMPLIED PROBABILITY: {impl_p:.1%}
EDGE: {edge:+.1%}
BEST ODDS: {odds} at {book}
BATTING ORDER: {batting}
PROJECTED PA: {proj_pa:.1f}
LINEUP CONFIRMED: {'Yes' if confirmed else 'Not yet'}
STATISTICAL SIGNALS: {json.dumps(reasons)}

Respond ONLY with this JSON (no markdown):
{{
  "ai_verdict": "STRONG BET | LEAN BET | MARGINAL | PASS",
  "confidence_adjustment": <float between -0.03 and +0.03>,
  "one_liner": "<25-word sharp bet summary>",
  "bull_case": "<main reason this HR hits today>",
  "bear_case": "<main risk or red flag>",
  "sharp_note": "<any edge, line movement, or market inefficiency note>",
  "ai_grade": "A+ | A | B+ | B | C | D"
}}"""

    raw = _call_llm(prompt)
    if not raw:
        # Return pick unchanged if AI unavailable
        pick["ai_verdict"] = "N/A"
        pick["ai_one_liner"] = ""
        pick["ai_bull"] = ""
        pick["ai_bear"] = ""
        pick["ai_sharp"] = ""
        pick["ai_grade"] = label
        pick["ai_confidence_adj"] = 0.0
        return pick

    try:
        # Strip any accidental markdown fences
        clean = raw.replace("```json", "").replace("```", "").strip()
        ai = json.loads(clean)
        pick["ai_verdict"]         = ai.get("ai_verdict", "N/A")
        pick["ai_one_liner"]       = ai.get("one_liner", "")
        pick["ai_bull"]            = ai.get("bull_case", "")
        pick["ai_bear"]            = ai.get("bear_case", "")
        pick["ai_sharp"]           = ai.get("sharp_note", "")
        pick["ai_grade"]           = ai.get("ai_grade", label)
        # Store adjustment for transparency but do NOT apply it to cal_prob.
        # The quantitative model's probability should not be overridden by LLM pattern-matching.
        # A/B test this after 6 weeks of paper trading to measure CLV impact.
        pick["ai_confidence_adj"] = float(ai.get("confidence_adjustment", 0.0))
        log.info(f"  AI [{label}] {player}: {pick['ai_verdict']} | {pick['ai_one_liner']}")
    except Exception as e:
        log.warning(f"AI JSON parse failed for {player}: {e} | raw: {raw[:200]}")
        pick["ai_verdict"]        = "N/A"
        pick["ai_one_liner"]      = ""
        pick["ai_bull"]           = ""
        pick["ai_bear"]           = ""
        pick["ai_sharp"]          = ""
        pick["ai_grade"]          = label
        pick["ai_confidence_adj"] = 0.0

    return pick


def analyze_full_card(picks: list[dict], game_date: str | None = None) -> tuple[list[dict], str]:
    """
    Run AI analysis on all A/B/C/D picks + generate a daily card summary.
    Returns (enriched_picks, card_summary_text).
    """
    if not picks:
        return picks, ""

    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    # Analyze each pick individually
    enriched = []
    for pick in picks:
        enriched.append(analyze_pick(pick, game_date))

    # Generate overall card summary
    summary_prompt = f"""You are an MLB HR prop analyst. Summarize today's betting card for {game_date}.

PICKS:
{json.dumps([{
    'player': p.get('player_name'),
    'rank': p.get('rank_label'),
    'verdict': p.get('ai_verdict'),
    'edge': f"{p.get('edge', 0):+.1%}",
    'odds': p.get('best_odds'),
    'one_liner': p.get('ai_one_liner')
} for p in enriched], indent=2)}

Write a sharp 3-4 sentence daily card summary. Mention the strongest play, overall card confidence, and any parlay recommendation. Be concise and direct like a professional handicapper. Plain text only, no JSON."""

    summary = _call_llm(summary_prompt) or ""
    return enriched, summary


if __name__ == "__main__":
    # Quick test
    logging.basicConfig(level=logging.INFO)
    test_pick = {
        "player_name": "Aaron Judge",
        "rank_label": "A",
        "cal_prob": 0.22,
        "implied_prob": 0.15,
        "edge": 0.07,
        "best_odds": 320,
        "best_book": "draftkings",
        "reasons": ["Elite barrel rate (18.2%) over 14d", "Wind blowing out to CF (12 mph)", "Warm conditions favor carry (84°F)"],
        "proj_pa": 4.2,
        "batting_order": 3,
        "confirmed": 1,
    }
    result = analyze_pick(test_pick)
    print(json.dumps(result, indent=2))
