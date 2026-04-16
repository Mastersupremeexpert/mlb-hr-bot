# v6 Update — Post-Deploy Instructions

This update fixes the issues visible in your dashboard screenshots:

1. **All picks showed "N/A" odds** — caused by hash-ID orphans in the odds table never matching real MLBAM IDs from lineups
2. **AI analysis section didn't render** — caused by wrong OpenRouter model slug (`claude-sonnet-4.6` doesn't exist)

Both are fixed in this zip. Follow these steps **in order**.

---

## Step 0 — Rotate your API keys (if you haven't yet)

You posted a screenshot showing `ODDS_API_KEY`, `OPENROUTER_API_KEY`, and `BOT_PASSWORD` in plaintext. Anyone with that screenshot can drain your API quotas. Rotate them now:

- **The Odds API** → regenerate at https://the-odds-api.com
- **OpenRouter** → revoke and create new at https://openrouter.ai/keys
- **Railway Postgres** → rotate password in the Postgres service
- **BOT_PASSWORD** → change to something new in Railway Variables

---

## Step 1 — Push v6 code to GitHub

1. Replace all files in your `mlb-hr-bot` repo with the contents of this zip
2. In GitHub Desktop (or web): commit with message "v6 fixes: MLBAM ID resolution + OpenRouter model slug"
3. Push to main — Railway will auto-redeploy in ~2 minutes

---

## Step 2 — Set the new environment variable

The OpenRouter model is now env-driven so you can change it without redeploying code.

1. Go to Railway → your `mlb-hr-bot` service → **Variables** tab
2. Click **+ New Variable**
3. Name: `OPENROUTER_MODEL`
4. Value: `anthropic/claude-opus-4.7`
5. Click **Deploy** to apply

(If you want to save money later, you can swap this to a cheaper Sonnet-class slug — Opus 4.7 costs $25/M output tokens and each pick analysis is ~600 tokens, so ~$0.015/pick or ~$0.075 per full card. Cheap but not free.)

---

## Step 3 — Run the one-time migration

Your existing database has orphan hash IDs from v5 that need to be merged into real MLBAM IDs. This is a one-time cleanup.

**From your phone/computer**, hit this URL (replace with your actual Railway URL, enter username/password when prompted):

```
POST https://YOUR-URL.up.railway.app/api/admin/migrate_ids
```

Easiest way: open a terminal / cmd prompt and run:

```bash
curl -X POST -u admin:YOUR_NEW_BOT_PASSWORD https://YOUR-URL.up.railway.app/api/admin/migrate_ids
```

You'll get back something like:

```json
{
  "status": "ok",
  "mappings": 47,
  "recommendations_updated": 12,
  "hash_players_deleted": 47
}
```

This means 47 orphan hash IDs were repointed to their real MLBAM equivalents and then deleted.

If you get `{"status": "no_migration_needed", "mappings": 0}` — that means your data is already clean, nothing to do.

---

## Step 4 — Re-run the pipeline

Now the fixed data flow matters. In the dashboard, click:

1. **Morning Run** — pulls fresh schedule, lineups, statcast, weather, and odds. With v6, odds will now resolve to real MLBAM IDs via MLB Stats API when the local DB doesn't have a match.
2. Wait ~90 seconds
3. **Post-Lineup Run** — confirms lineups + AI enrichment fires. Check your Railway logs while it runs. You should see:
   ```
   Running AI analysis on top picks...
     AI [A] <player name>: STRONG BET | ...
   ```
   If you see `OpenRouter returned 400 using model '...': ...` — the model slug is wrong. Update `OPENROUTER_MODEL` env var and try again.

---

## Step 5 — Verify the fix

After Post-Lineup Run, refresh the dashboard. You should now see:

- ✅ **Singles with real odds** (e.g. "+475 at draftkings"), not "N/A"
- ✅ **Positive edges** on top picks (not all negative)
- ✅ **AI verdict badge** (🤖 STRONG BET / LEAN BET / MARGINAL / PASS)
- ✅ **AI one-liner** below the player name
- ✅ **Bull/Bear/Sharp notes** at the bottom of each pick card
- ✅ **"Recommended Singles" panel** populated with the A/B/C/D singles

If any of these are still missing, check Railway logs — the error messages are now much more verbose.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Still seeing N/A odds after migration | Hit `/api/run/full` again to re-ingest odds with the new matching logic |
| AI section still empty | Check Railway logs for `OpenRouter returned` errors. Likely wrong model slug — update `OPENROUTER_MODEL` env var |
| Migration endpoint returns 401 | Use `-u admin:YOUR_BOT_PASSWORD` in the curl command |
| "No singles recommended" but picks show edges | Singles are only built when odds are attached. Re-run the pipeline after migration. |
| Top 20 shows odds but A/B/C/D shows N/A | Same root cause — migration didn't run or odds were ingested before migration. Run migration, then re-run pipeline. |

---

## What changed under the hood (for reference)

- `models/openrouter.py` — model slug is now env-driven, with verbose error logging
- `pipeline/ingest_odds.py` — player matching now hits MLB Stats API for real MLBAM IDs; hash fallback is last resort and logged as warning
- `pipeline/migrate_fix_orphan_ids.py` — NEW one-time migration script
- `dashboard/app.py` — NEW `/api/admin/migrate_ids` endpoint

Nothing else about your workflow changes. All your existing buttons, picks, parlays, and historical ROI tracking remain intact.
