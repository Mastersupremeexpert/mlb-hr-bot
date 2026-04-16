# MLB Home Run Bot — Setup Guide (Windows)

## What this bot does

Every day it:
1. Pulls the MLB schedule, lineups, and probable pitchers
2. Downloads Statcast contact-quality data from Baseball Savant
3. Fetches weather for each stadium
4. Pulls sportsbook home run prop odds
5. Scores every hitter with a probability model
6. Picks the 4 best underpriced hitters (A, B, C, D)
7. Builds the optimal singles + parlay card
8. Shows everything on a web dashboard you open in your browser

---

## Step 1: Install Python

1. Go to **https://www.python.org/downloads/**
2. Download Python **3.11** or **3.12**
3. Run the installer
4. **Important:** On the first screen, check the box that says **"Add Python to PATH"**
5. Click Install Now

To verify it worked: open **Command Prompt** (press Win+R, type `cmd`, hit Enter) and type:
```
python --version
```
You should see something like `Python 3.12.0`.

---

## Step 2: Get your free Odds API key

1. Go to **https://the-odds-api.com**
2. Click **Get API Key** — the free tier gives you 500 requests/month (enough for daily use)
3. Sign up and copy your API key

---

## Step 3: Add your API key to the bot

1. Open the `mlb_hr_bot` folder
2. Open the file called **`config.py`** in Notepad
3. Find this line:
   ```python
   ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "YOUR_ODDS_API_KEY_HERE")
   ```
4. Replace `YOUR_ODDS_API_KEY_HERE` with your actual key (keep the quotes)
5. Save the file

---

## Step 4: Start the bot

1. Open the `mlb_hr_bot` folder in File Explorer
2. Double-click **`start.bat`**
3. A black terminal window will open — let it run
4. On first launch it will install all dependencies automatically (takes 1-2 minutes)
5. When you see `Dashboard starting at: http://localhost:8000` — open your browser and go to:

   **http://localhost:8000**

---

## Step 5: Run the daily pipeline

Once the dashboard is open, click the green **"▶ Run Full Day Now"** button.

This will:
- Pull today's schedule and lineups
- Download Statcast data
- Fetch weather
- Pull odds from your sportsbook accounts
- Generate today's A/B/C/D card

Wait about 60-90 seconds, then refresh the page. Your picks will appear.

---

## Daily routine

| Time | What to do |
|------|------------|
| Morning (~10am) | Click **"🌅 Morning Run"** for early odds and Statcast refresh |
| ~30 min before first pitch | Click **"📋 Post-Lineup Run"** to lock in confirmed lineups |
| After games | Record results using the API (see below) |

---

## Recording bet results

After a game, record whether your bet won or lost. This builds your ROI history.

In Command Prompt (with the bot running), run:
```
curl -X POST "http://localhost:8000/api/result?rec_id=1&won=true&payout=45.50"
```

Replace:
- `rec_id` = the ID number from your bet_recommendations table
- `won=true` or `won=false`
- `payout` = how much you got back (0 if lost)

---

## Exporting your card

From the dashboard, use the **"⬇ Export JSON"** and **"⬇ Export CSV"** buttons.

Files save to the `exports/` folder inside `mlb_hr_bot/`.

---

## Running the backtest

After you have several weeks of recorded results:

1. Double-click **`run_backtest.bat`**
2. Or from Command Prompt:
   ```
   run_backtest.bat --start 2026-04-01 --end 2026-04-30
   ```

---

## Automating the daily run (optional)

You can schedule the pipeline to run automatically using Windows Task Scheduler:

1. Press **Win+S**, search for **Task Scheduler**, open it
2. Click **Create Basic Task**
3. Set the trigger to **Daily** at your preferred time (e.g., 11:30 AM)
4. Set the action to **Start a program**
5. Browse to `run_pipeline.bat` inside your `mlb_hr_bot` folder
6. Finish

---

## File structure

```
mlb_hr_bot/
├── config.py              ← Your settings and API keys
├── start.bat              ← Double-click to launch dashboard
├── run_pipeline.bat       ← Run pipeline manually
├── run_backtest.bat       ← Run backtest
├── requirements.txt       ← Python dependencies
├── data/
│   └── schema.py          ← Database setup
│   └── mlb_hr_bot.db      ← SQLite database (created on first run)
├── pipeline/
│   ├── ingest_mlb.py      ← Schedule, lineups, pitchers
│   ├── ingest_statcast.py ← Statcast / Baseball Savant
│   ├── ingest_weather.py  ← Open-Meteo weather
│   ├── ingest_odds.py     ← Sportsbook odds
│   ├── features.py        ← Feature engineering
│   └── workflow.py        ← Daily orchestrator
├── models/
│   ├── train.py           ← XGBoost + calibration model
│   ├── ranker.py          ← A/B/C/D scoring + edge calc
│   └── optimizer.py       ← Parlay combo optimizer
├── dashboard/
│   ├── app.py             ← FastAPI web server
│   └── templates/
│       └── dashboard.html ← Web UI
├── backtest/
│   └── backtest.py        ← Brier, ROI, CLV, calibration
├── exports/               ← Daily JSON/CSV exports
└── logs/                  ← Pipeline logs
```

---

## Important notes

- **The model starts heuristic** (rule-based) and switches to XGBoost automatically once you have 100+ labeled bets (won/lost) recorded.
- **Odds are required** for edge calculation. Without a valid Odds API key, the bot will still score players but cannot calculate edge against the book.
- **Lineups must be confirmed** before a player appears in the final card. The post-lineup run handles this.
- **No data is sent anywhere.** Everything runs on your local machine.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `python` not found | Re-install Python and check "Add Python to PATH" |
| Dashboard doesn't open | Make sure `start.bat` is still running; try refreshing |
| No picks showing | Run the pipeline and wait 90 seconds before refreshing |
| Odds not loading | Check your API key in `config.py` |
| Import errors | Delete the `venv` folder and re-run `start.bat` |
