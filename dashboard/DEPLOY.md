# MLB HR Bot — Cloud Deployment Guide (Railway)

Follow these steps exactly. Takes about 15 minutes total.

---

## What you'll end up with

A private URL like `https://mlb-hr-bot-production.up.railway.app` that:
- Works on your phone from anywhere
- Is password protected (only you can access it)
- Runs 24/7 on Railway's servers
- Has a full PostgreSQL database (no SQLite)

---

## Step 1 — Upload code to GitHub

You need to put the bot's code into your new GitHub account so Railway can deploy it.

**Option A — GitHub Desktop (easiest for beginners)**

1. Download **GitHub Desktop** from [desktop.github.com](https://desktop.github.com) and install it
2. Sign in with your new GitHub account
3. Click **File → Add local repository**
4. Browse to the `mlb_hr_bot_cloud` folder → click **Add repository**
   - If it says "not a git repo", click **create a repository** instead
5. Click **Publish repository** (top right)
6. Uncheck "Keep this code private" if you want, or leave it checked
7. Click **Publish Repository** — done

**Option B — GitHub website upload (no software needed)**

1. Go to [github.com](https://github.com) and log in
2. Click the **+** button top right → **New repository**
3. Name it `mlb-hr-bot`, leave everything else default, click **Create repository**
4. On the next page, click **uploading an existing file**
5. Drag and drop ALL files from the `mlb_hr_bot_cloud` folder into the upload box
6. Click **Commit changes**

---

## Step 2 — Create a Railway project

1. Go to [railway.app](https://railway.app) and log in
2. Click **New Project**
3. Click **Deploy from GitHub repo**
4. Click **Configure GitHub App** if prompted → authorize Railway to access your account
5. Select your `mlb-hr-bot` repository
6. Railway will start trying to build — that's fine, let it run

---

## Step 3 — Add a PostgreSQL database

1. In your Railway project, click **+ New** (top right of the project canvas)
2. Click **Database → Add PostgreSQL**
3. Railway creates a Postgres database and wires it to your app automatically
4. The `DATABASE_URL` variable is injected for you — you don't need to copy anything

---

## Step 4 — Set your environment variables

1. Click on your **app service** (not the Postgres one) in the Railway canvas
2. Click the **Variables** tab
3. Add these variables one by one using **+ New Variable**:

| Variable | Value |
|---|---|
| `ODDS_API_KEY` | Your key from the-odds-api.com |
| `BOT_PASSWORD` | Pick any password — you'll use this to log in from your phone |
| `BOT_USERNAME` | Pick any username (e.g. `admin`) |

4. Click **Deploy** after adding variables — Railway will redeploy with the new settings

---

## Step 5 — Get your public URL

1. Click on your app service
2. Click the **Settings** tab
3. Under **Networking**, click **Generate Domain**
4. You'll get a URL like `https://mlb-hr-bot-production.up.railway.app`
5. **Bookmark this on your phone**

---

## Step 6 — First login

1. Open the URL on your phone
2. Your browser will ask for a username and password
3. Enter the `BOT_USERNAME` and `BOT_PASSWORD` you set in Step 4
4. You're in — tap **"▶ Run Full Day Now"** to generate your first card

---

## Daily use from your phone

| What to do | When |
|---|---|
| Tap **"🌅 Morning Run"** | ~10am — pulls Statcast + early odds |
| Tap **"📋 Post-Lineup Run"** | ~30 min before first pitch — locks in lineups |
| View A/B/C/D card | Refresh after ~60 seconds |
| Record results | Use the API (see below) |

---

## Recording bet results (from your phone)

After games settle, record wins/losses to build your ROI history.

Open your browser and go to (replacing values):

```
https://YOUR-URL.up.railway.app/api/result?rec_id=1&won=true&payout=45.50
```

- `rec_id` = the ID shown in your bet table
- `won=true` or `won=false`
- `payout` = amount returned (0 if lost)

---

## Updating the bot in the future

If you need to update any code:
1. Edit the file on your computer
2. Open GitHub Desktop → you'll see the changed files
3. Type a short description at the bottom left, click **Commit to main**
4. Click **Push origin**
5. Railway automatically redeploys in about 2 minutes

---

## Costs

Railway charges based on actual usage:
- The bot running idle: ~$0/month (within free allowance)
- The bot with Postgres + active use: roughly **$5–10/month**
- Railway gives you $5 free credit/month on the Hobby plan ($5/month subscription)
- Net cost: roughly **$5–10/month total**

Check your usage at [railway.app/dashboard](https://railway.app/dashboard) anytime.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Deployment failed | Click **View Logs** in Railway — look for the error message |
| "Application error" on site | Check Variables tab — make sure DATABASE_URL is present |
| Can't log in | Double-check BOT_USERNAME and BOT_PASSWORD in Variables tab |
| No picks showing | Tap Run Full Day and wait 90 seconds before refreshing |
| Odds not loading | Check ODDS_API_KEY in Variables tab |
| Database error | Make sure the PostgreSQL plugin is added to your project |
