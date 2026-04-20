# Telegram integration (RiverFlowScanner)

This guide walks you through sending **Telegram group alerts** when the scanner shortlists a new symbol. You only need to do this once.

## What you need

1. A **Telegram account** (phone app or desktop).
2. A **Telegram group** where alerts should appear (you can create a new group).
3. This project’s **`telegram.py`** module (already in the repo) and the scanner running with environment variables set (see below).

---

## Step 1 — Create a bot with BotFather

1. Open Telegram and search for **`@BotFather`** (official Telegram bot).
2. Start a chat and send **`/newbot`**.
3. Follow the prompts: choose a **display name** and a **username** ending in `bot` (e.g. `RiverFlowAlertsBot`).
4. BotFather replies with an **HTTP API token** that looks like:

   `123456789:AAHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

5. **Copy that token** and keep it private (anyone with the token can control your bot).

This value is your **`TELEGRAM_BOT_TOKEN`**.

---

## Step 2 — Add the bot to your group

1. Open your **Telegram group** (create one if needed).
2. Tap the group name → **Add members** → search for your bot’s **username** (the one ending in `bot`).
3. Add the bot as a **member** of the group.

**Important:** Until the bot is in the group, it cannot post there.

---

## Step 3 — Get the group chat ID

Telegram needs a numeric **chat id** for the group. Common ways:

### Option A — Use a bot that shows your id (simple)

1. Add **`@userinfobot`** or **`@getidsbot`** to the **same group** (temporarily is fine).
2. Send any message in the group (e.g. `hello`).
3. The bot usually replies with **group id**; it often looks like **`-100xxxxxxxxxx`** (negative number for supergroups).

Copy that number — this is your **`TELEGRAM_CHAT_ID`**.

4. Remove the helper bot from the group if you no longer need it.

### Option B — Telegram Web + “getUpdates” (advanced)

1. After adding your bot to the group, send a message in the group mentioning the bot or any message.
2. In a browser open (replace `YOUR_TOKEN` with your real token):

   `https://api.telegram.org/botYOUR_TOKEN/getUpdates`

3. In the JSON, find `"chat":{"id":-100...` under `message` — that **`id`** is **`TELEGRAM_CHAT_ID`**.

---

## Step 4 — Set environment variables on your PC

The scanner reads **environment variables** (not the CSV file) for Telegram.

| Variable | Required | Meaning |
|----------|----------|---------|
| `TELEGRAM_BOT_TOKEN` | Yes | Token from BotFather |
| `TELEGRAM_CHAT_ID` | Yes | Group id (often `-100...`) |
| `TELEGRAM_ENABLED` | No | Set to `0` or `false` to turn alerts off without removing token |

### Windows (PowerShell — current session only)

```powershell
$env:TELEGRAM_BOT_TOKEN = "paste-your-token-here"
$env:TELEGRAM_CHAT_ID = "-100xxxxxxxxxx"
```

Then start your app from **the same** PowerShell window.

### Windows (persistent user environment)

1. Press **Win + R**, type `sysdm.cpl`, Enter.
2. **Advanced** → **Environment Variables**.
3. Under **User variables** → **New** (or **Edit**):
   - Name: `TELEGRAM_BOT_TOKEN`, Value: your token  
   - Name: `TELEGRAM_CHAT_ID`, Value: your group id  
4. OK out, then **fully restart** Cursor / terminal / Python so new values load.

---

## Step 5 — Run the scanner as you already do

Start **`main.py`** / your usual entry point **from the project folder** so Python can import **`telegram.py`**.

When a **new** symbol qualifies (first time in that “up” leg for the rule), the engine calls **`send_shortlist_alert`**. You should see a message in the group with:

- Symbol, time, LTP, % change, relative volume, value (Cr), rule tag, hit count, VTT delta  
- **FYERS** and **TradingView** chart links built from that symbol  

If Telegram is misconfigured, the scanner **still runs**; errors are printed to the console as `[telegram] ...`.

---

## Troubleshooting

| Problem | What to check |
|---------|----------------|
| No messages | Bot is in the group; `TELEGRAM_CHAT_ID` is the **group** id (often negative); token has no extra spaces |
| `403 Forbidden` / bot can’t message | Some groups restrict bots — check group **Permissions** / privacy; try making the bot an **admin** with “Post messages” |
| `400 Bad Request: chat not found` | Wrong chat id or bot was removed from the group |
| Duplicate alerts | By design, **each new shortlist event** (first qualification after dropping out) sends one alert; repeated ticks for the same “up” leg do not re-fire until the symbol qualifies again after resetting |

---

## Security note

Do **not** commit your bot token to git. Use environment variables or a local `.env` that is listed in **`.gitignore`** if you add dotenv support later.
