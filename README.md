# Raghuveer Fyers Scanner

A Python tool that pulls NSE equity symbols, logs into [Fyers](https://fyers.in/) with TOTP-based automation, builds a per-symbol **volume SMA** baseline from historical **1-minute** candles, then watches live **SymbolUpdate** websocket data. A background evaluator runs every second on cached ticks to flag names where **intraday volume traded today (VTT)** in the current IST **minute** (relative to that minute’s baseline) and **rupee value** (delta × LTP) exceed configurable rules. A Flask dashboard shows hits (newest first), persists them for the trading session, and exposes a compact “sample data” view of VTT math per symbol.

---

## Dependencies

Install from `requirements.txt`:

| Package        | Role |
|----------------|------|
| `requests`     | HTTP calls during Fyers login flow |
| `pandas`       | CSV/symbol handling, candle frames, timezones (IST) |
| `polars`       | DataFrame backend used for indicator expression execution |
| `polars-ta`    | Technical indicator expressions (`ts_mean`) for SMA on volume |
| `fyers-apiv3`  | Fyers REST (`history`, profile, etc.) and `FyersDataSocket` websocket |
| `flask`        | Web UI for settings, watchlist, and start/stop |
| `pyotp`        | TOTP for automated 2FA during login |
| `pytz`         | Timezone support (used in integration helpers) |

**Runtime:** Python 3.10+ recommended (uses modern typing syntax in the scanner).

---

## How to run the project

### 1. Virtual environment and packages

```bash
cd "d:\Desktop\python projects\RaghuveerFyersScanner"
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Fyers credentials (required for the scanner)

Create `FyersCredentials.csv` in the project root (this file is gitignored). Use a header row with columns **`Title`** and **`Value`**, and rows for:

- `redirect_uri`
- `client_id`
- `secret_key`
- `totpkey` (your TOTP secret for Fyers 2FA)
- `FY_ID` (Fyers login id)
- `PIN` (trading PIN)

The scanner loads this file and calls `automated_login()` in `FyresIntegration.py` to obtain an access token and a `FyersModel` instance.

### 3. Refresh the NSE symbol list (optional but typical)

Downloads Fyers’ public NSE cash-market master, keeps **`-EQ`** names, drops rows whose description contains **SME**, and writes `NSE_EQ_symbols.csv`:

```bash
python main.py
```

### 4. Start the web app (main way to use the scanner)

```bash
python app.py
```

Then open the URL Flask prints (default **http://127.0.0.1:5000**). Use the UI to adjust rules, manage a watchlist, and **start** or **stop** the background scanner thread.

---

## What the project does (current approach)

### Symbol universe (`main.py`)

- Reads `https://public.fyers.in/sym_details/NSE_CM.csv` with pandas.
- Keeps symbols ending in `-EQ` and excludes SME names from the description column.
- Writes a single-column CSV `NSE_EQ_symbols.csv` (`symbol_name`).

### Scanner engine (`scanner_engine.py`)

1. **Bootstrap**  
   On start, refreshes the symbol CSV, then loads symbols from `NSE_EQ_symbols.csv`.  
   `BOOTSTRAP_SYMBOL_LIMIT` controls how many symbols to use from the end of the list; **`0` means all symbols** (non‑positive = full universe).

2. **Login**  
   Uses `FyersCredentials.csv` and `automated_login()` to get `access_token` and an authenticated Fyers client.

3. **Historical baseline**  
   For each symbol, fetches **1-minute** candles over a date range derived from the configured SMA period, computes a **rolling mean of volume** (SMA), and builds a baseline row (close, SMA volume, etc.).  
   Results are written to **`combinedsymbol.csv`** and upserted into SQLite **`scanner_data.db`** (`symbol_snapshot` table). This path is unchanged for “history first, then live.”

4. **Live websocket (ingest only)**  
   Subscribes to **`SymbolUpdate`** in batches. Ticks update **`_last_tick_by_symbol`** and drive **IST minute** bookkeeping:
   - On the **first tick in a new IST minute** for a symbol, **`vol_traded_today` (VTT)** is stored as that minute’s **baseline** for that symbol.
   - If VTT moves backward (exchange correction), the baseline is adjusted down to stay consistent.
   - **Rules are not evaluated on every tick** (avoids lag under heavy tick rates).

5. **Minute boundary / “scan ready”**  
   When the exchange clock rolls to a **new IST minute** (from tick timestamps), the engine sets **`_scan_ready_from_next_minute`** after the first boundary (same “wait for clean minute” idea as before). Until then, live rules do not arm.

6. **Rule evaluation (1 Hz)**  
   A daemon thread runs every **`EVAL_INTERVAL_SECS` (1 s)** and reads only **cached** websocket state. For each symbol with baseline + tick + SMA:
   - **`diff`** = `max(current_VTT − minute_baseline_VTT, 0)`  
   - **`value_rupees`** = `diff × LTP`  
   Two branches (**C1** / **C2**) are OR’d; each requires volume vs SMA and rupee notionals in **crore** scale (`× 1e7` in code), e.g.  
   `diff > ruleN_volume_multiplier × SMA_volume` **and** `value_rupees > ruleN_value_cr × 1e7`.  
   **Leading edge:** a new shortlist row is appended only when the condition becomes true after having been false (so you do not get a row every second while it stays true). When it fires again later the same IST **session day**, you get **another row** (multiple hits per symbol per day).

7. **Shortlist UI semantics**  
   - Rows are **newest first** (last qualifying event at the top).  
   - **`symbol_repeat`**: styling / “hit again today” when the symbol has more than one qualifying edge that session.  
   - In-memory list is capped (**`SHORTLIST_MAX_EVENTS`**); the dashboard snapshot returns the latest slice (e.g. top 300).

8. **Persisted session state (server / multi-device)**  
   - **`runtime_shortlist_state.json`** stores the shortlist for the current **IST session day**.  
   - Session date uses **`shortlist_session_date_ist()`**: before **08:00 IST** it still counts as the **previous** calendar day’s session; from **08:00** onward it is the **new** session.  
   - The file is rewritten whenever a new shortlist event is stored. **`snapshot()`** reloads from disk if the file is newer (so a phone and a laptop against the same server see the same list after refresh).  
   - **`start()`** does **not** wipe the shortlist for the same session (stop/start same day keeps history).  
   - At **08:00 IST** the daily scheduler runs prep: if a run was still active it is **stopped**, any **websocket** is closed, stale **`runtime_ws_state.json`** cleanup runs, and the **shortlist file + in-memory shortlist** are cleared for the new session—then **`start()`** runs (history immediately, websocket at **09:15 IST** when using the auto schedule).

9. **Daily auto schedule (constants)**  
   - **`AUTO_LOGIN_AND_HISTORY_HOUR_IST` / `MINUTE_IST`** — default **08:00** prep (login + history + wait).  
   - **`AUTO_WS_START_HOUR_IST` / `MINUTE_IST`** — default **09:15** websocket connect.  
   - Websocket connect path already disconnects any prior client before subscribing.

10. **Stop / cleanup**  
    Stopping bumps run ids, stops the eval thread, and closes the websocket. On Windows, a stale subprocess may be cleared via `taskkill` using `runtime_ws_state.json` if present.

### Web UI (`app.py` + `templates/`)

- Form posts adjust rule multipliers, SMA periods (passed into the engine at start), and watchlist; live metric is **VTT delta per minute** (described in the UI; not a user-selectable LTQ mode anymore).
- **Dashboard** polls **`/api/dashboard-data` every 2 seconds**; tables show all shortlist events for the session (watchlist is a **view filter** in the UI, not a server-side trim of stored hits).
- **Sample data** page polls **`/api/sample-data` every 2 seconds** and shows, per symbol: **baseline VTT**, **current VTT**, **minute diff**, **value (₹)**.

### Fyers integration (`FyresIntegration.py`)

- **`automated_login`:** OTP send → TOTP verify → PIN verify → token URL → auth code → `generate_token` → sets global `access_token` and `fyers` client used by the scanner.
- Additional helpers (quotes, OHLC, orders, alternate websocket helpers) are available for other workflows; the scanner primarily uses login + history + websocket.

---

## Cadence summary

| Layer | Interval |
|--------|-----------|
| Websocket | As fast as the exchange sends ticks (ingest only) |
| Rule evaluation | **1 s** on cached state (`EVAL_INTERVAL_SECS`) |
| Dashboard + sample data HTTP refresh | **2 s** (`setInterval` in templates) |

---

## Generated / local files

You may see these after running (some are gitignored or environment-specific):

| File | Purpose |
|------|--------|
| `NSE_EQ_symbols.csv` | Equity symbol list |
| `combinedsymbol.csv` | Last bootstrap snapshot per symbol |
| `scanner_data.db` | SQLite snapshots |
| `runtime_ws_state.json` | Optional PID hint for websocket cleanup |
| `runtime_shortlist_state.json` | Persisted shortlist + per-symbol hit counts for the current IST session (gitignored) |
| Fyers log files | From the API SDK in the project directory |

---

## Security note

Never commit `FyersCredentials.csv` or live tokens. Keep secrets local and rotate keys if they are exposed.
