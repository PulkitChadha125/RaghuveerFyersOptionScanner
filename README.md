# Raghuveer Fyers Scanner

A Python tool that pulls NSE equity symbols, logs into [Fyers](https://fyers.in/) with TOTP-based automation, builds a per-symbol volume baseline from historical 1-minute candles, then watches live **SymbolUpdate** websocket ticks to flag stocks that spike on volume and traded value against that baseline.

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

## What the project does (logic)

### Symbol universe (`main.py`)

- Reads `https://public.fyers.in/sym_details/NSE_CM.csv` with pandas.
- Keeps symbols ending in `-EQ` and excludes SME names from the description column.
- Writes a single-column CSV `NSE_EQ_symbols.csv` (`symbol_name`).

### Scanner engine (`scanner_engine.py`)

1. **Bootstrap**  
   On start, refreshes the symbol CSV, then loads symbols from `NSE_EQ_symbols.csv`. For faster startup it currently uses only the **last 50** symbols (`BOOTSTRAP_SYMBOL_LIMIT`).

2. **Login**  
   Uses `FyersCredentials.csv` and `automated_login()` to get `access_token` and an authenticated Fyers client.

3. **Historical baseline**  
   For each symbol, fetches **1-minute** candles over a date range derived from the configured SMA period (and related constants), computes a **rolling mean of volume** over that period, and takes the last useful row (yesterday if available, else latest).  
   Results are written to **`combinedsymbol.csv`** and upserted into SQLite **`scanner_data.db`** (`symbol_snapshot` table).

4. **Live websocket**  
   Subscribes to **`SymbolUpdate`** in batches. Ticks in the **first** IST minute seen on the wire are used only to establish a minute bucket; on the **first clock rollover** to a new minute, in-memory per-minute accumulators are cleared and **`_scan_ready_from_next_minute`** turns on. From that boundary onward, each minute is a fresh bucket (matches your “wait for the next minute, then accumulate” behaviour). State is kept **in process** (Python dicts), not Redis—enough for a single app instance.

5. **Matching rules (volume / value only)**  
   For each tick after scan-ready, the engine reads a configurable **metric** (default `last_traded_qty`; can fall back to `ltq`). For `last_traded_qty`, each print’s quantity is **added** into `_minute_accum_metric[symbol]` for the current IST minute; **`effective_qty`** is that running sum until the minute changes.

   On each **new** minute, `_minute_accum_metric` and `_matched_this_minute` are **reset**. If a symbol already matched in the current minute, it is not re-evaluated until the next minute (so you don’t spam the list with the same bar).

   Two conditions (C1 / C2) combine:

   - **Volume vs baseline:** effective quantity &gt; `ruleN_volume_multiplier × SMA(volume)`  
   - **Traded value:** `effective_qty × LTP` &gt; `ruleN_value_cr × 1e7` (value expressed in “crore” scale in code)

   Defaults in the Flask app: Rule1 uses ×30 volume and 4 Cr; Rule2 uses ×10 volume and 8 Cr. C1 is checked before C2. When a row matches, it is stored in the shortlist shown in the UI (and updated on later hits via hit count where applicable).

6. **Output**  
   Shortlisted symbols (with LTP, change %, relative volume, trade value in Cr, condition, hit count) feed the UI. Recent pipeline/websocket lines are kept in memory for the dashboard.

7. **Stop / cleanup**  
   Stopping closes the websocket; on Windows, a stale subprocess may be cleared via `taskkill` using `runtime_ws_state.json` if present.

### Web UI (`app.py` + `templates/`)

- Form posts adjust rule multipliers, SMA periods (passed into the engine at start), metric source, watchlist, and start/stop.
- Watchlist filters which shortlisted rows are highlighted; engine can trim stored shortlist to watchlist symbols when the watchlist is non-empty.

### Fyers integration (`FyresIntegration.py`)

- **`automated_login`:** OTP send → TOTP verify → PIN verify → token URL → auth code → `generate_token` → sets global `access_token` and `fyers` client used by the scanner.
- Additional helpers (quotes, OHLC, orders, alternate websocket helpers) are available for other workflows; the scanner primarily uses login + history + websocket.

---

## Generated / local files

You may see these after running (some are gitignored or environment-specific):

- `NSE_EQ_symbols.csv` — equity symbol list  
- `combinedsymbol.csv` — last bootstrap snapshot per symbol  
- `scanner_data.db` — SQLite snapshots  
- `runtime_ws_state.json` — optional PID hint for cleanup  
- Fyers log files in the project directory (from the API SDK)

---

## Security note

Never commit `FyersCredentials.csv` or live tokens. Keep secrets local and rotate keys if they are exposed.
