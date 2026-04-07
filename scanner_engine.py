from __future__ import annotations

import csv
import json
import os
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from fyers_apiv3.FyersWebsocket import data_ws

from FyresIntegration import automated_login
from main import OUTPUT_CSV, write_symbols_csv

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / OUTPUT_CSV
DB_PATH = BASE_DIR / "scanner_data.db"
COMBINED_CSV_PATH = BASE_DIR / "combinedsymbol.csv"
CREDS_PATH = BASE_DIR / "fyers_credentials.json"
WS_STATE_PATH = BASE_DIR / "runtime_ws_state.json"
FILENAME_CREDENTIALS = BASE_DIR / "FyersCredentials.csv"
BOOTSTRAP_SYMBOL_LIMIT = 0
RATE_LIMIT_COOLDOWN_SECS = 30
FETCH_INTERVAL_SECS = 0.3
AUTO_LOGIN_AND_HISTORY_HOUR_IST = 8
AUTO_LOGIN_AND_HISTORY_MINUTE_IST = 0
AUTO_WS_START_HOUR_IST = 9
AUTO_WS_START_MINUTE_IST = 15
IST_ZONE = ZoneInfo("Asia/Kolkata")

data_socket: data_ws.FyersDataSocket | None = None


def close_fyres_websocket() -> None:
    """
    Close the active Fyers data websocket connection, if any.
    """
    global data_socket
    try:
        if data_socket is not None:
            print("[WS] Closing FyersDataSocket connection...")
            # Ensure the client will NOT auto-reconnect once we ask it to close.
            try:
                setattr(data_socket, "reconnect", False)
            except Exception:
                # If the attribute is missing/readonly we still attempt close.
                pass
            # Ask the socket to close its underlying websocket connection.
            data_socket.close_connection()
            # Drop our reference so a fresh socket can be created on next start.
            data_socket = None
        else:
            print("[WS] close_fyres_websocket called but no active socket.")
    except Exception as e:
        print("[WS] Error while closing FyersDataSocket:", e)


@dataclass
class EngineStatus:
    is_running: bool = False
    last_message: str = "Idle"
    last_error: str = ""
    last_started_at: str = ""
    recent_events: list[str] = field(default_factory=list)
    shortlisted_rows: list[dict[str, Any]] = field(default_factory=list)


class ScannerEngine:
    def __init__(self) -> None:
        self.status = EngineStatus()
        self._lock = threading.Lock()
        self._worker: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._ws_client: data_ws.FyersDataSocket | None = None
        self._recent_events: list[str] = []
        self._ws_msg_count = 0
        self._baseline_by_symbol: dict[str, dict[str, Any]] = {}
        self._shortlisted: dict[str, dict[str, Any]] = {}
        self._hit_count: dict[str, int] = {}
        self._minute_accum_metric: dict[str, float] = {}
        self._minute_ltq_sum: dict[str, float] = {}
        self._minute_trade_value_sum: dict[str, float] = {}
        self._matched_this_minute: set[str] = set()
        self._current_minute_key: str | None = None
        self._startup_minute_key: str | None = None
        self._scan_ready_from_next_minute = False
        self._last_tick_by_symbol: dict[str, dict[str, Any]] = {}
        self._scan_config: dict[str, Any] = {
            "rule1_volume_multiplier": 30,
            "rule1_value_cr": 4,
            "rule2_volume_multiplier": 10,
            "rule2_value_cr": 8,
            "metric_source": "last_traded_qty",
            "watchlist": [],
        }
        self._active_run_id = 0
        self._scheduler_thread: threading.Thread | None = None
        self._scheduler_stop_event = threading.Event()

    def start(
        self,
        sma_period: int,
        scan_config: dict[str, Any] | None = None,
        websocket_start_at_ist: datetime | None = None,
    ) -> None:
        with self._lock:
            if self.status.is_running:
                return
            self._active_run_id += 1
            run_id = self._active_run_id
            self._stop_event.clear()
            self.status.is_running = True
            self.status.last_error = ""
            self.status.last_message = "Starting day bootstrap..."
            self.status.last_started_at = datetime.now().isoformat(timespec="seconds")
            self._recent_events = []
            self._ws_msg_count = 0
            self._shortlisted = {}
            self._hit_count = {}
            self._minute_accum_metric = {}
            self._minute_ltq_sum = {}
            self._minute_trade_value_sum = {}
            self._matched_this_minute = set()
            self._current_minute_key = None
            self._startup_minute_key = None
            self._scan_ready_from_next_minute = False
            self._last_tick_by_symbol = {}
            if scan_config:
                self._scan_config.update(scan_config)
            self._worker = threading.Thread(
                target=self._run_pipeline,
                args=(sma_period, run_id, websocket_start_at_ist),
                name="scanner-engine",
                daemon=True,
            )
            self._worker.start()

    def start_daily_scheduler(self, sma_period: int = 1125) -> None:
        with self._lock:
            if self._scheduler_thread is not None and self._scheduler_thread.is_alive():
                return
            self._scheduler_stop_event.clear()
            self._scheduler_thread = threading.Thread(
                target=self._daily_scheduler_loop,
                args=(sma_period,),
                name="scanner-daily-scheduler",
                daemon=True,
            )
            self._scheduler_thread.start()
        self._push_event("Daily auto scheduler enabled (08:00 prep, 09:15 websocket IST).")

    def stop_daily_scheduler(self) -> None:
        self._scheduler_stop_event.set()
        self._push_event("Daily auto scheduler stopped.")

    def update_scan_config(self, scan_config: dict[str, Any]) -> None:
        with self._lock:
            self._scan_config.update(scan_config)
            watchlist = set(self._scan_config.get("watchlist") or [])
            if watchlist:
                self._shortlisted = {
                    k: v for k, v in self._shortlisted.items() if k in watchlist
                }

    def stop(self) -> None:
        with self._lock:
            self._active_run_id += 1
            self._stop_event.set()
            self._disconnect_ws()
            self._close_stale_process()
            self.status.is_running = False
            self.status.last_message = "Stopped"
            self._push_event("Pipeline stopped by user.")

    def snapshot(self) -> EngineStatus:
        with self._lock:
            return EngineStatus(
                is_running=self.status.is_running,
                last_message=self.status.last_message,
                last_error=self.status.last_error,
                last_started_at=self.status.last_started_at,
                recent_events=list(self._recent_events),
                shortlisted_rows=list(self._shortlisted.values())[:300],
            )

    def sample_data_snapshot(self) -> tuple[str, list[dict[str, Any]]]:
        with self._lock:
            minute_key = self._current_minute_key or "--"
            rows: list[dict[str, Any]] = []
            for symbol in sorted(self._baseline_by_symbol.keys()):
                base = self._baseline_by_symbol.get(symbol, {})
                tick = self._last_tick_by_symbol.get(symbol, {})
                ltq_sum = float(self._minute_ltq_sum.get(symbol, 0.0))
                sma_value = float(base.get("sma_value") or 0.0)
                rows.append(
                    {
                        "symbol": symbol,
                        "ltq_sum": round(ltq_sum, 2),
                        "trade_value_sum_cr": round(
                            float(self._minute_trade_value_sum.get(symbol, 0.0)) / 1e7, 4
                        ),
                        "sma_value": round(sma_value, 2),
                        "relative_vs_sma": round((ltq_sum / sma_value), 3) if sma_value > 0 else 0.0,
                        "last_traded_qty": tick.get("last_traded_qty"),
                        "ltp": tick.get("ltp"),
                        "exch_time": tick.get("exch_time", "--"),
                    }
                )
            return minute_key, rows

    def _is_active(self, run_id: int) -> bool:
        with self._lock:
            return run_id == self._active_run_id and self.status.is_running

    def _set_message(self, message: str, run_id: int | None = None) -> None:
        if run_id is not None and not self._is_active(run_id):
            return
        with self._lock:
            self.status.last_message = message
        self._push_event(message)

    def _push_event(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        print(line)
        with self._lock:
            self._recent_events.insert(0, line)
            self._recent_events = self._recent_events[:20]

    def _set_error(self, message: str) -> None:
        with self._lock:
            self.status.last_error = message
            self.status.last_message = "Failed"
            self.status.is_running = False
        self._push_event(f"FAILED: {message}")

    def _run_pipeline(
        self,
        sma_period: int,
        run_id: int,
        websocket_start_at_ist: datetime | None = None,
    ) -> None:
        try:
            self._set_message("Refreshing symbols CSV...", run_id)
            self._refresh_symbols_csv()
            if not self._is_active(run_id):
                return
            symbols = self._load_symbols_from_csv()
            if not symbols:
                raise RuntimeError("No symbols found after refresh.")

            self._set_message("Logging in to FYERS...", run_id)
            fyers, access_token = self._login_fyers_from_csv()
            if not self._is_active(run_id):
                return

            self._set_message("Fetching historical candles + SMA...", run_id)
            rows = self._fetch_and_build_snapshots(
                fyers=fyers,
                symbols=symbols,
                sma_period=sma_period,
                run_id=run_id,
            )
            if not self._is_active(run_id):
                return
            self._save_snapshots(rows)
            self._baseline_by_symbol = {row["symbol_name"]: row for row in rows}
            self._set_message(f"Saved {len(rows)} rows to {COMBINED_CSV_PATH.name}", run_id)

            if self._stop_event.is_set():
                self.stop()
                return

            if websocket_start_at_ist is not None:
                self._wait_until_ist(websocket_start_at_ist, run_id)
                if not self._is_active(run_id):
                    return

            self._set_message("Starting websocket subscriptions...", run_id)
            self._start_websocket(access_token, symbols, run_id)
            self._set_message("Running. Websocket subscribed.", run_id)
        except Exception as exc:  # noqa: BLE001
            self._set_error(str(exc))

    def _refresh_symbols_csv(self) -> list[str]:
        write_symbols_csv()

    def _load_symbols_from_csv(self) -> list[str]:
        if not CSV_PATH.exists():
            return []
        df = pd.read_csv(CSV_PATH)
        all_symbols = [str(x).strip() for x in df.get("symbol_name", []).tolist() if str(x).strip()]
        if BOOTSTRAP_SYMBOL_LIMIT <= 0:
            return all_symbols
        return all_symbols[-BOOTSTRAP_SYMBOL_LIMIT:]

    def _daily_scheduler_loop(self, sma_period: int) -> None:
        while not self._scheduler_stop_event.is_set():
            next_prep = self._next_ist_occurrence(
                hour=AUTO_LOGIN_AND_HISTORY_HOUR_IST,
                minute=AUTO_LOGIN_AND_HISTORY_MINUTE_IST,
            )
            now_ist = datetime.now(IST_ZONE)
            wait_seconds = max((next_prep - now_ist).total_seconds(), 0)
            self._push_event(
                f"Auto run scheduled at {next_prep.strftime('%Y-%m-%d %H:%M:%S IST')} "
                f"(in {int(wait_seconds)}s)."
            )
            if self._scheduler_stop_event.wait(wait_seconds):
                return

            if self.status.is_running:
                self._push_event("Skipped auto start because scanner is already running.")
                continue

            ws_start_at = next_prep.replace(
                hour=AUTO_WS_START_HOUR_IST,
                minute=AUTO_WS_START_MINUTE_IST,
                second=0,
                microsecond=0,
            )
            self._push_event(
                "Auto start triggered: logging in and fetching historical data now. "
                f"Websocket will start at {ws_start_at.strftime('%H:%M IST')}."
            )
            self.start(sma_period=sma_period, websocket_start_at_ist=ws_start_at)

            while self.status.is_running and not self._scheduler_stop_event.is_set():
                time.sleep(5)

    def _next_ist_occurrence(self, hour: int, minute: int) -> datetime:
        now_ist = datetime.now(IST_ZONE)
        target = now_ist.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if now_ist >= target:
            target = target + timedelta(days=1)
        return target

    def _wait_until_ist(self, target_dt_ist: datetime, run_id: int) -> None:
        target = (
            target_dt_ist.replace(tzinfo=IST_ZONE)
            if target_dt_ist.tzinfo is None
            else target_dt_ist.astimezone(IST_ZONE)
        )
        while self._is_active(run_id) and not self._stop_event.is_set():
            now_ist = datetime.now(IST_ZONE)
            remaining = (target - now_ist).total_seconds()
            if remaining <= 0:
                return
            self._set_message(
                f"Historical data ready. Waiting for websocket start at "
                f"{target.strftime('%H:%M:%S IST')} ({int(remaining)}s left)...",
                run_id,
            )
            time.sleep(min(30, max(1, remaining)))

    def _load_fyers_credentials_csv(self) -> dict[str, str]:
        if not FILENAME_CREDENTIALS.exists():
            raise FileNotFoundError(f"Missing {FILENAME_CREDENTIALS.name} in project root.")

        with FILENAME_CREDENTIALS.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # CSV format: Title,Value
        out: dict[str, str] = {}
        for row in rows:
            title = (row.get("Title") or "").strip()
            value = (row.get("Value") or "").strip()
            if title:
                out[title] = value

        required = ["redirect_uri", "client_id", "secret_key", "totpkey", "FY_ID", "PIN"]
        missing = [k for k in required if k not in out or not out[k]]
        if missing:
            raise ValueError(f"Missing required keys in {FILENAME_CREDENTIALS.name}: {missing}")

        return out

    def _login_fyers_from_csv(self) -> tuple[Any, str]:
        creds = self._load_fyers_credentials_csv()

        # automated_login updates globals inside FyresIntegration.py
        automated_login(
            client_id=creds["client_id"],
            secret_key=creds["secret_key"],
            FY_ID=creds["FY_ID"],
            TOTP_KEY=creds["totpkey"],
            PIN=creds["PIN"],
            redirect_uri=creds["redirect_uri"],
        )

        from FyresIntegration import access_token, fyers

        if not access_token or fyers is None:
            raise RuntimeError("FYERS login failed: access_token not found after automated_login().")

        return fyers, access_token

    def _fetch_and_build_snapshots(
        self,
        fyers: Any,
        symbols: list[str],
        sma_period: int,
        run_id: int,
    ) -> list[dict[str, Any]]:
        today_ist = pd.Timestamp.now(tz="Asia/Kolkata").date()
        # Fetch extra calendar days so we reliably get >= sma_period completed 1-min candles.
        trading_minutes_per_day = 375
        days_back = max(14, int(sma_period / trading_minutes_per_day) + 10)
        range_from_dt = date.today() - timedelta(days=days_back)
        range_from = range_from_dt.strftime("%Y-%m-%d")
        range_to = today_ist.strftime("%Y-%m-%d")
        rows: list[dict[str, Any]] = []

        total = len(symbols)
        for idx, symbol in enumerate(symbols, start=1):
            if self._stop_event.is_set() or not self._is_active(run_id):
                break

            if idx > 1 and FETCH_INTERVAL_SECS > 0:
                time.sleep(FETCH_INTERVAL_SECS)

            self._set_message(f"Historical {idx}/{total}: {symbol}", run_id)
            candles = self._fetch_history_with_retry(
                fyers=fyers,
                symbol=symbol,
                range_from=range_from,
                range_to=range_to,
                run_id=run_id,
            )
            if not candles:
                continue

            frame = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            frame["ts"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True).dt.tz_convert(
                "Asia/Kolkata"
            )
            # Strict baseline mode: remove all current-day candles (IST).
            frame = frame[frame["ts"].dt.date < today_ist].copy()
            if frame.empty:
                continue

            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
            # Match chart parameter: SMA(length=sma_period) on 1-min volume bars.
            frame["sma_volume"] = (
                frame["volume"].rolling(window=sma_period, min_periods=sma_period).mean()
            )

            # Use latest completed candle (previous trading day close candle).
            last_row = frame.iloc[-1]
            if pd.isna(last_row["sma_volume"]):
                # If history is shorter than sma_period, average available completed bars.
                last_row = last_row.copy()
                lookback = min(len(frame), max(sma_period, 1))
                last_row["sma_volume"] = float(frame["volume"].tail(lookback).mean())
            row_ts = last_row["ts"].strftime("%Y-%m-%d %H:%M:%S")
            row_close = float(last_row["close"])
            row_volume = float(last_row["volume"])
            self._set_message(
                f"{idx}/{total} {symbol} -> last row {row_ts}, close={row_close}, "
                f"volume={row_volume}, sma={float(last_row['sma_volume']):.2f}",
                run_id,
            )
            rows.append(
                {
                    "symbol_name": symbol,
                    "sma_value": float(last_row["sma_volume"])
                    if pd.notna(last_row["sma_volume"])
                    else None,
                    "sma_period": int(sma_period),
                    "sma_volume": float(last_row["sma_volume"])
                    if pd.notna(last_row["sma_volume"])
                    else None,
                    "volume_value": float(last_row["volume"]),
                    "timestamp": last_row["ts"].strftime("%Y-%m-%d %H:%M:%S"),
                    "open": float(last_row["open"]),
                    "high": float(last_row["high"]),
                    "low": float(last_row["low"]),
                    "close": float(last_row["close"]),
                }
            )
        return rows

    def _fetch_history_with_retry(
        self,
        fyers: Any,
        symbol: str,
        range_from: str,
        range_to: str,
        run_id: int,
    ) -> list[list[Any]]:
        payload = {
            "symbol": symbol,
            "resolution": "1",
            "date_format": "1",
            "range_from": range_from,
            "range_to": range_to,
            "cont_flag": "1",
        }

        while not self._stop_event.is_set() and self._is_active(run_id):
            response = fyers.history(data=payload)
            if self._is_rate_limited(response):
                self._set_message(
                    f"Rate limit hit for {symbol}. Cooling down {RATE_LIMIT_COOLDOWN_SECS}s...",
                    run_id,
                )
                for _ in range(RATE_LIMIT_COOLDOWN_SECS):
                    if self._stop_event.is_set() or not self._is_active(run_id):
                        return []
                    time.sleep(1)
                continue

            candles = response.get("candles", [])
            return candles

        return []

    def _is_rate_limited(self, response: Any) -> bool:
        if not isinstance(response, dict):
            return False
        if response.get("s") != "error":
            return False

        text = " ".join(
            str(x)
            for x in [
                response.get("message", ""),
                response.get("code", ""),
                response.get("error", ""),
            ]
        ).lower()
        keywords = ("rate", "limit", "too many", "429")
        return any(k in text for k in keywords)

    def _save_snapshots(self, rows: list[dict[str, Any]]) -> None:
        COMBINED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        with COMBINED_CSV_PATH.open("w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(
                file_obj,
                fieldnames=[
                    "symbol_name",
                    "sma_value",
                    "sma_period",
                    "sma_volume",
                    "volume_value",
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_snapshot (
                    snapshot_date TEXT NOT NULL,
                    symbol_name TEXT NOT NULL,
                    sma_value REAL,
                    volume_value REAL,
                    timestamp TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (snapshot_date, symbol_name)
                )
                """
            )
            snapshot_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            created_at = datetime.now().isoformat(timespec="seconds")
            conn.executemany(
                """
                INSERT OR REPLACE INTO symbol_snapshot (
                    snapshot_date, symbol_name, sma_value, volume_value, timestamp,
                    open, high, low, close, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_date,
                        row["symbol_name"],
                        row["sma_value"],
                        row["volume_value"],
                        row["timestamp"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        created_at,
                    )
                    for row in rows
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def _disconnect_ws(self) -> None:
        global data_socket
        if self._ws_client is not None:
            data_socket = self._ws_client
            close_fyres_websocket()
            self._ws_client = None

    def _close_stale_process(self) -> None:
        if not WS_STATE_PATH.exists():
            return
        try:
            payload = json.loads(WS_STATE_PATH.read_text(encoding="utf-8"))
            pid = int(payload.get("pid", 0))
        except Exception:  # noqa: BLE001
            WS_STATE_PATH.unlink(missing_ok=True)
            return

        if pid and pid != os.getpid():
            try:
                os.kill(pid, 0)
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/F"],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except OSError:
                pass

        WS_STATE_PATH.unlink(missing_ok=True)

    def _start_websocket(self, access_token: str, symbols: list[str], run_id: int) -> None:
        global data_socket
        self._disconnect_ws()
        self._close_stale_process()
        WS_STATE_PATH.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")

        def on_message(message: dict[str, Any]) -> None:
            if not self._is_active(run_id):
                return
            self._ws_msg_count += 1
            self._apply_scanner_rules(message)

        def on_error(message: Any) -> None:
            if not self._is_active(run_id):
                return
            self._set_error(f"Websocket error: {message}")

        def on_close(message: Any) -> None:
            if not self._stop_event.is_set() and self._is_active(run_id):
                self._set_message(f"Websocket closed: {message}", run_id)

        def on_open() -> None:
            if not self._is_active(run_id):
                return
            self._startup_minute_key = datetime.now().strftime("%Y-%m-%d %H:%M")
            self._scan_ready_from_next_minute = False
            self._set_message(
                f"Websocket live. Waiting for next minute after {self._startup_minute_key}...",
                run_id,
            )
            batch_size = 200
            for start in range(0, len(symbols), batch_size):
                batch = symbols[start : start + batch_size]
                self._ws_client.subscribe(symbols=batch, data_type="SymbolUpdate")
            self._ws_client.keep_running()

        self._ws_client = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path=str(BASE_DIR),
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message,
        )
        data_socket = self._ws_client
        self._ws_client.connect()

    def _apply_scanner_rules(self, message: dict[str, Any]) -> None:
        symbol = str(message.get("symbol") or "").strip()
        if not symbol:
            return

        base = self._baseline_by_symbol.get(symbol)
        if not base:
            return

        raw_ts = message.get("exch_feed_time") or message.get("last_traded_time")
        minute_key = self._to_ist_minute_key(raw_ts)
        if minute_key is None:
            return
        self._roll_minute_if_needed(minute_key)

        raw_ltq = message.get("last_traded_qty")
        if raw_ltq is None:
            raw_ltq = message.get("ltq")
        parsed_ltq: float | None = None
        try:
            if raw_ltq is not None:
                parsed_ltq = max(float(raw_ltq), 0.0)
        except (TypeError, ValueError):
            parsed_ltq = None

        self._last_tick_by_symbol[symbol] = {
            "last_traded_qty": parsed_ltq,
            "exch_time": self._to_ist_second_str(raw_ts),
        }
        ltp_value: float | None = None
        try:
            if message.get("ltp") is not None:
                ltp_value = float(message.get("ltp"))
        except (TypeError, ValueError):
            ltp_value = None

        if parsed_ltq is not None:
            self._minute_ltq_sum[symbol] = self._minute_ltq_sum.get(symbol, 0.0) + parsed_ltq
            if ltp_value is not None:
                self._minute_trade_value_sum[symbol] = (
                    self._minute_trade_value_sum.get(symbol, 0.0) + (ltp_value * parsed_ltq)
                )

        if not self._scan_ready_from_next_minute:
            return

        try:
            ltp = float(message.get("ltp"))
            sma_value = float(base.get("sma_value") or 0.0)
            base_close = float(base.get("close") or 0.0)
        except (TypeError, ValueError):
            return

        rule1_mult = float(self._scan_config.get("rule1_volume_multiplier", 30))
        rule1_value_cr = float(self._scan_config.get("rule1_value_cr", 4))
        rule2_mult = float(self._scan_config.get("rule2_volume_multiplier", 10))
        rule2_value_cr = float(self._scan_config.get("rule2_value_cr", 8))

        # Always use minute LTQ sum for real-time condition checks.
        effective_qty = self._minute_ltq_sum.get(symbol, 0.0)
        self._minute_accum_metric[symbol] = effective_qty
        if effective_qty <= 0:
            return

        trade_value = self._minute_trade_value_sum.get(symbol, 0.0)
        relative_vol = (effective_qty / sma_value) if sma_value > 0 else 0.0
        change_pct = ((ltp - base_close) / base_close * 100.0) if base_close > 0 else 0.0
        condition1 = (
            effective_qty > (rule1_mult * sma_value)
            and trade_value > (rule1_value_cr * 1e7)
        )
        condition2 = (
            effective_qty > (rule2_mult * sma_value)
            and trade_value > (rule2_value_cr * 1e7)
        )

        matched_condition = ""
        if condition1:
            matched_condition = "C1"
        elif condition2:
            matched_condition = "C2"

        # If symbol has already qualified in this minute, don't reevaluate until next minute.
        if symbol in self._matched_this_minute:
            return

        if not matched_condition:
            return

        self._matched_this_minute.add(symbol)
        self._hit_count[symbol] = self._hit_count.get(symbol, 0) + 1

        readable_ts = "--"
        try:
            if raw_ts is not None:
                readable_ts = (
                    pd.to_datetime(int(raw_ts), unit="s", utc=True)
                    .tz_convert("Asia/Kolkata")
                    .strftime("%Y-%m-%d %H:%M:%S")
                )
        except Exception:  # noqa: BLE001
            readable_ts = str(raw_ts or "--")

        row = {
            "time": readable_ts,
            "symbol": symbol,
            "ltp": round(ltp, 2),
            "change_pct": f"{change_pct:+.2f}%",
            "metric_value": round(effective_qty, 2),
            "metric_source": "last_traded_qty",
            "relative_vol": round(relative_vol, 2),
            "trade_value_cr": round(trade_value / 1e7, 3),
            "condition": matched_condition,
            "hits": self._hit_count.get(symbol, 1),
        }
        self._shortlisted[symbol] = row

        self._push_event(
            f"SHORTLISTED {symbol} via {matched_condition} "
            f"(last_traded_qty={effective_qty:.2f}, rel_vol={relative_vol:.2f}x, "
            f"trade_value={trade_value/1e7:.2f} Cr, hits={self._hit_count.get(symbol, 1)})"
        )

    def _to_ist_minute_key(self, epoch_ts: Any) -> str | None:
        try:
            if epoch_ts is None:
                return None
            return datetime.fromtimestamp(int(epoch_ts), tz=IST_ZONE).strftime("%Y-%m-%d %H:%M")
        except Exception:  # noqa: BLE001
            return None

    def _to_ist_second_str(self, epoch_ts: Any) -> str:
        try:
            if epoch_ts is None:
                return "--"
            return datetime.fromtimestamp(int(epoch_ts), tz=IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:  # noqa: BLE001
            return "--"

    def _roll_minute_if_needed(self, minute_key: str) -> None:
        if self._current_minute_key is None:
            self._current_minute_key = minute_key
            return
        if minute_key == self._current_minute_key:
            return

        self._current_minute_key = minute_key
        self._minute_accum_metric = {}
        self._minute_ltq_sum = {}
        self._minute_trade_value_sum = {}
        self._matched_this_minute = set()
        if not self._scan_ready_from_next_minute:
            self._scan_ready_from_next_minute = True
            self._push_event(f"Minute boundary reached at {minute_key}. Live scanner active.")
        else:
            self._push_event(f"New minute {minute_key}. Reset minute accumulators.")


ENGINE = ScannerEngine()
