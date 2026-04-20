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
from collections.abc import Iterable
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
# Persisted shortlist for the current IST "session" (day rolls at 08:00 IST, same as auto prep).
SHORTLIST_STATE_PATH = BASE_DIR / "runtime_shortlist_state.json"
FILENAME_CREDENTIALS = BASE_DIR / "FyersCredentials.csv"
BOOTSTRAP_SYMBOL_LIMIT = 0
RATE_LIMIT_COOLDOWN_SECS = 30
FETCH_INTERVAL_SECS = 0.3
AUTO_LOGIN_AND_HISTORY_HOUR_IST = 8
AUTO_LOGIN_AND_HISTORY_MINUTE_IST = 0
AUTO_WS_START_HOUR_IST = 9
AUTO_WS_START_MINUTE_IST = 15
IST_ZONE = ZoneInfo("Asia/Kolkata")
LOG_WS_TICK_LATENCY = False
# Fyers SDK appends each subscribe to internal scrips_per_channel; unsubscribe first on resubscribe.
WS_RESUBSCRIBE_SETTLE_SEC = 2.0
WS_SYMBOL_DATA_TYPE = "SymbolUpdate"
# After a resubscribe burst, avoid churning subscribe/unsubscribe when the tape stays quiet.
WS_RESUBSCRIBE_COOLDOWN_SEC = 90.0
# If last symbol tick's exchange epoch is this far behind wall clock, resubscribe only replays stale LTP.
WS_EXCHANGE_TIME_STALE_SKIP_RESUB_SEC = 600.0
WS_STALE_FEED_NOTICE_INTERVAL_SEC = 300.0


def dedupe_symbols_preserve_order(symbols: Iterable[str]) -> list[str]:
    """Strip, drop empties, first occurrence wins (stable order)."""
    seen: set[str] = set()
    out: list[str] = []
    for sym in symbols:
        s = str(sym).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def shortlist_session_date_ist(now: datetime | None = None) -> str:
    """IST calendar date for shortlist persistence; before 08:00 IST, still 'yesterday'."""
    z = IST_ZONE
    d = now or datetime.now(z)
    if d.tzinfo is None:
        d = d.replace(tzinfo=z)
    else:
        d = d.astimezone(z)
    if d.hour < AUTO_LOGIN_AND_HISTORY_HOUR_IST:
        d = d - timedelta(days=1)
    return d.strftime("%Y-%m-%d")


WS_LATENCY_SUMMARY_INTERVAL_SECS = 2.0
# Evaluate rules on cached websocket state every 1s; UI polls every 2s (templates).
EVAL_INTERVAL_SECS = 1.0
TRADE_MONITOR_INTERVAL_SECS = 1.0
SHORTLIST_MAX_EVENTS = 2000
ORDER_LOG_MAX_EVENTS = 4000
EVAL_CONSOLE_HEARTBEAT = True
DEBUG_PRINT_SUBSCRIBED_SYMBOLS = True
DEBUG_PRINT_WS_RAW_MESSAGES = True
WS_RAW_MAX_PRINTS_PER_SEC = 12
WS_SUBSCRIBE_SYMBOLS_PREVIEW_COUNT = 20
# Print latest websocket payload every eval second (for live feed diagnostics).
DEBUG_PRINT_WS_FEED_EVERY_SEC = True
WS_FEED_PRINT_MAX_CHARS = 700

data_socket: data_ws.FyersDataSocket | None = None


def close_fyres_websocket() -> None:
    """
    Close the active Fyers data websocket connection, if any.
    Clears fyers_apiv3's process-wide singleton so the next session builds a fresh client.
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
        else:
            print("[WS] close_fyres_websocket called but no active socket.")
    except Exception as e:
        print("[WS] Error while closing FyersDataSocket:", e)
    finally:
        data_socket = None
        try:
            setattr(data_ws.FyersDataSocket, "_instance", None)
        except Exception:
            pass


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
        # Re-entrant lock prevents self-deadlock when logging from locked sections.
        self._lock = threading.RLock()
        self._worker: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._ws_client: data_ws.FyersDataSocket | None = None
        self._recent_events: list[str] = []
        self._ws_msg_count = 0
        self._baseline_by_symbol: dict[str, dict[str, Any]] = {}
        # Append-only shortlist: one row per qualification event (latest first in UI).
        self._shortlist_events: list[dict[str, Any]] = []
        self._symbol_today_hits: dict[str, int] = {}
        # IST session key (shortlist_session_date_ist); empty until bootstrap.
        self._shortlist_session: str = ""
        self._shortlist_state_mtime: float = 0.0
        self._last_qual_true: dict[str, bool] = {}
        # Per-symbol VTT baseline for current IST minute (first tick in that minute).
        self._vtt_baseline: dict[str, float] = {}
        self._vtt_symbol_minute: dict[str, str] = {}
        self._current_minute_key: str | None = None
        self._startup_minute_key: str | None = None
        # Wall-clock IST when historical SMA rows were last loaded into _baseline_by_symbol.
        self._hist_baseline_loaded_at_ist: str | None = None
        # Wall-clock IST when we first saw the current exchange-minute bucket (VTT baseline window).
        self._vtt_minute_anchor_wall_ist: str | None = None
        self._scan_ready_from_next_minute = False
        self._last_tick_by_symbol: dict[str, dict[str, Any]] = {}
        self._ws_update_seq = 0
        self._subscribed_symbols: list[str] = []
        self._last_ws_msg_monotonic: float = 0.0
        self._last_ws_health_log_monotonic: float = 0.0
        self._last_ws_resubscribe_monotonic: float = 0.0
        self._ws_last_symbol_feed_epoch: float | None = None
        self._last_ws_stale_feed_notice_monotonic: float = 0.0
        self._ws_last_raw_message: dict[str, Any] = {}
        self._ws_last_raw_received_at: str = "--"
        self._ws_last_message_summary: dict[str, Any] = {}
        self._ws_msg_count_last_heartbeat: int = 0
        self._ws_symbol_msg_count: int = 0
        self._ws_nonsymbol_msg_count: int = 0
        self._ws_symbol_msg_count_last_heartbeat: int = 0
        self._ws_nonsymbol_msg_count_last_heartbeat: int = 0
        self._ws_raw_log_window_sec: int = 0
        self._ws_raw_log_count: int = 0
        self._trade_client: Any | None = None
        self._managed_trades: dict[str, dict[str, Any]] = {}
        self._order_logs: list[dict[str, Any]] = []
        self._trade_monitor_thread: threading.Thread | None = None
        self._trade_monitor_stop_event = threading.Event()
        self._positions_cache_rows: list[dict[str, Any]] = []
        self._positions_cache_at_monotonic: float = 0.0
        self._scan_config: dict[str, Any] = {
            "rule1_volume_multiplier": 30,
            "rule1_sma_period": 1125,
            "rule1_value_cr": 4,
            "rule2_volume_multiplier": 10,
            "rule2_sma_period": 1125,
            "rule2_value_cr": 6,
            "metric_source": "vol_traded_today_delta",
            "watchlist": [],
        }
        self._active_run_id = 0
        self._scheduler_thread: threading.Thread | None = None
        self._scheduler_stop_event = threading.Event()
        self._eval_thread: threading.Thread | None = None
        self._eval_stop_event = threading.Event()
        self._eval_run_id = 0
        with self._lock:
            self._bootstrap_shortlist_state_locked()

    def _try_acquire_lock(self, timeout_secs: float = 1.0) -> bool:
        try:
            return bool(self._lock.acquire(timeout=timeout_secs))
        except Exception:  # noqa: BLE001
            return False

    def start(
        self,
        sma_period_rule1: int,
        sma_period_rule2: int | None = None,
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
            self._ensure_shortlist_session_locked()
            self._reload_shortlist_if_disk_newer()
            self._last_qual_true = {}
            self._vtt_baseline = {}
            self._vtt_symbol_minute = {}
            self._current_minute_key = None
            self._startup_minute_key = None
            self._hist_baseline_loaded_at_ist = None
            self._vtt_minute_anchor_wall_ist = None
            self._scan_ready_from_next_minute = False
            self._last_tick_by_symbol = {}
            self._ws_update_seq = 0
            self._subscribed_symbols = []
            self._last_ws_msg_monotonic = 0.0
            self._last_ws_health_log_monotonic = 0.0
            self._last_ws_resubscribe_monotonic = 0.0
            self._ws_last_symbol_feed_epoch = None
            self._last_ws_stale_feed_notice_monotonic = 0.0
            self._ws_last_raw_message = {}
            self._ws_last_raw_received_at = "--"
            self._ws_last_message_summary = {}
            self._ws_msg_count_last_heartbeat = 0
            self._ws_symbol_msg_count = 0
            self._ws_nonsymbol_msg_count = 0
            self._ws_symbol_msg_count_last_heartbeat = 0
            self._ws_nonsymbol_msg_count_last_heartbeat = 0
            self._ws_raw_log_window_sec = 0
            self._ws_raw_log_count = 0
            self._managed_trades = {}
            self._positions_cache_rows = []
            self._positions_cache_at_monotonic = 0.0
            self._trade_monitor_stop_event.clear()
            if self._trade_monitor_thread is None or not self._trade_monitor_thread.is_alive():
                self._trade_monitor_thread = threading.Thread(
                    target=self._trade_monitor_loop,
                    name="scanner-trade-monitor",
                    daemon=True,
                )
                self._trade_monitor_thread.start()
            self._eval_stop_event.clear()
            self._eval_run_id = run_id
            if scan_config:
                self._scan_config.update(scan_config)
            sma2 = int(sma_period_rule2) if sma_period_rule2 is not None else int(sma_period_rule1)
            self._worker = threading.Thread(
                target=self._run_pipeline,
                args=(int(sma_period_rule1), sma2, run_id, websocket_start_at_ist),
                name="scanner-engine",
                daemon=False,
            )
            self._worker.start()

    def start_daily_scheduler(self, sma_period_rule1: int = 1125, sma_period_rule2: int | None = None) -> None:
        with self._lock:
            if self._scheduler_thread is not None and self._scheduler_thread.is_alive():
                return
            self._scheduler_stop_event.clear()
            sma2 = int(sma_period_rule2) if sma_period_rule2 is not None else int(sma_period_rule1)
            self._scheduler_thread = threading.Thread(
                target=self._daily_scheduler_loop,
                args=(int(sma_period_rule1), sma2),
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

    def _as_float(self, value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _as_int(self, value: Any) -> int | None:
        try:
            if value is None:
                return None
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def _extract_order_id(self, response: Any) -> str | None:
        if not isinstance(response, dict):
            return None
        keys = ("id", "order_id", "orderId", "norenordno")
        for key in keys:
            value = response.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        payload = response.get("data")
        if isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    def _extract_orderbook_rows(self, response: Any) -> list[dict[str, Any]]:
        if not isinstance(response, dict):
            return []
        for key in ("orderBook", "orderbook", "orders", "data"):
            value = response.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                nested = value.get("orderBook") or value.get("orders")
                if isinstance(nested, list):
                    return [x for x in nested if isinstance(x, dict)]
        return []

    def _extract_positions_rows(self, response: Any) -> list[dict[str, Any]]:
        if not isinstance(response, dict):
            return []
        for key in ("netPositions", "net_positions", "positions", "data"):
            value = response.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                nested = value.get("netPositions") or value.get("positions")
                if isinstance(nested, list):
                    return [x for x in nested if isinstance(x, dict)]
        return []

    def _broker_response_error_message(self, response: Any) -> str:
        """Return broker error text if response indicates failure, else empty string."""
        if not isinstance(response, dict):
            return ""
        s_flag = str(response.get("s") or response.get("status") or "").strip().lower()
        code_val = self._as_int(response.get("code"))
        msg = str(response.get("message") or response.get("error") or "").strip()
        if s_flag == "error":
            return msg or "broker returned error status"
        if code_val is not None and code_val < 0:
            return msg or f"broker error code {code_val}"
        # Some payloads use success text/status code only.
        if s_flag in {"", "ok", "success"} and (code_val is None or code_val >= 0):
            return ""
        if s_flag not in {"", "ok", "success"}:
            return msg or f"broker status {s_flag}"
        return ""

    def _normalize_order_state(self, row: dict[str, Any]) -> str:
        text = str(
            row.get("status")
            or row.get("orderStatus")
            or row.get("order_status")
            or row.get("message")
            or ""
        ).strip().lower()
        if any(k in text for k in ("filled", "traded", "complete", "executed")):
            return "filled"
        if any(k in text for k in ("cancel", "rejected", "expired", "failed")):
            return "closed_bad"
        if any(k in text for k in ("open", "pending", "transit", "trigger")):
            return "open"
        return text or "unknown"

    def _find_order_row_by_id(self, client: Any, order_id: str) -> dict[str, Any] | None:
        if not order_id:
            return None
        try:
            ob = client.orderbook()
        except Exception:  # noqa: BLE001
            return None
        rows = self._extract_orderbook_rows(ob)
        if not rows:
            return None
        keys = ("id", "order_id", "orderId", "norenordno")
        for row in rows:
            for key in keys:
                value = row.get(key)
                if str(value or "").strip() == order_id:
                    return row
        return None

    def _position_qty_for_symbol(self, symbol: str) -> int:
        rows = self.net_positions_snapshot(force_refresh=True)
        symbol_key = str(symbol or "").strip()
        for row in rows:
            if str(row.get("symbol") or "").strip() == symbol_key:
                return int(row.get("net_qty") or 0)
        return 0

    def _refresh_net_positions_cache(self) -> list[dict[str, Any]]:
        with self._lock:
            client = self._trade_client
        if client is None:
            with self._lock:
                self._positions_cache_rows = []
                self._positions_cache_at_monotonic = time.monotonic()
            return []

        try:
            payload = client.positions()
        except Exception:  # noqa: BLE001
            return []

        rows: list[dict[str, Any]] = []
        for item in self._extract_positions_rows(payload):
            symbol = str(item.get("symbol") or item.get("tradingsymbol") or "").strip()
            if not symbol:
                continue
            net_qty = self._as_int(
                item.get("netQty")
                or item.get("netqty")
                or item.get("qty")
                or item.get("quantity")
            ) or 0
            realized = self._as_float(
                item.get("realized_profit")
                or item.get("realizedPnl")
                or item.get("pl_realized")
                or item.get("rpnl")
                or 0
            ) or 0.0
            unrealized = self._as_float(
                item.get("unrealized_profit")
                or item.get("unrealizedPnl")
                or item.get("pl")
                or item.get("upnl")
                or 0
            ) or 0.0
            ltp = self._as_float(item.get("ltp") or item.get("last_price"))
            rows.append(
                {
                    "symbol": symbol,
                    "net_qty": int(net_qty),
                    "side": "LONG" if net_qty > 0 else ("SHORT" if net_qty < 0 else "FLAT"),
                    "realized_pnl": round(realized, 2),
                    "unrealized_pnl": round(unrealized, 2),
                    "ltp": round(float(ltp), 2) if ltp is not None else None,
                }
            )
        rows.sort(key=lambda r: (r["symbol"], -abs(int(r["net_qty"]))))
        with self._lock:
            self._positions_cache_rows = rows
            self._positions_cache_at_monotonic = time.monotonic()
        return [dict(x) for x in rows]

    def net_positions_snapshot(self, force_refresh: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(x) for x in self._positions_cache_rows]
            ts = float(self._positions_cache_at_monotonic or 0.0)
        if not force_refresh and rows and (time.monotonic() - ts) <= 2.0:
            return rows
        return self._refresh_net_positions_cache()

    def managed_trades_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(v) for v in self._managed_trades.values()]
        rows.sort(key=lambda x: str(x.get("updated_at") or ""), reverse=True)
        return rows

    def _append_order_log(
        self,
        *,
        action: str,
        symbol: str,
        side: str,
        status: str,
        request_payload: Any,
        response_payload: Any,
        message: str = "",
    ) -> None:
        now_txt = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        rec = {
            "log_id": f"log-{int(time.time() * 1000)}",
            "created_at": now_txt,
            "action": str(action or "").strip(),
            "symbol": str(symbol or "").strip(),
            "side": str(side or "").strip(),
            "status": str(status or "").strip(),
            "message": str(message or "").strip(),
            "request": request_payload,
            "response": response_payload,
        }
        with self._lock:
            self._order_logs.append(rec)
            if len(self._order_logs) > ORDER_LOG_MAX_EVENTS:
                self._order_logs = self._order_logs[-ORDER_LOG_MAX_EVENTS:]

    def order_logs_snapshot(
        self,
        *,
        filter_mode: str = "today",
        custom_date: str = "",
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(x) for x in self._order_logs]
        mode = str(filter_mode or "today").strip().lower()
        date_key = str(custom_date or "").strip()
        if mode == "today":
            date_key = datetime.now(IST_ZONE).strftime("%Y-%m-%d")
        if mode in {"today", "custom"} and date_key:
            rows = [r for r in rows if str(r.get("created_at") or "").startswith(date_key)]
        rows.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
        return rows

    def place_limit_order_from_latest_tick(
        self,
        symbol: str,
        side: str,
        quantity: int,
        target_pct: float,
        stop_loss_pct: float,
    ) -> dict[str, Any]:
        side_norm = str(side or "").strip().upper()
        if side_norm not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        qty = max(int(quantity), 1)
        target_pct = max(float(target_pct), 0.0)
        stop_loss_pct = max(float(stop_loss_pct), 0.0)
        symbol_key = str(symbol or "").strip()
        if not symbol_key:
            raise ValueError("symbol is required")

        with self._lock:
            if not self.status.is_running:
                raise RuntimeError("scanner is not running")
            tick = self._last_tick_by_symbol.get(symbol_key) or {}
            ltp_raw = tick.get("ltp")
            try:
                base_price = float(ltp_raw)
            except (TypeError, ValueError):
                raise ValueError(f"latest price unavailable for {symbol_key}") from None
            client = self._trade_client

        if client is None:
            raise RuntimeError("scanner not ready for order placement; start scanner first")

        side_num = 1 if side_norm == "BUY" else -1
        limit_price = round(base_price, 2)
        target_delta = round(limit_price * (target_pct / 100.0), 2)
        stop_delta = round(limit_price * (stop_loss_pct / 100.0), 2)
        if side_norm == "BUY":
            target_price = round(limit_price + target_delta, 2)
            stop_price = round(max(limit_price - stop_delta, 0.0), 2)
        else:
            target_price = round(max(limit_price - target_delta, 0.0), 2)
            stop_price = round(limit_price + stop_delta, 2)

        payload = {
            "symbol": symbol_key,
            "qty": qty,
            "type": 1,  # Limit order
            "side": side_num,
            "productType": "INTRADAY",
            "limitPrice": limit_price,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            # Keep TP/SL as local scanner-managed logic to avoid broker-side validation
            # issues on regular limit orders ("takeProfit does not match: 0").
            "stopLoss": 0,
            "takeProfit": 0,
            "orderTag": "scanner-ui-limit",
        }
        try:
            response = client.place_order(data=payload)
        except Exception as exc:  # noqa: BLE001
            self._append_order_log(
                action="place_entry",
                symbol=symbol_key,
                side=side_norm,
                status="error",
                request_payload=payload,
                response_payload={"error": str(exc)},
                message="place_order failed",
            )
            raise
        broker_err = self._broker_response_error_message(response)
        if broker_err:
            self._append_order_log(
                action="place_entry",
                symbol=symbol_key,
                side=side_norm,
                status="error",
                request_payload=payload,
                response_payload=response,
                message=f"broker rejected entry: {broker_err}",
            )
            raise RuntimeError(f"entry rejected by broker: {broker_err}")
        order_id = self._extract_order_id(response)
        now_txt = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        trade_id = order_id or f"local-{int(time.time() * 1000)}"
        with self._lock:
            self._managed_trades[trade_id] = {
                "trade_id": trade_id,
                "entry_order_id": order_id,
                "exit_order_id": None,
                "symbol": symbol_key,
                "side": side_norm,
                "quantity": qty,
                "entry_price": limit_price,
                "target_price": target_price,
                "stop_loss_price": stop_price,
                "target_pct": target_pct,
                "stop_loss_pct": stop_loss_pct,
                "status": "pending_entry" if order_id else "active",
                "close_reason": "",
                "created_at": now_txt,
                "filled_at": "",
                "updated_at": now_txt,
                "last_price": limit_price,
                "last_exit_attempt_epoch": 0.0,
                "last_modify_attempt_epoch": 0.0,
                "last_modified_price": limit_price,
            }
        self._append_order_log(
            action="place_entry",
            symbol=symbol_key,
            side=side_norm,
            status="ok",
            request_payload=payload,
            response_payload=response,
            message=(
                f"entry_order_id={order_id or '-'} "
                f"(local_tp={target_price:.2f} local_sl={stop_price:.2f})"
            ),
        )
        self._push_event(
            f"ORDER {side_norm} {symbol_key} qty={qty} limit={limit_price:.2f} "
            f"tp={target_price:.2f} ({target_pct:.2f}%) sl={stop_price:.2f} ({stop_loss_pct:.2f}%)"
        )
        return {
            "trade_id": trade_id,
            "entry_order_id": order_id,
            "symbol": symbol_key,
            "side": side_norm,
            "quantity": qty,
            "limit_price": limit_price,
            "target_pct": target_pct,
            "stop_loss_pct": stop_loss_pct,
            "target_price": target_price,
            "stop_loss_price": stop_price,
            "stop_loss_points": stop_delta,
            "take_profit_points": target_delta,
            "broker_payload": payload,
            "broker_response": response,
        }

    def _close_position_market(self, symbol: str, net_qty: int, reason: str) -> dict[str, Any]:
        symbol_key = str(symbol or "").strip()
        if not symbol_key:
            raise ValueError("symbol is required")
        if net_qty == 0:
            return {"ok": True, "symbol": symbol_key, "message": "already flat"}
        with self._lock:
            client = self._trade_client
            tick = self._last_tick_by_symbol.get(symbol_key) or {}
            ltp = self._as_float(tick.get("ltp")) or 0.0
        if client is None:
            raise RuntimeError("trading client unavailable")

        side_num = -1 if net_qty > 0 else 1
        qty = abs(int(net_qty))
        payload = {
            "symbol": symbol_key,
            "qty": qty,
            "type": 2,  # Market exit
            "side": side_num,
            "productType": "INTRADAY",
            "limitPrice": 0,
            "stopPrice": 0,
            "validity": "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
            "stopLoss": 0,
            "takeProfit": 0,
            "orderTag": "scanner-auto-exit",
        }
        try:
            response = client.place_order(data=payload)
        except Exception as exc:  # noqa: BLE001
            self._append_order_log(
                action="exit_position",
                symbol=symbol_key,
                side="SELL" if side_num < 0 else "BUY",
                status="error",
                request_payload=payload,
                response_payload={"error": str(exc)},
                message=f"exit failed ({reason})",
            )
            raise
        exit_order_id = self._extract_order_id(response)
        self._append_order_log(
            action="exit_position",
            symbol=symbol_key,
            side="SELL" if side_num < 0 else "BUY",
            status="ok",
            request_payload=payload,
            response_payload=response,
            message=f"reason={reason}",
        )
        self._push_event(
            f"EXIT {symbol_key} qty={qty} reason={reason} "
            f"(side={'SELL' if side_num < 0 else 'BUY'}, ltp={ltp:.2f})"
        )
        return {
            "ok": True,
            "symbol": symbol_key,
            "qty": qty,
            "exit_side": "SELL" if side_num < 0 else "BUY",
            "exit_order_id": exit_order_id,
            "broker_response": response,
        }

    def exit_position(self, symbol: str) -> dict[str, Any]:
        qty = self._position_qty_for_symbol(symbol)
        result = self._close_position_market(symbol=symbol, net_qty=qty, reason="manual_exit")
        now_txt = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        symbol_key = str(symbol or "").strip()
        with self._lock:
            for trade in self._managed_trades.values():
                if str(trade.get("symbol") or "").strip() != symbol_key:
                    continue
                if trade.get("status") in {"closed", "entry_failed"}:
                    continue
                trade["status"] = "closed"
                trade["close_reason"] = "manual_exit"
                trade["updated_at"] = now_txt
                if result.get("exit_order_id"):
                    trade["exit_order_id"] = result.get("exit_order_id")
        self.net_positions_snapshot(force_refresh=True)
        return result

    def _trade_monitor_loop(self) -> None:
        while not self._trade_monitor_stop_event.is_set():
            try:
                self._monitor_managed_trades_once()
            except Exception as exc:  # noqa: BLE001
                self._push_event(f"Trade monitor warning: {exc}")
            self._trade_monitor_stop_event.wait(TRADE_MONITOR_INTERVAL_SECS)

    def _monitor_managed_trades_once(self) -> None:
        with self._lock:
            if not self.status.is_running:
                return
            client = self._trade_client
            trades = [dict(v) for v in self._managed_trades.values()]
        if client is None or not trades:
            return

        now_txt = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        for trade in trades:
            trade_id = str(trade.get("trade_id") or "")
            status = str(trade.get("status") or "")
            symbol = str(trade.get("symbol") or "")
            entry_order_id = str(trade.get("entry_order_id") or "")
            if not trade_id or not symbol:
                continue

            if status == "pending_entry" and entry_order_id:
                row = self._find_order_row_by_id(client, entry_order_id)
                if row:
                    order_state = self._normalize_order_state(row)
                    if order_state == "open":
                        self._modify_open_entry_to_best_price(client=client, trade_id=trade_id)
                    if order_state == "filled":
                        fill_price = self._as_float(
                            row.get("tradedPrice")
                            or row.get("avgPrice")
                            or row.get("average_price")
                            or row.get("limitPrice")
                        )
                        with self._lock:
                            cur = self._managed_trades.get(trade_id)
                            if cur:
                                cur["status"] = "active"
                                cur["filled_at"] = now_txt
                                cur["updated_at"] = now_txt
                                if fill_price is not None and fill_price > 0:
                                    cur["entry_price"] = round(fill_price, 2)
                                    tp_pct = self._as_float(cur.get("target_pct")) or 0.0
                                    sl_pct = self._as_float(cur.get("stop_loss_pct")) or 0.0
                                    if str(cur.get("side") or "") == "BUY":
                                        cur["target_price"] = round(fill_price * (1 + (tp_pct / 100.0)), 2)
                                        cur["stop_loss_price"] = round(max(fill_price * (1 - (sl_pct / 100.0)), 0.0), 2)
                                    else:
                                        cur["target_price"] = round(max(fill_price * (1 - (tp_pct / 100.0)), 0.0), 2)
                                        cur["stop_loss_price"] = round(fill_price * (1 + (sl_pct / 100.0)), 2)
                        self._append_order_log(
                            action="entry_filled",
                            symbol=symbol,
                            side=str(trade.get("side") or ""),
                            status="ok",
                            request_payload={"entry_order_id": entry_order_id, "trade_id": trade_id},
                            response_payload=row,
                            message=f"entry filled at {fill_price if fill_price is not None else '-'}",
                        )
                        self._push_event(
                            f"ENTRY FILLED {symbol} order={entry_order_id} "
                            f"trade_id={trade_id}"
                        )
                        status = "active"
                    elif order_state == "closed_bad":
                        with self._lock:
                            cur = self._managed_trades.get(trade_id)
                            if cur:
                                cur["status"] = "entry_failed"
                                cur["close_reason"] = "entry_rejected_or_cancelled"
                                cur["updated_at"] = now_txt
                        self._append_order_log(
                            action="entry_failed",
                            symbol=symbol,
                            side=str(trade.get("side") or ""),
                            status="error",
                            request_payload={"entry_order_id": entry_order_id, "trade_id": trade_id},
                            response_payload=row,
                            message="entry rejected/cancelled/expired",
                        )
                        self._push_event(f"ENTRY FAILED {symbol} order={entry_order_id}")
                        continue

            if status != "active":
                continue

            with self._lock:
                cur = self._managed_trades.get(trade_id)
                tick = self._last_tick_by_symbol.get(symbol) or {}
            if not cur:
                continue
            ltp = self._as_float(tick.get("ltp"))
            if ltp is None:
                continue
            side = str(cur.get("side") or "")
            live_qty = self._position_qty_for_symbol(symbol)
            expected_sign = 1 if side == "BUY" else (-1 if side == "SELL" else 0)
            if live_qty == 0:
                with self._lock:
                    cur_flat = self._managed_trades.get(trade_id)
                    if cur_flat:
                        cur_flat["status"] = "closed"
                        cur_flat["close_reason"] = "manual_or_external_exit_detected"
                        cur_flat["updated_at"] = now_txt
                self._append_order_log(
                    action="trade_closed",
                    symbol=symbol,
                    side=side,
                    status="ok",
                    request_payload={"trade_id": trade_id},
                    response_payload={"net_qty": live_qty},
                    message="position already flat; auto-exit skipped",
                )
                continue
            if expected_sign and (live_qty * expected_sign) < 0:
                with self._lock:
                    cur_flip = self._managed_trades.get(trade_id)
                    if cur_flip:
                        cur_flip["status"] = "closed"
                        cur_flip["close_reason"] = "position_side_reversed_externally"
                        cur_flip["updated_at"] = now_txt
                self._append_order_log(
                    action="trade_closed",
                    symbol=symbol,
                    side=side,
                    status="ok",
                    request_payload={"trade_id": trade_id},
                    response_payload={"net_qty": live_qty},
                    message="position reversed externally; auto-exit skipped",
                )
                continue
            target_price = self._as_float(cur.get("target_price"))
            stop_price = self._as_float(cur.get("stop_loss_price"))
            hit_target = False
            hit_stop = False
            if side == "BUY":
                hit_target = target_price is not None and ltp >= target_price
                hit_stop = stop_price is not None and ltp <= stop_price
            elif side == "SELL":
                hit_target = target_price is not None and ltp <= target_price
                hit_stop = stop_price is not None and ltp >= stop_price

            with self._lock:
                cur = self._managed_trades.get(trade_id)
                if cur:
                    cur["last_price"] = round(ltp, 2)
                    cur["updated_at"] = now_txt
            if not (hit_target or hit_stop):
                continue

            reason = "target_hit" if hit_target else "stop_loss_hit"
            last_attempt = self._as_float(cur.get("last_exit_attempt_epoch")) or 0.0
            now_epoch = time.monotonic()
            if now_epoch - last_attempt < 2.0:
                continue
            with self._lock:
                cur2 = self._managed_trades.get(trade_id)
                if cur2:
                    cur2["last_exit_attempt_epoch"] = now_epoch

            qty_live = self._position_qty_for_symbol(symbol)
            if qty_live == 0:
                with self._lock:
                    cur3 = self._managed_trades.get(trade_id)
                    if cur3:
                        cur3["status"] = "closed"
                        cur3["close_reason"] = "manual_or_external_exit_detected"
                        cur3["updated_at"] = now_txt
                self._append_order_log(
                    action="trade_closed",
                    symbol=symbol,
                    side=side,
                    status="ok",
                    request_payload={"trade_id": trade_id, "reason": reason},
                    response_payload={"net_qty": qty_live},
                    message="trigger hit but position already flat; auto-exit skipped",
                )
                continue
            exit_result = self._close_position_market(symbol=symbol, net_qty=qty_live, reason=reason)
            with self._lock:
                cur4 = self._managed_trades.get(trade_id)
                if cur4:
                    cur4["status"] = "closed"
                    cur4["close_reason"] = reason
                    cur4["updated_at"] = now_txt
                    cur4["exit_order_id"] = exit_result.get("exit_order_id")
            self.net_positions_snapshot(force_refresh=True)

    def _modify_open_entry_to_best_price(self, client: Any, trade_id: str) -> None:
        with self._lock:
            trade = self._managed_trades.get(trade_id)
            if not trade:
                return
            now_epoch = time.monotonic()
            last_attempt = self._as_float(trade.get("last_modify_attempt_epoch")) or 0.0
            if now_epoch - last_attempt < 2.0:
                return
            trade["last_modify_attempt_epoch"] = now_epoch
            symbol = str(trade.get("symbol") or "").strip()
            side = str(trade.get("side") or "").strip().upper()
            qty = max(int(self._as_int(trade.get("quantity")) or 1), 1)
            entry_order_id = str(trade.get("entry_order_id") or "").strip()
            tick = self._last_tick_by_symbol.get(symbol) or {}
            bid = self._as_float(tick.get("bid_price"))
            ask = self._as_float(tick.get("ask_price"))
            ltp = self._as_float(tick.get("ltp"))
            prev_price = self._as_float(trade.get("last_modified_price")) or self._as_float(
                trade.get("entry_price")
            )

        if not entry_order_id or side not in {"BUY", "SELL"}:
            return
        desired = ask if side == "BUY" else bid
        if desired is None or desired <= 0:
            desired = ltp
        if desired is None or desired <= 0:
            return
        desired = round(float(desired), 2)
        if prev_price is not None and abs(float(prev_price) - desired) < 0.01:
            return

        payload = {
            "id": entry_order_id,
            "type": 1,
            "qty": qty,
            "limitPrice": desired,
            "stopPrice": 0,
        }
        try:
            response = client.modify_order(data=payload)
        except TypeError:
            response = client.modify_order(payload)
        except Exception as exc:  # noqa: BLE001
            self._append_order_log(
                action="modify_entry",
                symbol=symbol,
                side=side,
                status="error",
                request_payload=payload,
                response_payload={"error": str(exc)},
                message="modify_order failed",
            )
            return

        now_txt = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            cur = self._managed_trades.get(trade_id)
            if cur:
                cur["entry_price"] = desired
                cur["updated_at"] = now_txt
                cur["last_modified_price"] = desired
        self._append_order_log(
            action="modify_entry",
            symbol=symbol,
            side=side,
            status="ok",
            request_payload=payload,
            response_payload=response,
            message=f"entry_order_id={entry_order_id}",
        )

    def _persist_shortlist_state_nolock(self) -> None:
        want = shortlist_session_date_ist()
        self._shortlist_session = want
        payload = {
            "session_date": want,
            "events": list(self._shortlist_events),
            "symbol_hits": {str(k): int(v) for k, v in self._symbol_today_hits.items()},
        }
        tmp = SHORTLIST_STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(SHORTLIST_STATE_PATH)
        try:
            self._shortlist_state_mtime = SHORTLIST_STATE_PATH.stat().st_mtime
        except OSError:
            self._shortlist_state_mtime = 0.0

    def _apply_shortlist_payload(self, payload: dict[str, Any]) -> None:
        want = shortlist_session_date_ist()
        file_sess = str(payload.get("session_date") or "")
        if file_sess != want:
            self._shortlist_events = []
            self._symbol_today_hits = {}
            self._shortlist_session = want
            self._persist_shortlist_state_nolock()
            return
        self._shortlist_events = list(payload.get("events") or [])
        sh = payload.get("symbol_hits") or {}
        self._symbol_today_hits = {str(k): int(v) for k, v in sh.items()}
        self._shortlist_session = file_sess

    def _bootstrap_shortlist_state_locked(self) -> None:
        if not SHORTLIST_STATE_PATH.exists():
            self._shortlist_events = []
            self._symbol_today_hits = {}
            self._persist_shortlist_state_nolock()
            return
        try:
            payload = json.loads(SHORTLIST_STATE_PATH.read_text(encoding="utf-8"))
            self._apply_shortlist_payload(payload)
            self._shortlist_state_mtime = SHORTLIST_STATE_PATH.stat().st_mtime
        except Exception:  # noqa: BLE001
            self._shortlist_events = []
            self._symbol_today_hits = {}
            self._persist_shortlist_state_nolock()

    def _reload_shortlist_if_disk_newer(self) -> None:
        if not SHORTLIST_STATE_PATH.exists():
            return
        try:
            st = SHORTLIST_STATE_PATH.stat()
        except OSError:
            return
        if st.st_mtime <= self._shortlist_state_mtime:
            return
        try:
            payload = json.loads(SHORTLIST_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return
        self._apply_shortlist_payload(payload)
        self._shortlist_state_mtime = st.st_mtime

    def _ensure_shortlist_session_locked(self) -> None:
        want = shortlist_session_date_ist()
        if self._shortlist_session == want:
            return
        self._shortlist_events = []
        self._symbol_today_hits = {}
        self._shortlist_session = want
        self._persist_shortlist_state_nolock()

    def _daily_prep_clear_websocket_and_shortlist(self) -> None:
        """08:00 IST: drop stale sockets and start a fresh shortlist session on disk."""
        self._push_event("08:00 IST prep: closing any open FYERS websocket and resetting shortlist session.")
        close_fyres_websocket()
        with self._lock:
            self._disconnect_ws()
            self._close_stale_process()
            want = shortlist_session_date_ist()
            self._shortlist_events = []
            self._symbol_today_hits = {}
            self._shortlist_session = want
            self._last_qual_true = {}
            self._persist_shortlist_state_nolock()

    def stop(self) -> None:
        monitor_thread: threading.Thread | None = None
        with self._lock:
            self._active_run_id += 1
            self._eval_run_id = self._active_run_id
            self._stop_event.set()
            self._eval_stop_event.set()
            self._trade_monitor_stop_event.set()
            monitor_thread = self._trade_monitor_thread
            self._disconnect_ws()
            self._close_stale_process()
            self._trade_client = None
            self.status.is_running = False
            self.status.last_message = "Stopped"
            self._push_event("Pipeline stopped by user.")
        if monitor_thread is not None and monitor_thread.is_alive():
            monitor_thread.join(timeout=2.0)

    def snapshot(self) -> EngineStatus:
        if not self._try_acquire_lock(timeout_secs=1.0):
            # Never block HTTP requests indefinitely; return best-effort view.
            return EngineStatus(
                is_running=self.status.is_running,
                last_message=self.status.last_message or "Busy",
                last_error=self.status.last_error,
                last_started_at=self.status.last_started_at,
                recent_events=[],
                shortlisted_rows=[],
            )
        try:
            self._ensure_shortlist_session_locked()
            self._reload_shortlist_if_disk_newer()
            rows = list(reversed(self._shortlist_events))[:300]
            return EngineStatus(
                is_running=self.status.is_running,
                last_message=self.status.last_message,
                last_error=self.status.last_error,
                last_started_at=self.status.last_started_at,
                recent_events=list(self._recent_events),
                shortlisted_rows=rows,
            )
        finally:
            self._lock.release()

    def baseline_loaded_count(self) -> int:
        if not self._try_acquire_lock(timeout_secs=1.0):
            return 0
        try:
            return len(self._baseline_by_symbol)
        finally:
            self._lock.release()

    def latest_ltp_for_symbol(self, symbol: str) -> float | None:
        symbol_key = str(symbol or "").strip()
        if not symbol_key:
            return None
        if not self._try_acquire_lock(timeout_secs=1.0):
            return None
        try:
            tick = self._last_tick_by_symbol.get(symbol_key) or {}
            raw = tick.get("ltp")
            try:
                return float(raw) if raw is not None else None
            except (TypeError, ValueError):
                return None
        finally:
            self._lock.release()

    def latest_quote_for_symbol(self, symbol: str) -> dict[str, float | None] | None:
        symbol_key = str(symbol or "").strip()
        if not symbol_key:
            return None
        if not self._try_acquire_lock(timeout_secs=1.0):
            return None
        try:
            tick = self._last_tick_by_symbol.get(symbol_key) or {}
            if not tick:
                return None

            def pick_float(*keys: str) -> float | None:
                for key in keys:
                    raw = tick.get(key)
                    try:
                        if raw is not None:
                            return float(raw)
                    except (TypeError, ValueError):
                        continue
                return None

            return {
                "ltp": pick_float("ltp"),
                "upper_circuit": pick_float("upper_circuit", "upper_ckt", "uc", "uc_price"),
                "lower_circuit": pick_float("lower_circuit", "lower_ckt", "lc", "lc_price"),
                "bid_price": pick_float("bid_price"),
                "ask_price": pick_float("ask_price"),
            }
        finally:
            self._lock.release()

    def _sample_data_meta_nolock(self) -> dict[str, Any]:
        return {
            "historical_baseline_loaded_at_ist": self._hist_baseline_loaded_at_ist,
            "vtt_exchange_minute_key": self._current_minute_key,
            "vtt_minute_anchor_wall_ist": self._vtt_minute_anchor_wall_ist,
        }

    def sample_data_snapshot(self) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        if not self._try_acquire_lock(timeout_secs=1.0):
            minute_key = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M")
            return minute_key, [], {
                "historical_baseline_loaded_at_ist": None,
                "vtt_exchange_minute_key": None,
                "vtt_minute_anchor_wall_ist": None,
            }
        try:
            minute_key = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M")
            rows: list[dict[str, Any]] = []
            for symbol in sorted(self._baseline_by_symbol.keys()):
                tick = self._last_tick_by_symbol.get(symbol, {})
                vtt_raw = tick.get("vol_traded_today")
                vtt_f: float | None = None
                try:
                    if vtt_raw is not None:
                        vtt_f = float(vtt_raw)
                except (TypeError, ValueError):
                    vtt_f = None
                baseline = self._vtt_baseline.get(symbol)
                diff: float | None = None
                if vtt_f is not None and baseline is not None:
                    diff = max(vtt_f - float(baseline), 0.0)
                ltp_raw = tick.get("ltp")
                try:
                    ltp_f = float(ltp_raw) if ltp_raw is not None else None
                except (TypeError, ValueError):
                    ltp_f = None
                value_rupees: float | None = None
                if diff is not None and ltp_f is not None:
                    value_rupees = diff * ltp_f
                rows.append(
                    {
                        "symbol": symbol,
                        "vtt_baseline": round(float(baseline), 2) if baseline is not None else None,
                        "vol_traded_today": vtt_f,
                        "vtt_diff": round(diff, 2) if diff is not None else None,
                        "value_rupees": round(value_rupees, 2) if value_rupees is not None else None,
                        "exch_time": tick.get("exch_time", "--"),
                    }
                )
            return minute_key, rows, self._sample_data_meta_nolock()
        finally:
            self._lock.release()

    def sample_data_delta(
        self,
        since_seq: int,
        since_minute: str,
    ) -> tuple[str, int, bool, list[dict[str, Any]], dict[str, Any]]:
        if not self._try_acquire_lock(timeout_secs=1.0):
            minute_key = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M")
            return minute_key, int(self._ws_update_seq), True, [], {
                "historical_baseline_loaded_at_ist": None,
                "vtt_exchange_minute_key": None,
                "vtt_minute_anchor_wall_ist": None,
            }
        try:
            minute_key = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M")
            current_seq = int(self._ws_update_seq)
            force_full = since_minute != minute_key
            rows: list[dict[str, Any]] = []
            symbols = (
                sorted(self._baseline_by_symbol.keys())
                if force_full
                else sorted(
                    s
                    for s, tick in self._last_tick_by_symbol.items()
                    if int(tick.get("_u", 0)) > since_seq
                )
            )

            for symbol in symbols:
                base = self._baseline_by_symbol.get(symbol, {})
                if not base:
                    continue
                tick = self._last_tick_by_symbol.get(symbol, {})
                vtt_raw = tick.get("vol_traded_today")
                vtt_f: float | None = None
                try:
                    if vtt_raw is not None:
                        vtt_f = float(vtt_raw)
                except (TypeError, ValueError):
                    vtt_f = None
                baseline = self._vtt_baseline.get(symbol)
                diff: float | None = None
                if vtt_f is not None and baseline is not None:
                    diff = max(vtt_f - float(baseline), 0.0)
                ltp_raw = tick.get("ltp")
                try:
                    ltp_f = float(ltp_raw) if ltp_raw is not None else None
                except (TypeError, ValueError):
                    ltp_f = None
                value_rupees: float | None = None
                if diff is not None and ltp_f is not None:
                    value_rupees = diff * ltp_f
                rows.append(
                    {
                        "symbol": symbol,
                        "vtt_baseline": round(float(baseline), 2) if baseline is not None else None,
                        "vol_traded_today": vtt_f,
                        "vtt_diff": round(diff, 2) if diff is not None else None,
                        "value_rupees": round(value_rupees, 2) if value_rupees is not None else None,
                        "exch_time": tick.get("exch_time", "--"),
                    }
                )
            return minute_key, current_seq, force_full, rows, self._sample_data_meta_nolock()
        finally:
            self._lock.release()

    def websocket_debug_snapshot(self) -> dict[str, Any]:
        if not self._try_acquire_lock(timeout_secs=1.0):
            return {
                "last_raw_received_at": "--",
                "last_raw_message": {},
                "last_message_summary": {},
                "total_ticks": int(self._ws_msg_count),
                "symbol_ticks": int(self._ws_symbol_msg_count),
                "control_ticks": int(self._ws_nonsymbol_msg_count),
                "last_symbol_feed_lag_sec": None,
            }
        try:
            lag = self._ws_exchange_lag_vs_wall_seconds()
            return {
                "last_raw_received_at": self._ws_last_raw_received_at,
                "last_raw_message": dict(self._ws_last_raw_message or {}),
                "last_message_summary": dict(self._ws_last_message_summary or {}),
                "total_ticks": int(self._ws_msg_count),
                "symbol_ticks": int(self._ws_symbol_msg_count),
                "control_ticks": int(self._ws_nonsymbol_msg_count),
                "last_symbol_feed_lag_sec": round(float(lag), 2) if lag is not None else None,
            }
        finally:
            self._lock.release()

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
        sma_period_rule1: int,
        sma_period_rule2: int,
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
            self._trade_client = fyers

            self._set_message("Fetching historical candles + SMA...", run_id)
            rows = self._fetch_and_build_snapshots(
                fyers=fyers,
                symbols=symbols,
                sma_period_rule1=sma_period_rule1,
                sma_period_rule2=sma_period_rule2,
                run_id=run_id,
            )
            if not self._is_active(run_id):
                return
            self._save_snapshots(rows)
            self._baseline_by_symbol = {row["symbol_name"]: row for row in rows}
            self._hist_baseline_loaded_at_ist = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
            self._set_message(f"Saved {len(rows)} rows to {COMBINED_CSV_PATH.name}", run_id)

            if self._stop_event.is_set():
                self.stop()
                return

            if websocket_start_at_ist is not None:
                self._wait_until_ist(websocket_start_at_ist, run_id)
                if not self._is_active(run_id):
                    return

            # One canonical list after history fetch (unique, same order as successful rows).
            ws_symbols = dedupe_symbols_preserve_order(row["symbol_name"] for row in rows)
            self._set_message(
                f"Starting websocket subscriptions for {len(ws_symbols)} symbols "
                f"(unique post-fetch; CSV had {len(symbols)}).",
                run_id,
            )
            self._start_websocket(access_token, ws_symbols, run_id)
            self._set_message("Running. Websocket subscribed.", run_id)
        except Exception as exc:  # noqa: BLE001
            self._set_error(str(exc))

    def _refresh_symbols_csv(self) -> list[str]:
        write_symbols_csv()

    def _load_symbols_from_csv(self) -> list[str]:
        if not CSV_PATH.exists():
            return []
        df = pd.read_csv(CSV_PATH)
        raw = [str(x).strip() for x in df.get("symbol_name", []).tolist() if str(x).strip()]
        all_symbols = dedupe_symbols_preserve_order(raw)
        if BOOTSTRAP_SYMBOL_LIMIT <= 0:
            return all_symbols
        return all_symbols[-BOOTSTRAP_SYMBOL_LIMIT:]

    def _daily_scheduler_loop(self, sma_period_rule1: int, sma_period_rule2: int) -> None:
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
                self._push_event(
                    "Scheduled 08:00 IST prep: stopping prior scanner run so today's job can start clean."
                )
                self.stop()
                time.sleep(1.0)

            self._daily_prep_clear_websocket_and_shortlist()

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
            self.start(
                sma_period_rule1=sma_period_rule1,
                sma_period_rule2=sma_period_rule2,
                websocket_start_at_ist=ws_start_at,
            )

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
        sma_period_rule1: int,
        sma_period_rule2: int,
        run_id: int,
    ) -> list[dict[str, Any]]:
        today_ist = pd.Timestamp.now(tz="Asia/Kolkata").date()
        # Fetch extra calendar days so we reliably get >= max(rule SMA period) completed 1-min candles.
        max_sma_period = max(int(sma_period_rule1), int(sma_period_rule2), 1)
        trading_minutes_per_day = 375
        days_back = max(14, int(max_sma_period / trading_minutes_per_day) + 10)
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
            # Independent SMA baselines per rule on 1-min volume bars.
            frame["sma_volume_rule1"] = frame["volume"].rolling(
                window=sma_period_rule1, min_periods=sma_period_rule1
            ).mean()
            frame["sma_volume_rule2"] = frame["volume"].rolling(
                window=sma_period_rule2, min_periods=sma_period_rule2
            ).mean()

            # Use latest completed candle (previous trading day close candle).
            last_row = frame.iloc[-1]
            if pd.isna(last_row["sma_volume_rule1"]):
                # If history is shorter than configured period, average available completed bars.
                last_row = last_row.copy()
                lookback1 = min(len(frame), max(sma_period_rule1, 1))
                last_row["sma_volume_rule1"] = float(frame["volume"].tail(lookback1).mean())
            if pd.isna(last_row["sma_volume_rule2"]):
                last_row = last_row.copy()
                lookback2 = min(len(frame), max(sma_period_rule2, 1))
                last_row["sma_volume_rule2"] = float(frame["volume"].tail(lookback2).mean())
            row_ts = last_row["ts"].strftime("%Y-%m-%d %H:%M:%S")
            row_close = float(last_row["close"])
            row_volume = float(last_row["volume"])
            self._set_message(
                f"{idx}/{total} {symbol} -> last row {row_ts}, close={row_close}, "
                f"volume={row_volume}, sma1={float(last_row['sma_volume_rule1']):.2f}, "
                f"sma2={float(last_row['sma_volume_rule2']):.2f}",
                run_id,
            )
            rows.append(
                {
                    "symbol_name": symbol,
                    "sma_value1": float(last_row["sma_volume_rule1"])
                    if pd.notna(last_row["sma_volume_rule1"])
                    else None,
                    "sma_period1": int(sma_period_rule1),
                    "sma_value2": float(last_row["sma_volume_rule2"])
                    if pd.notna(last_row["sma_volume_rule2"])
                    else None,
                    "sma_period2": int(sma_period_rule2),
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
                    "sma_value1",
                    "sma_period1",
                    "sma_value2",
                    "sma_period2",
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
                    sma_value1 REAL,
                    sma_period1 INTEGER,
                    sma_value2 REAL,
                    sma_period2 INTEGER,
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
            # Backward-compatible migration for older DBs that only had one SMA column.
            for alter_sql in (
                "ALTER TABLE symbol_snapshot ADD COLUMN sma_value1 REAL",
                "ALTER TABLE symbol_snapshot ADD COLUMN sma_period1 INTEGER",
                "ALTER TABLE symbol_snapshot ADD COLUMN sma_value2 REAL",
                "ALTER TABLE symbol_snapshot ADD COLUMN sma_period2 INTEGER",
            ):
                try:
                    conn.execute(alter_sql)
                except sqlite3.OperationalError:
                    pass
            snapshot_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            created_at = datetime.now().isoformat(timespec="seconds")
            conn.executemany(
                """
                INSERT OR REPLACE INTO symbol_snapshot (
                    snapshot_date, symbol_name, sma_value1, sma_period1, sma_value2, sma_period2, volume_value, timestamp,
                    open, high, low, close, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_date,
                        row["symbol_name"],
                        row["sma_value1"],
                        row["sma_period1"],
                        row["sma_value2"],
                        row["sma_period2"],
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

    def _close_all_websockets_before_subscribe(self) -> None:
        """Best-effort websocket cleanup before starting a fresh subscribe flow."""
        self._disconnect_ws()
        self._close_stale_process()

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

    def _evaluation_loop(self, run_id: int) -> None:
        while not self._eval_stop_event.is_set() and self._is_active(run_id):
            processed_symbols, new_hits = self._evaluate_all_symbols(run_id)
            self._print_eval_console_heartbeat(processed_symbols, new_hits)
            self._watch_websocket_health(run_id)
            self._eval_stop_event.wait(EVAL_INTERVAL_SECS)

    def _print_eval_console_heartbeat(self, processed_symbols: list[str], new_hits: int) -> None:
        ts = datetime.now(IST_ZONE).strftime("%H:%M:%S")
        total = len(processed_symbols)
        with self._lock:
            ws_total = int(self._ws_msg_count)
            ws_prev = int(self._ws_msg_count_last_heartbeat)
            self._ws_msg_count_last_heartbeat = ws_total
            ws_symbol_total = int(self._ws_symbol_msg_count)
            ws_symbol_prev = int(self._ws_symbol_msg_count_last_heartbeat)
            self._ws_symbol_msg_count_last_heartbeat = ws_symbol_total
            ws_nonsymbol_total = int(self._ws_nonsymbol_msg_count)
            ws_nonsymbol_prev = int(self._ws_nonsymbol_msg_count_last_heartbeat)
            self._ws_nonsymbol_msg_count_last_heartbeat = ws_nonsymbol_total
            ws_last = dict(self._ws_last_message_summary) if self._ws_last_message_summary else {}
            ws_raw = dict(self._ws_last_raw_message) if self._ws_last_raw_message else {}
            ws_raw_at = self._ws_last_raw_received_at
        ws_delta = ws_total - ws_prev
        ws_symbol_delta = ws_symbol_total - ws_symbol_prev
        ws_nonsymbol_delta = ws_nonsymbol_total - ws_nonsymbol_prev
        if ws_last:
            ws_tail = (
                f"last_symbol={ws_last.get('symbol', '-')}, "
                f"ltp={ws_last.get('ltp', '-')}, "
                f"vtt={ws_last.get('vol_traded_today', '-')}, "
                f"ts={ws_last.get('exch_time', '-')}, "
                f"kind={ws_last.get('kind', '-')}, "
                f"info={ws_last.get('info', '-')}"
            )
        else:
            ws_tail = "last_symbol=-, ltp=-, vtt=-, ts=-, kind=-, info=-"
        print(
            f"[WS {ts}] ticks/sec={ws_delta}, symbol_ticks/sec={ws_symbol_delta}, "
            f"control_ticks/sec={ws_nonsymbol_delta}, total_ticks={ws_total}, {ws_tail}"
        )
        if DEBUG_PRINT_WS_FEED_EVERY_SEC:
            if ws_raw:
                try:
                    raw_txt = json.dumps(ws_raw, ensure_ascii=False)
                except Exception:  # noqa: BLE001
                    raw_txt = str(ws_raw)
                if len(raw_txt) > WS_FEED_PRINT_MAX_CHARS:
                    raw_txt = raw_txt[:WS_FEED_PRINT_MAX_CHARS] + "...<truncated>"
                print(f"[WS FEED {ts}] received_at={ws_raw_at} payload={raw_txt}")
            else:
                print(f"[WS FEED {ts}] received_at=-- payload=<none>")
        if total == 0:
            print(f"[EVAL {ts}] processed=0 (waiting for websocket ticks / baselines), new_hits={new_hits}")
            return
        preview_n = min(15, total)
        preview = ", ".join(processed_symbols[:preview_n])
        suffix = " ..." if total > preview_n else ""
        print(f"[EVAL {ts}] processed={total}, new_hits={new_hits}, symbols=[{preview}{suffix}]")

    def _ws_exchange_lag_vs_wall_seconds(self) -> float | None:
        """Seconds between wall clock and last symbol tick's exch_feed_time / last_traded_time epoch."""
        with self._lock:
            ep = self._ws_last_symbol_feed_epoch
        if ep is None:
            return None
        try:
            return max(0.0, time.time() - float(ep))
        except (TypeError, ValueError):
            return None

    def _watch_websocket_health(self, run_id: int) -> None:
        if not self._is_active(run_id):
            return
        now = time.monotonic()
        with self._lock:
            last = float(self._last_ws_msg_monotonic or 0.0)
            can_log = now - float(self._last_ws_health_log_monotonic or 0.0) >= 30.0
            has_symbols = bool(self._subscribed_symbols)
            ws = self._ws_client
        if not has_symbols or ws is None:
            return
        if not self._is_market_hours_ist():
            return
        if last <= 0:
            if can_log:
                with self._lock:
                    self._last_ws_health_log_monotonic = now
                self._push_event("No websocket tick received yet after subscribe (market hours).")
            return
        stale_secs = now - last
        if stale_secs < 20.0:
            return
        lag_wall = self._ws_exchange_lag_vs_wall_seconds()
        if lag_wall is not None and lag_wall > WS_EXCHANGE_TIME_STALE_SKIP_RESUB_SEC:
            with self._lock:
                last_notice = float(self._last_ws_stale_feed_notice_monotonic or 0.0)
            if now - last_notice >= WS_STALE_FEED_NOTICE_INTERVAL_SEC:
                with self._lock:
                    self._last_ws_stale_feed_notice_monotonic = now
                self._push_event(
                    f"Websocket quiet ~{int(stale_secs)}s but last quote is ~{int(lag_wall)}s behind "
                    "wall clock vs exchange time — skipping resubscribe (no live tape / snapshot only)."
                )
            return
        if not can_log:
            return
        last_resub = float(self._last_ws_resubscribe_monotonic or 0.0)
        if last_resub > 0 and (now - last_resub) < WS_RESUBSCRIBE_COOLDOWN_SEC:
            with self._lock:
                self._last_ws_health_log_monotonic = now
            return
        with self._lock:
            self._last_ws_health_log_monotonic = now
            symbols = list(self._subscribed_symbols)
        self._last_ws_resubscribe_monotonic = now
        self._push_event(
            f"No websocket tick for {int(stale_secs)}s; re-subscribing {len(symbols)} symbols "
            "(unsubscribe first so FYERS SDK does not accumulate past 5000)."
        )
        try:
            ws.unsubscribe(symbols=symbols, data_type=WS_SYMBOL_DATA_TYPE)
        except Exception as exc:  # noqa: BLE001
            self._push_event(f"Websocket unsubscribe before resubscribe failed (continuing): {exc}")
        time.sleep(WS_RESUBSCRIBE_SETTLE_SEC)
        try:
            ws.subscribe(symbols=symbols, data_type=WS_SYMBOL_DATA_TYPE)
        except Exception as exc:  # noqa: BLE001
            self._push_event(f"Websocket re-subscribe failed: {exc}")

    def _is_market_hours_ist(self) -> bool:
        now_ist = datetime.now(IST_ZONE)
        minute = now_ist.hour * 60 + now_ist.minute
        start = AUTO_WS_START_HOUR_IST * 60 + AUTO_WS_START_MINUTE_IST
        end = 15 * 60 + 31
        return start <= minute <= end

    def _evaluate_all_symbols(self, run_id: int) -> tuple[list[str], int]:
        processed_symbols: list[str] = []
        new_hits = 0
        if not self._is_active(run_id):
            return processed_symbols, new_hits
        with self._lock:
            if not self._scan_ready_from_next_minute:
                return processed_symbols, new_hits
            self._ensure_shortlist_session_locked()
            rule1_mult = float(self._scan_config.get("rule1_volume_multiplier", 30))
            rule1_value_cr = float(self._scan_config.get("rule1_value_cr", 4))
            rule2_mult = float(self._scan_config.get("rule2_volume_multiplier", 10))
            rule2_value_cr = float(self._scan_config.get("rule2_value_cr", 6))
            symbols = list(self._baseline_by_symbol.keys())

        for symbol in symbols:
            if not self._is_active(run_id) or self._eval_stop_event.is_set():
                return processed_symbols, new_hits
            with self._lock:
                base = self._baseline_by_symbol.get(symbol)
                tick = self._last_tick_by_symbol.get(symbol)
                baseline_vtt = self._vtt_baseline.get(symbol)
                if not base or not tick or baseline_vtt is None:
                    continue
                vtt_raw = tick.get("vol_traded_today")
                try:
                    vtt_f = float(vtt_raw) if vtt_raw is not None else None
                except (TypeError, ValueError):
                    vtt_f = None
                if vtt_f is None:
                    continue
                processed_symbols.append(symbol)
                diff = max(vtt_f - float(baseline_vtt), 0.0)
                try:
                    ltp = float(tick.get("ltp"))
                except (TypeError, ValueError):
                    continue
                sma_value1 = float(base.get("sma_value1") or 0.0)
                sma_value2 = float(base.get("sma_value2") or 0.0)
                base_close = float(base.get("close") or 0.0)
                raw_ts = tick.get("exch_feed_time") or tick.get("last_traded_time")
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

            value_rupees = diff * ltp
            condition1 = diff > (rule1_mult * sma_value1) and value_rupees > (rule1_value_cr * 1e7)
            condition2 = diff > (rule2_mult * sma_value2) and value_rupees > (rule2_value_cr * 1e7)
            condition_any = condition1 or condition2

            with self._lock:
                if not self._is_active(run_id):
                    return processed_symbols, new_hits
                prev = self._last_qual_true.get(symbol, False)
                if condition_any:
                    if not prev:
                        self._symbol_today_hits[symbol] = self._symbol_today_hits.get(symbol, 0) + 1
                        hit_n = self._symbol_today_hits[symbol]
                        change_pct = (
                            ((ltp - base_close) / base_close * 100.0) if base_close > 0 else 0.0
                        )
                        relative_vol1 = (diff / sma_value1) if sma_value1 > 0 else 0.0
                        relative_vol2 = (diff / sma_value2) if sma_value2 > 0 else 0.0
                        if condition1 and condition2:
                            relative_vol = max(relative_vol1, relative_vol2)
                        elif condition1:
                            relative_vol = relative_vol1
                        else:
                            relative_vol = relative_vol2
                        if condition1 and condition2:
                            condition_tag = "C1+C2"
                        elif condition1:
                            condition_tag = "C1"
                        else:
                            condition_tag = "C2"
                        row = {
                            "time": readable_ts,
                            "symbol": symbol,
                            "ltp": round(ltp, 2),
                            "change_pct": f"{change_pct:+.2f}%",
                            "metric_value": round(diff, 2),
                            "metric_source": "vol_traded_today_delta",
                            "relative_vol": round(relative_vol, 2),
                            "relative_vol_rule1": round(relative_vol1, 2),
                            "relative_vol_rule2": round(relative_vol2, 2),
                            "trade_value_cr": round(value_rupees / 1e7, 3),
                            "condition": condition_tag,
                            "hits": hit_n,
                            "symbol_repeat": hit_n > 1,
                        }
                        self._shortlist_events.append(row)
                        if len(self._shortlist_events) > SHORTLIST_MAX_EVENTS:
                            self._shortlist_events = self._shortlist_events[-SHORTLIST_MAX_EVENTS:]
                        self._persist_shortlist_state_nolock()
                        try:
                            from telegram import send_shortlist_alert

                            send_shortlist_alert(dict(row))
                        except Exception as exc:  # noqa: BLE001
                            print(f"[telegram] notify error: {exc}")
                        self._push_event(
                            f"SHORTLISTED {symbol} via {condition_tag} "
                            f"(vtt_diff={diff:.0f}, rel_vol={relative_vol:.2f}x, "
                            f"trade_value={value_rupees/1e7:.2f} Cr, day_hit={hit_n})"
                        )
                        new_hits += 1
                    self._last_qual_true[symbol] = True
                else:
                    self._last_qual_true[symbol] = False
        return processed_symbols, new_hits

    def _start_websocket(self, access_token: str, symbols: list[str], run_id: int) -> None:
        global data_socket
        self._close_all_websockets_before_subscribe()
        WS_STATE_PATH.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
        latency_stats = {
            "window_start": time.perf_counter(),
            "ticks": 0,
            "recv_sum_ms": 0.0,
            "recv_count": 0,
            "proc_sum_ms": 0.0,
        }

        def on_message(message: dict[str, Any]) -> None:
            if not self._is_active(run_id):
                return
            if not isinstance(message, dict):
                return
            self._ws_last_raw_message = dict(message)
            self._ws_last_raw_received_at = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
            if DEBUG_PRINT_WS_RAW_MESSAGES:
                now_sec = int(time.time())
                if now_sec != self._ws_raw_log_window_sec:
                    self._ws_raw_log_window_sec = now_sec
                    self._ws_raw_log_count = 0
                if self._ws_raw_log_count < WS_RAW_MAX_PRINTS_PER_SEC:
                    try:
                        print(f"[WS RAW] {json.dumps(message, ensure_ascii=False)}")
                    except Exception:
                        print(f"[WS RAW] {message}")
                    self._ws_raw_log_count += 1
                elif self._ws_raw_log_count == WS_RAW_MAX_PRINTS_PER_SEC:
                    print(
                        f"[WS RAW] throttled: showing first {WS_RAW_MAX_PRINTS_PER_SEC} "
                        "messages/sec only"
                    )
                    self._ws_raw_log_count += 1
            t0 = time.perf_counter()
            self._ws_msg_count += 1
            self._last_ws_msg_monotonic = time.monotonic()
            symbol = message.get("symbol") or message.get("Symbol")
            raw_ts = message.get("exch_feed_time") or message.get("last_traded_time")
            if symbol and raw_ts is not None:
                try:
                    self._ws_last_symbol_feed_epoch = float(raw_ts)
                except (TypeError, ValueError):
                    pass
            if symbol:
                self._ws_symbol_msg_count += 1
                kind = "symbol_tick"
                info = "ok"
            else:
                self._ws_nonsymbol_msg_count += 1
                kind = "control_or_ack"
                keys_preview = ",".join(list(message.keys())[:6]) if isinstance(message, dict) else "-"
                info = f"keys={keys_preview}"
            self._ws_last_message_summary = {
                "symbol": symbol,
                "ltp": message.get("ltp"),
                "vol_traded_today": message.get("vol_traded_today"),
                "exch_time": self._to_ist_second_str(raw_ts),
                "kind": kind,
                "info": info,
            }
            self._apply_scanner_rules(message)
            t1 = time.perf_counter()

            if LOG_WS_TICK_LATENCY:
                recv_latency_ms: str | float = "--"
                try:
                    if raw_ts is not None:
                        recv_latency_ms = round((time.time() - float(raw_ts)) * 1000.0, 2)
                except Exception:  # noqa: BLE001
                    recv_latency_ms = "--"

                processing_ms = round((t1 - t0) * 1000.0, 3)
                latency_stats["ticks"] += 1
                latency_stats["proc_sum_ms"] += float(processing_ms)
                if isinstance(recv_latency_ms, (int, float)):
                    latency_stats["recv_sum_ms"] += float(recv_latency_ms)
                    latency_stats["recv_count"] += 1

                now = time.perf_counter()
                elapsed = now - float(latency_stats["window_start"])
                if elapsed >= WS_LATENCY_SUMMARY_INTERVAL_SECS:
                    recv_count = int(latency_stats["recv_count"])
                    ticks = int(latency_stats["ticks"])
                    proc_avg_ms = (
                        latency_stats["proc_sum_ms"] / ticks if ticks > 0 else 0.0
                    )
                    recv_avg_ms = (
                        latency_stats["recv_sum_ms"] / recv_count if recv_count > 0 else 0.0
                    )
                    print(
                        "[WS LATENCY SUMMARY] "
                        f"ticks={ticks} "
                        f"recv_avg_ms={recv_avg_ms:.2f} "
                        f"processing_avg_ms={proc_avg_ms:.4f}"
                    )
                    latency_stats["window_start"] = now
                    latency_stats["ticks"] = 0
                    latency_stats["recv_sum_ms"] = 0.0
                    latency_stats["recv_count"] = 0
                    latency_stats["proc_sum_ms"] = 0.0

        def on_error(message: Any) -> None:
            if not self._is_active(run_id):
                return
            try:
                self._push_event(f"Websocket on_error payload: {json.dumps(message, ensure_ascii=False)[:500]}")
            except Exception:
                self._push_event(f"Websocket on_error payload: {str(message)[:500]}")
            text = str(message or "")
            lower = text.lower()
            # Treat transport hiccups as transient so SDK reconnect can recover.
            if (
                "attempting reconnect" in lower
                or "connection to remote host was lost" in lower
                or "timed out" in lower
            ):
                self._set_message(f"Websocket transient issue: {text}", run_id)
                return
            self._set_error(f"Websocket error: {text}")

        def on_close(message: Any) -> None:
            if not self._stop_event.is_set() and self._is_active(run_id):
                self._set_message(f"Websocket closed: {message}", run_id)

        def on_open() -> None:
            if not self._is_active(run_id):
                return
            self._eval_stop_event.set()
            if self._eval_thread is not None and self._eval_thread.is_alive():
                self._eval_thread.join(timeout=2.5)
            self._eval_stop_event.clear()
            self._eval_run_id = run_id
            self._startup_minute_key = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M")
            self._scan_ready_from_next_minute = True
            self._set_message(
                f"Websocket live. Scanner active immediately (started at {self._startup_minute_key}).",
                run_id,
            )
            self._subscribed_symbols = dedupe_symbols_preserve_order(symbols)
            self._last_ws_msg_monotonic = time.monotonic()
            self._last_ws_health_log_monotonic = 0.0
            self._ws_last_symbol_feed_epoch = None
            # Single subscribe call; SDK chunks internally and this reduces token API pressure.
            self._ws_client.subscribe(
                symbols=self._subscribed_symbols, data_type=WS_SYMBOL_DATA_TYPE
            )
            self._push_event(
                f"Single subscribe call sent for {len(self._subscribed_symbols)} symbols "
                f"(data_type={WS_SYMBOL_DATA_TYPE})."
            )
            if DEBUG_PRINT_SUBSCRIBED_SYMBOLS:
                total = len(self._subscribed_symbols)
                preview_n = min(WS_SUBSCRIBE_SYMBOLS_PREVIEW_COUNT, total)
                preview = ", ".join(self._subscribed_symbols[:preview_n])
                suffix = "" if total <= preview_n else f", ... (+{total - preview_n} more)"
                print(f"[WS SUBSCRIBE LIST] total={total} [{preview}{suffix}]")
            mapped_tokens = len(getattr(self._ws_client, "symbol_token", {}) or {})
            self._push_event(
                f"Websocket subscribe status: requested={len(self._subscribed_symbols)}, "
                f"mapped_tokens={mapped_tokens}"
            )
            self._eval_thread = threading.Thread(
                target=self._evaluation_loop,
                args=(run_id,),
                name="scanner-eval",
                daemon=True,
            )
            self._eval_thread.start()
            try:
                self._ws_client.keep_running()
            except Exception:
                pass

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
        # SDK connect() sleeps then runs on_connect (subscribe). Wait until internal ws exists.
        ws_ready_deadline = time.monotonic() + 45.0
        while time.monotonic() < ws_ready_deadline and self._is_active(run_id):
            try:
                if self._ws_client.is_connected():
                    break
            except Exception:
                pass
            time.sleep(0.15)
        else:
            if self._is_active(run_id):
                self._push_event(
                    "Websocket connect finished but is_connected() is still false — "
                    "check fyersDataSocket.log; market may be closed (no tape)."
                )

    def _apply_scanner_rules(self, message: dict[str, Any]) -> None:
        """Ingest websocket tick: update VTT baseline per IST minute and cache last tick."""
        symbol = str(message.get("symbol") or message.get("Symbol") or "").strip()
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

        vtt_f: float | None = None
        try:
            if message.get("vol_traded_today") is not None:
                vtt_f = float(message.get("vol_traded_today"))
        except (TypeError, ValueError):
            vtt_f = None

        raw_ltq = message.get("last_traded_qty")
        if raw_ltq is None:
            raw_ltq = message.get("ltq")
        parsed_ltq: float | None = None
        try:
            if raw_ltq is not None:
                parsed_ltq = max(float(raw_ltq), 0.0)
        except (TypeError, ValueError):
            parsed_ltq = None

        prev_minute = self._vtt_symbol_minute.get(symbol)
        if prev_minute != minute_key:
            self._vtt_symbol_minute[symbol] = minute_key
            if vtt_f is not None:
                self._vtt_baseline[symbol] = vtt_f
        elif vtt_f is not None and symbol not in self._vtt_baseline:
            self._vtt_baseline[symbol] = vtt_f

        if vtt_f is not None and symbol in self._vtt_baseline and vtt_f < self._vtt_baseline[symbol]:
            self._vtt_baseline[symbol] = vtt_f

        self._last_tick_by_symbol[symbol] = {
            "last_traded_qty": parsed_ltq,
            "ltq": message.get("ltq"),
            "ltp": message.get("ltp"),
            "upper_circuit": (
                message.get("upper_circuit")
                if message.get("upper_circuit") is not None
                else (
                    message.get("upper_ckt")
                    if message.get("upper_ckt") is not None
                    else (message.get("uc") if message.get("uc") is not None else message.get("uc_price"))
                )
            ),
            "lower_circuit": (
                message.get("lower_circuit")
                if message.get("lower_circuit") is not None
                else (
                    message.get("lower_ckt")
                    if message.get("lower_ckt") is not None
                    else (message.get("lc") if message.get("lc") is not None else message.get("lc_price"))
                )
            ),
            "vol_traded_today": message.get("vol_traded_today"),
            "bid_price": message.get("bid_price"),
            "ask_price": message.get("ask_price"),
            "high_price": message.get("high_price"),
            "exch_feed_time": message.get("exch_feed_time"),
            "last_traded_time": message.get("last_traded_time"),
            "exch_time": self._to_ist_second_str(raw_ts),
            "_u": self._ws_update_seq + 1,
        }
        self._ws_update_seq += 1

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
            dt = datetime.fromtimestamp(float(epoch_ts), tz=IST_ZONE)
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except Exception:  # noqa: BLE001
            return "--"

    def _roll_minute_if_needed(self, minute_key: str) -> None:
        if self._current_minute_key is None:
            self._current_minute_key = minute_key
            self._vtt_minute_anchor_wall_ist = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
            return
        if minute_key == self._current_minute_key:
            return
        if minute_key < self._current_minute_key:
            return

        self._current_minute_key = minute_key
        self._vtt_minute_anchor_wall_ist = datetime.now(IST_ZONE).strftime("%Y-%m-%d %H:%M:%S")
        if not self._scan_ready_from_next_minute:
            self._scan_ready_from_next_minute = True
            self._push_event(f"Minute boundary reached at {minute_key}. Live scanner active.")
        else:
            self._push_event(f"New minute {minute_key}.")


ENGINE = ScannerEngine()
