from __future__ import annotations

import argparse
import csv
import signal
import time
from pathlib import Path
from threading import Event

from fyers_apiv3.FyersWebsocket import data_ws

from FyresIntegration import automated_login
from main import write_symbols_csv

BASE_DIR = Path(__file__).resolve().parent
FILENAME_CREDENTIALS = BASE_DIR / "FyersCredentials.csv"

access_token: str | None = None
fyers = None
data_socket: data_ws.FyersDataSocket | None = None
shared_data: dict[str, float] = {}
VERBOSE_LTP_STREAM = False
LIQUID_TEST_SYMBOLS = [
    "NSE:RELIANCE-EQ",
    "NSE:SBIN-EQ",
    "NSE:HDFCBANK-EQ",
    "NSE:INFY-EQ",
    "NSE:TCS-EQ",
]


def load_fyers_credentials_csv() -> dict[str, str]:
    if not FILENAME_CREDENTIALS.exists():
        raise FileNotFoundError(f"Missing {FILENAME_CREDENTIALS.name} in project root.")

    with FILENAME_CREDENTIALS.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

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


def login_fyers() -> tuple[object, str]:
    creds = load_fyers_credentials_csv()
    automated_login(
        client_id=creds["client_id"],
        secret_key=creds["secret_key"],
        FY_ID=creds["FY_ID"],
        TOTP_KEY=creds["totpkey"],
        PIN=creds["PIN"],
        redirect_uri=creds["redirect_uri"],
    )
    from FyresIntegration import access_token as fy_access_token, fyers as fyers_client

    if not fy_access_token or fyers_client is None:
        raise RuntimeError("FYERS login failed: access_token not found after automated_login().")
    return fyers_client, str(fy_access_token)


def build_symbol_list(limit: int) -> list[str]:
    symbols = write_symbols_csv()
    deduped = list(dict.fromkeys(str(s).strip() for s in symbols if str(s).strip()))
    if limit > 0:
        deduped = deduped[:limit]
    return deduped


def close_fyres_websocket() -> None:
    global data_socket
    try:
        if data_socket is not None:
            print("[WS] Closing FyersDataSocket connection...")
            try:
                setattr(data_socket, "reconnect", False)
            except Exception:
                pass
            data_socket.close_connection()
            data_socket = None
        else:
            print("[WS] close_fyres_websocket called but no active socket.")
    except Exception as e:  # noqa: BLE001
        print("[WS] Error while closing FyersDataSocket:", e)


def fyres_websocket(
    symbollist: list[str], stop_event: Event, stale_after_sec: int, stale_window_sec: int
) -> str:
    global access_token, data_socket
    close_fyres_websocket()
    state: dict[str, float | None] = {
        "last_recv_monotonic": None,
        "last_exch_feed_epoch": None,
    }

    def onmessage(message: dict) -> None:
        print("Response:", message)
        if isinstance(message, dict):
            state["last_recv_monotonic"] = time.monotonic()
            raw_epoch = message.get("exch_feed_time") or message.get("last_traded_time")
            try:
                if raw_epoch is not None:
                    state["last_exch_feed_epoch"] = float(raw_epoch)
            except (TypeError, ValueError):
                pass
        if isinstance(message, dict) and "symbol" in message and "ltp" in message:
            symbol = str(message["symbol"])
            try:
                ltp = float(message["ltp"])
            except (TypeError, ValueError):
                return
            shared_data[symbol] = ltp

    def onerror(message: dict) -> None:
        print("Error:", message)

    def onclose(message: dict) -> None:
        print("Connection closed:", message)

    def onopen() -> None:
        data_type = "SymbolUpdate"
        symbols = symbollist
        print(f"Subscribing {len(symbols)} symbols...")
        fyers.subscribe(symbols=symbols, data_type=data_type)
        fyers.keep_running()

    fyers = data_ws.FyersDataSocket(
        access_token=access_token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=onopen,
        on_close=onclose,
        on_error=onerror,
        on_message=onmessage,
    )

    data_socket = fyers
    fyers.connect()
    stale_started_at: float | None = None
    while not stop_event.is_set():
        time.sleep(1.0)
        last_recv = state.get("last_recv_monotonic")
        last_epoch = state.get("last_exch_feed_epoch")
        if last_epoch is None:
            print("[WS STATUS] waiting_for_first_tick")
            continue
        lag_sec = max(0.0, time.time() - float(last_epoch))
        age_sec = (
            "--" if last_recv is None else f"{max(0.0, time.monotonic() - float(last_recv)):.1f}s"
        )
        mode = "STALE" if lag_sec > float(stale_after_sec) else "LIVE"
        print(
            f"[WS STATUS] mode={mode} lag_vs_now={lag_sec:.1f}s "
            f"last_msg_age={age_sec} threshold={stale_after_sec}s"
        )
        if mode == "STALE":
            if stale_started_at is None:
                stale_started_at = time.monotonic()
            elif (time.monotonic() - stale_started_at) >= float(stale_window_sec):
                print(
                    f"[WS STATUS] stale for {stale_window_sec}s continuously; reconnect requested."
                )
                close_fyres_websocket()
                return "stale_reconnect"
        else:
            stale_started_at = None
    close_fyres_websocket()
    return "stopped"


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple FYERS websocket smoke test.")
    parser.add_argument("--limit", type=int, default=200, help="How many symbols to subscribe (0 = all).")
    parser.add_argument(
        "--liquid-test",
        action="store_true",
        help="Use only 5 liquid symbols for websocket validation.",
    )
    parser.add_argument(
        "--stale-after-sec",
        type=int,
        default=120,
        help="Mark feed stale if exchange time lags this many seconds.",
    )
    parser.add_argument(
        "--stale-window-sec",
        type=int,
        default=20,
        help="Require stale condition for this many seconds before reconnect.",
    )
    parser.add_argument(
        "--verbose-ltp",
        action="store_true",
        help="Print [LTP] per symbol tick in addition to raw payload.",
    )
    args = parser.parse_args()

    global fyers, access_token, VERBOSE_LTP_STREAM
    VERBOSE_LTP_STREAM = bool(args.verbose_ltp)

    fyers, access_token = login_fyers()
    symbols = LIQUID_TEST_SYMBOLS if args.liquid_test else build_symbol_list(limit=args.limit)
    if not symbols:
        raise RuntimeError("No symbols available to subscribe.")

    print(f"[INIT] logged in OK; symbols={len(symbols)}")
    if args.liquid_test:
        print(f"[INIT] liquid test mode enabled: {symbols}")
    stop_event = Event()

    def _handle_signal(_signum, _frame):
        stop_event.set()
        close_fyres_websocket()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    reconnected_once = False
    while not stop_event.is_set():
        result = fyres_websocket(
            symbollist=symbols,
            stop_event=stop_event,
            stale_after_sec=int(args.stale_after_sec),
            stale_window_sec=int(args.stale_window_sec),
        )
        if result == "stale_reconnect" and not reconnected_once and not stop_event.is_set():
            reconnected_once = True
            print("[WS STATUS] performing one-time stale reconnect...")
            time.sleep(2.0)
            continue
        break

    return 0



if __name__ == "__main__":
    raise SystemExit(main())
