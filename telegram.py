"""
Telegram alerts when RiverFlowScanner shortlists a new symbol (scanner hit).

Configure with environment variables (see TELEGRAM INTEGRATION.md):
  TELEGRAM_BOT_TOKEN  - from @BotFather
  TELEGRAM_CHAT_ID    - group or channel id (e.g. -1001234567890)

Optional:
  TELEGRAM_ENABLED    - set to "0" or "false" to disable sending (default: enabled if token is set)
"""

from __future__ import annotations

import os
import urllib.parse
from typing import Any

import requests

TELEGRAM_API = "https://api.telegram.org"


def _env_bool(name: str, default: bool = True) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def is_telegram_configured() -> bool:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    return bool(token and chat)


def fyers_chart_url(symbol: str) -> str:
    """FYERS popout chart; symbol query uses lowercase exchange prefix (e.g. nse:TRITURBINE-EQ)."""
    sym = str(symbol or "").strip()
    if ":" in sym:
        exch, rest = sym.split(":", 1)
        sym = f"{exch.lower()}:{rest}"
    return (
        "https://trade.fyers.in/popout/index.html?"
        + urllib.parse.urlencode({"symbol": sym, "resolution": "1", "theme": "dark"})
    )


def tradingview_chart_url(symbol: str) -> str:
    """TradingView India chart; symbol is typically NSE:TICKER-EQ."""
    sym = str(symbol or "").strip()
    return "https://in.tradingview.com/chart/?" + urllib.parse.urlencode({"symbol": sym})


def format_shortlist_message(row: dict[str, Any]) -> str:
    """Build plain-text message with stats + chart links."""
    symbol = row.get("symbol") or "-"
    lines = [
        "<b>RiverFlowScanner — New shortlist</b>",
        "",
        f"<b>Symbol</b>: <code>{_escape_html(symbol)}</code>",
        f"<b>Time</b>: {_escape_html(str(row.get('time') or '-'))}",
        f"<b>LTP</b>: {_escape_html(str(row.get('ltp') or '-'))}",
        f"<b>% Chg</b>: {_escape_html(str(row.get('change_pct') or '-'))}",
        f"<b>Rel Vol</b>: {_escape_html(str(row.get('relative_vol') or '-'))}x",
        f"<b>Value (Cr)</b>: {_escape_html(str(row.get('trade_value_cr') or '-'))}",
        f"<b>Rule</b>: {_escape_html(str(row.get('condition') or '-'))}",
        f"<b>Hit #</b>: {_escape_html(str(row.get('hits') or '-'))}",
        f"<b>VTT Δ</b>: {_escape_html(str(row.get('metric_value') or '-'))}",
        "",
        "<b>Charts</b>",
        f'• <a href="{_escape_attr(fyers_chart_url(symbol))}">FYERS</a>',
        f'• <a href="{_escape_attr(tradingview_chart_url(symbol))}">TradingView</a>',
    ]
    return "\n".join(lines)


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _escape_attr(text: str) -> str:
    return _escape_html(text).replace('"', "&quot;")


def send_shortlist_alert(row: dict[str, Any]) -> bool:
    """
    Send one Telegram message for a new scanner shortlist row.
    Returns True if sent (or skipped as disabled), False on hard failure after logging.
    """
    if not _env_bool("TELEGRAM_ENABLED", default=True):
        return True
    if not is_telegram_configured():
        return True

    token = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    chat_id = os.environ["TELEGRAM_CHAT_ID"].strip()
    url = f"{TELEGRAM_API}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": format_shortlist_message(row),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        if not resp.ok or not data.get("ok"):
            err = data.get("description") or resp.text[:500]
            print(f"[telegram] sendMessage failed: {resp.status_code} {err}")
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"[telegram] sendMessage error: {exc}")
        return False
