from __future__ import annotations

from pathlib import Path
import threading
import webbrowser

import pandas as pd
from flask import Flask, jsonify, redirect, render_template, request, url_for

from main import OUTPUT_CSV
from scanner_engine import BOOTSTRAP_SYMBOL_LIMIT, ENGINE, RATE_LIMIT_COOLDOWN_SECS

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / OUTPUT_CSV

DEFAULT_SETTINGS = {
    "rule1_volume_multiplier": 30,
    "rule1_sma_period": 1125,
    "rule1_value_cr": 4,
    "rule2_volume_multiplier": 10,
    "rule2_value_cr": 6,
    "metric_source": "vol_traded_today_delta",
    "watchlist": [],
    "is_started": False,
}

RUNTIME_SETTINGS = DEFAULT_SETTINGS.copy()
# Fresh start mode: do not auto-start daily scheduler on import.
# You can still start scanner from UI, and scheduler can be enabled explicitly later if needed.

def _universe_symbol_count() -> int:
    if not CSV_PATH.exists():
        return 0
    try:
        df = pd.read_csv(CSV_PATH)
        col = "symbol_name" if "symbol_name" in df.columns else df.columns[0]
        return int(df[col].astype(str).str.strip().ne("").sum())
    except Exception:  # noqa: BLE001
        return 0


def parse_int(field_name: str, fallback: int) -> int:
    raw_value = request.form.get(field_name, "").strip()
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return fallback


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        action = request.form.get("action", "save_settings")
        if action == "stop_scanner":
            ENGINE.stop()
            RUNTIME_SETTINGS["is_started"] = False
            return redirect(url_for("home"))

        if action == "save_watchlist":
            selected = [s.strip() for s in request.form.getlist("watch_symbols") if s.strip()]
            RUNTIME_SETTINGS["watchlist"] = sorted(set(selected))
            ENGINE.update_scan_config({"watchlist": RUNTIME_SETTINGS["watchlist"]})
            return redirect(url_for("home"))

        if action == "add_watch_symbol":
            symbol_to_add = (request.form.get("symbol_to_add") or "").strip()
            if symbol_to_add:
                watch = set(RUNTIME_SETTINGS["watchlist"])
                watch.add(symbol_to_add)
                RUNTIME_SETTINGS["watchlist"] = sorted(watch)
                ENGINE.update_scan_config({"watchlist": RUNTIME_SETTINGS["watchlist"]})
            return redirect(url_for("home"))

        if action == "remove_watch_symbol":
            symbol_to_remove = (request.form.get("symbol_to_remove") or "").strip()
            RUNTIME_SETTINGS["watchlist"] = [
                s for s in RUNTIME_SETTINGS["watchlist"] if s != symbol_to_remove
            ]
            ENGINE.update_scan_config({"watchlist": RUNTIME_SETTINGS["watchlist"]})
            return redirect(url_for("home"))

        if action == "toggle_start":
            if RUNTIME_SETTINGS["is_started"]:
                ENGINE.stop()
                RUNTIME_SETTINGS["is_started"] = False
            else:
                ENGINE.start(
                    sma_period=RUNTIME_SETTINGS["rule1_sma_period"],
                    scan_config={
                        "rule1_volume_multiplier": RUNTIME_SETTINGS["rule1_volume_multiplier"],
                        "rule1_value_cr": RUNTIME_SETTINGS["rule1_value_cr"],
                        "rule2_volume_multiplier": RUNTIME_SETTINGS["rule2_volume_multiplier"],
                        "rule2_value_cr": RUNTIME_SETTINGS["rule2_value_cr"],
                        "metric_source": RUNTIME_SETTINGS["metric_source"],
                        "watchlist": RUNTIME_SETTINGS["watchlist"],
                    },
                )
                RUNTIME_SETTINGS["is_started"] = True
            return redirect(url_for("home"))

        if action == "start_scanner":
            if not RUNTIME_SETTINGS["is_started"]:
                ENGINE.start(
                    sma_period=RUNTIME_SETTINGS["rule1_sma_period"],
                    scan_config={
                        "rule1_volume_multiplier": RUNTIME_SETTINGS["rule1_volume_multiplier"],
                        "rule1_value_cr": RUNTIME_SETTINGS["rule1_value_cr"],
                        "rule2_volume_multiplier": RUNTIME_SETTINGS["rule2_volume_multiplier"],
                        "rule2_value_cr": RUNTIME_SETTINGS["rule2_value_cr"],
                        "metric_source": RUNTIME_SETTINGS["metric_source"],
                        "watchlist": RUNTIME_SETTINGS["watchlist"],
                    },
                )
                RUNTIME_SETTINGS["is_started"] = True
            return redirect(url_for("home"))

        RUNTIME_SETTINGS["rule1_volume_multiplier"] = parse_int(
            "rule1_volume_multiplier", RUNTIME_SETTINGS["rule1_volume_multiplier"]
        )
        RUNTIME_SETTINGS["rule1_sma_period"] = parse_int(
            "rule1_sma_period", RUNTIME_SETTINGS["rule1_sma_period"]
        )
        RUNTIME_SETTINGS["rule1_value_cr"] = parse_int(
            "rule1_value_cr", RUNTIME_SETTINGS["rule1_value_cr"]
        )
        RUNTIME_SETTINGS["rule2_volume_multiplier"] = parse_int(
            "rule2_volume_multiplier", RUNTIME_SETTINGS["rule2_volume_multiplier"]
        )
        RUNTIME_SETTINGS["rule2_value_cr"] = parse_int(
            "rule2_value_cr", RUNTIME_SETTINGS["rule2_value_cr"]
        )
        RUNTIME_SETTINGS["metric_source"] = request.form.get(
            "metric_source", RUNTIME_SETTINGS["metric_source"]
        )
        if request.form.get("reset_defaults") == "1":
            RUNTIME_SETTINGS.update(DEFAULT_SETTINGS)
        ENGINE.update_scan_config(
            {
                "rule1_volume_multiplier": RUNTIME_SETTINGS["rule1_volume_multiplier"],
                "rule1_value_cr": RUNTIME_SETTINGS["rule1_value_cr"],
                "rule2_volume_multiplier": RUNTIME_SETTINGS["rule2_volume_multiplier"],
                "rule2_value_cr": RUNTIME_SETTINGS["rule2_value_cr"],
                "metric_source": RUNTIME_SETTINGS["metric_source"],
                "watchlist": RUNTIME_SETTINGS["watchlist"],
            }
        )
        return redirect(url_for("home"))

    status = ENGINE.snapshot()
    RUNTIME_SETTINGS["is_started"] = status.is_running
    all_symbols = list(status.shortlisted_rows)
    watchset = set(RUNTIME_SETTINGS["watchlist"])
    watchlist_symbols = [row for row in all_symbols if row.get("symbol") in watchset]
    universe_total = _universe_symbol_count()
    baseline_n = ENGINE.baseline_loaded_count()
    return render_template(
        "index.html",
        all_symbols=all_symbols,
        watchlist_symbols=watchlist_symbols,
        settings=RUNTIME_SETTINGS,
        watchlist=RUNTIME_SETTINGS["watchlist"],
        engine_status=status,
        engine_events=status.recent_events,
        bootstrap_symbol_limit=BOOTSTRAP_SYMBOL_LIMIT,
        rate_limit_cooldown_secs=RATE_LIMIT_COOLDOWN_SECS,
        universe_total=universe_total,
        baseline_loaded_count=baseline_n,
    )


@app.route("/sample-data", methods=["GET"])
def sample_data():
    status = ENGINE.snapshot()
    RUNTIME_SETTINGS["is_started"] = status.is_running
    minute_key, rows, sample_meta = ENGINE.sample_data_snapshot()
    ws_debug = ENGINE.websocket_debug_snapshot()
    return render_template(
        "sample_data.html",
        settings=RUNTIME_SETTINGS,
        engine_status=status,
        minute_key=minute_key,
        sample_rows=rows,
        sample_meta=sample_meta,
        ws_debug=ws_debug,
    )


@app.route("/api/dashboard-data", methods=["GET"])
def dashboard_data():
    status = ENGINE.snapshot()
    all_symbols = list(status.shortlisted_rows)
    watchset = set(RUNTIME_SETTINGS["watchlist"])
    watchlist_symbols = [row for row in all_symbols if row.get("symbol") in watchset]
    net_positions = ENGINE.net_positions_snapshot()
    managed_trades = ENGINE.managed_trades_snapshot()
    return jsonify(
        {
            "is_started": status.is_running,
            "last_message": status.last_message,
            "last_error": status.last_error,
            "events": status.recent_events,
            "watchlist": RUNTIME_SETTINGS["watchlist"],
            "watchlist_symbols": watchlist_symbols,
            "all_symbols": all_symbols,
            "net_positions": net_positions,
            "managed_trades": managed_trades,
            "universe_total": _universe_symbol_count(),
            "baseline_loaded_count": ENGINE.baseline_loaded_count(),
        }
    )


@app.route("/api/sample-data", methods=["GET"])
def sample_data_api():
    since_seq_raw = request.args.get("since_seq", "0").strip()
    since_minute = request.args.get("since_minute", "").strip()
    try:
        since_seq = max(int(since_seq_raw), 0)
    except ValueError:
        since_seq = 0
    minute_key, seq, full, rows, sample_meta = ENGINE.sample_data_delta(
        since_seq=since_seq, since_minute=since_minute
    )
    return jsonify(
        {
            "minute_key": minute_key,
            "seq": seq,
            "full": full,
            "rows": rows,
            "sample_meta": sample_meta,
            "ws_debug": ENGINE.websocket_debug_snapshot(),
        }
    )


@app.route("/api/place-order", methods=["POST"])
def place_order_api():
    payload = request.get_json(silent=True) or {}
    symbol = str(payload.get("symbol") or "").strip()
    side = str(payload.get("side") or "").strip().upper()
    try:
        quantity = max(int(payload.get("quantity", 1)), 1)
    except (TypeError, ValueError):
        quantity = 1
    try:
        target_pct = max(float(payload.get("target_pct", 0)), 0.0)
    except (TypeError, ValueError):
        target_pct = 0.0
    try:
        stop_loss_pct = max(float(payload.get("stop_loss_pct", 0)), 0.0)
    except (TypeError, ValueError):
        stop_loss_pct = 0.0

    if not symbol:
        return jsonify({"ok": False, "error": "symbol is required"}), 400
    if side not in {"BUY", "SELL"}:
        return jsonify({"ok": False, "error": "side must be BUY or SELL"}), 400
    try:
        placed = ENGINE.place_limit_order_from_latest_tick(
            symbol=symbol,
            side=side,
            quantity=quantity,
            target_pct=target_pct,
            stop_loss_pct=stop_loss_pct,
        )
        return jsonify({"ok": True, "order": placed})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/exit-position", methods=["POST"])
def exit_position_api():
    payload = request.get_json(silent=True) or {}
    symbol = str(payload.get("symbol") or "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "symbol is required"}), 400
    try:
        out = ENGINE.exit_position(symbol=symbol)
        return jsonify({"ok": True, "result": out})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 400


if __name__ == "__main__":
    threading.Timer(1.0, lambda: webbrowser.open("http://127.0.0.1:5001")).start()
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False, threaded=True)
