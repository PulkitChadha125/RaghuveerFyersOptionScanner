from __future__ import annotations

from datetime import datetime
from pathlib import Path
import threading
import webbrowser

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
    "rule2_sma_period": 1125,
    "rule2_value_cr": 8,
    "metric_source": "last_traded_qty",
    "watchlist": [],
    "is_started": False,
}

RUNTIME_SETTINGS = DEFAULT_SETTINGS.copy()
PERSISTED_SHORTLIST: dict[str, dict] = {}
ENGINE.start_daily_scheduler(sma_period=DEFAULT_SETTINGS["rule1_sma_period"])


def parse_int(field_name: str, fallback: int) -> int:
    raw_value = request.form.get(field_name, "").strip()
    try:
        return max(int(raw_value), 1)
    except ValueError:
        return fallback


def row_time_sort_key(row: dict) -> datetime:
    raw = str(row.get("time") or "").strip()
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.min


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
                # Fresh run: clear previous in-memory UI shortlist cache.
                PERSISTED_SHORTLIST.clear()
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
        RUNTIME_SETTINGS["rule2_sma_period"] = parse_int(
            "rule2_sma_period", RUNTIME_SETTINGS["rule2_sma_period"]
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
    if status.shortlisted_rows:
        for row in status.shortlisted_rows:
            symbol = str(row.get("symbol") or "").strip()
            if symbol:
                PERSISTED_SHORTLIST[symbol] = row
    all_symbols = sorted(
        PERSISTED_SHORTLIST.values(),
        key=row_time_sort_key,
        reverse=True,
    )
    watchset = set(RUNTIME_SETTINGS["watchlist"])
    watchlist_symbols = [row for row in all_symbols if row.get("symbol") in watchset]
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
    )


@app.route("/sample-data", methods=["GET"])
def sample_data():
    status = ENGINE.snapshot()
    RUNTIME_SETTINGS["is_started"] = status.is_running
    minute_key, rows = ENGINE.sample_data_snapshot()
    return render_template(
        "sample_data.html",
        settings=RUNTIME_SETTINGS,
        engine_status=status,
        minute_key=minute_key,
        sample_rows=rows,
    )


@app.route("/api/dashboard-data", methods=["GET"])
def dashboard_data():
    status = ENGINE.snapshot()
    if status.shortlisted_rows:
        for row in status.shortlisted_rows:
            symbol = str(row.get("symbol") or "").strip()
            if symbol:
                PERSISTED_SHORTLIST[symbol] = row
    all_symbols = sorted(PERSISTED_SHORTLIST.values(), key=row_time_sort_key, reverse=True)
    watchset = set(RUNTIME_SETTINGS["watchlist"])
    watchlist_symbols = [row for row in all_symbols if row.get("symbol") in watchset]
    return jsonify(
        {
            "is_started": status.is_running,
            "last_message": status.last_message,
            "last_error": status.last_error,
            "events": status.recent_events,
            "watchlist": RUNTIME_SETTINGS["watchlist"],
            "watchlist_symbols": watchlist_symbols,
            "all_symbols": all_symbols,
        }
    )


@app.route("/api/sample-data", methods=["GET"])
def sample_data_api():
    minute_key, rows = ENGINE.sample_data_snapshot()
    return jsonify({"minute_key": minute_key, "rows": rows})


if __name__ == "__main__":
    threading.Timer(1.0, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
