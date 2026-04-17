from __future__ import annotations

from io import StringIO
from pathlib import Path

import certifi
import pandas as pd
import requests



URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
OUTPUT_CSV = "NSE_EQ_symbols.csv"


def _symbols_from_csv_file(path: str | Path) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        df = pd.read_csv(p)
    except Exception:  # noqa: BLE001
        return []
    if "symbol_name" not in df.columns:
        return []
    out = [str(x).strip() for x in df["symbol_name"].tolist() if str(x).strip()]
    seen: set[str] = set()
    unique: list[str] = []
    for sym in out:
        if sym in seen:
            continue
        seen.add(sym)
        unique.append(sym)
    return unique

def get_nse_eq_symbols():
    # Pull source through requests with certifi CA bundle for stable SSL verification.
    resp = requests.get(URL, timeout=30, verify=certifi.where())
    resp.raise_for_status()
    # Source has no header row, so use positional columns.
    df = pd.read_csv(StringIO(resp.text), header=None)

    # Column 9 contains Fyers symbol values like NSE:20MICRONS-EQ
    symbol_col = df[9].astype(str)
    description_col = df[1].astype(str)

    # Keep only EQ stocks and optionally skip SME stocks.
    eq_mask = symbol_col.str.endswith("-EQ", na=False)
    non_sme_mask = ~description_col.str.contains("SME", case=False, na=False)

    return symbol_col[eq_mask & non_sme_mask].tolist()


def write_symbols_csv() -> list[str]:
    try:
        symbols = get_nse_eq_symbols()
        symbols_df = pd.DataFrame(symbols, columns=["symbol_name"])
        symbols_df.to_csv(OUTPUT_CSV, index=False)
        return symbols
    except Exception as exc:  # noqa: BLE001
        # Keep scanner usable if network/cert is broken by falling back to last local symbol file.
        cached = _symbols_from_csv_file(OUTPUT_CSV)
        if cached:
            print(f"[symbols] warning: live download failed ({exc}); using local {OUTPUT_CSV}")
            return cached
        raise RuntimeError(
            f"Unable to download symbols and no local {OUTPUT_CSV} exists. Root error: {exc}"
        ) from exc


if __name__ == "__main__":
    write_symbols_csv()



