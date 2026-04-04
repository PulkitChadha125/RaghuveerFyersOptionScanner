import pandas as pd



URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
OUTPUT_CSV = "NSE_EQ_symbols.csv"

def get_nse_eq_symbols():
    # Source has no header row, so use positional columns.
    df = pd.read_csv(URL, header=None)

    # Column 9 contains Fyers symbol values like NSE:20MICRONS-EQ
    symbol_col = df[9].astype(str)
    description_col = df[1].astype(str)

    # Keep only EQ stocks and optionally skip SME stocks.
    eq_mask = symbol_col.str.endswith("-EQ", na=False)
    non_sme_mask = ~description_col.str.contains("SME", case=False, na=False)

    return symbol_col[eq_mask & non_sme_mask].tolist()


def write_symbols_csv() -> list[str]:
    symbols = get_nse_eq_symbols()
    symbols_df = pd.DataFrame(symbols, columns=["symbol_name"])
    symbols_df.to_csv(OUTPUT_CSV, index=False)
    return symbols


if __name__ == "__main__":
    write_symbols_csv()



