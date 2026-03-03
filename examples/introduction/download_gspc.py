"""
Download ^GSPC (S&P 500 index) and SPY (ETF) daily data with yfinance.
^GSPC -> data/GSPC.csv; SPY -> data/futures/adjusted_prices_csv/SPY_yfinance.csv.
"""

import os
import pandas as pd
import yfinance as yf

def _download_and_save(symbol: str, out_path: str):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="max", auto_adjust=True)
    if df.empty:
        raise RuntimeError("No data returned for %s" % symbol)
    df = df[["Close"]].rename(columns={"Close": "price"})
    df.index = df.index.tz_localize(None)
    df.index = df.index.normalize() + pd.Timedelta(hours=23)
    df.index.name = "DATETIME"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path)
    print("Saved %d rows to %s" % (len(df), out_path))
    print("  Range: %s to %s" % (df.index[0], df.index[-1]))

def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    _download_and_save("^GSPC", os.path.join(repo_root, "data", "GSPC.csv"))
    _download_and_save("SPY", os.path.join(repo_root, "data", "futures", "adjusted_prices_csv", "SPY_yfinance.csv"))

if __name__ == "__main__":
    main()
