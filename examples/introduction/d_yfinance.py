"""
Download market data and simple metrics via yfinance and Fed (FRED).

Outputs:
  - GSPC.csv        : ^GSPC (S&P 500 index) daily prices
  - SPY_yfinance.csv: SPY ETF daily prices
  - SPY_metrics.csv : 1y dividend yield for SPY
  - FEDFUNDS.csv    : Effective Fed funds rate from FRED

All files are written to: pysystemtradeanson/data/my yfinance
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _out_dir() -> str:
    path = os.path.join(_repo_root(), "data", "my yfinance")
    os.makedirs(path, exist_ok=True)
    return path


def _download_price_series(symbol: str, filename: str) -> None:
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="max", auto_adjust=True)
    if df.empty:
        raise RuntimeError("No data returned for %s" % symbol)
    df = df[["Close"]].rename(columns={"Close": "price"})
    df.index = df.index.tz_localize(None)
    df.index = df.index.normalize() + pd.Timedelta(hours=23)
    df.index.name = "DATETIME"

    out_path = os.path.join(_out_dir(), filename)
    df.to_csv(out_path)
    print("Saved %d rows for %s to %s" % (len(df), symbol, out_path))
    print("  Range: %s to %s" % (df.index[0], df.index[-1]))


def _spy_dividend_yield_1y() -> float:
    """Compute past 1y SPY cash dividends / current SPY price using yfinance."""
    spy = yf.Ticker("SPY")
    divs = spy.dividends
    if divs.empty:
        return float("nan")

    cutoff = pd.Timestamp(datetime.utcnow().date()) - pd.Timedelta(days=365)
    last_year_divs = divs[divs.index.tz_localize(None) >= cutoff].sum()

    px = spy.history(period="1d")
    if px.empty:
        return float("nan")
    current_price = float(px["Close"].iloc[-1])
    if current_price == 0:
        return float("nan")

    return float(last_year_divs / current_price)


def _save_spy_metrics() -> None:
    dy = _spy_dividend_yield_1y()
    metrics = pd.DataFrame(
        [{"metric": "dividend_yield_1y", "value": dy}],
        columns=["metric", "value"],
    )
    out_path = os.path.join(_out_dir(), "SPY_metrics.csv")
    metrics.to_csv(out_path, index=False)
    print("Saved SPY metrics to %s (dividend_yield_1y=%.6f)" % (out_path, dy))


def _download_fed_funds() -> None:
    """Fetch effective Fed funds rate from the St. Louis Fed (FRED) CSV endpoint."""
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS"
    df = pd.read_csv(url)
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"])
    out_path = os.path.join(_out_dir(), "FEDFUNDS.csv")
    df.to_csv(out_path, index=False)
    last_row = df.dropna().iloc[-1]
    print(
        "Saved FEDFUNDS to %s (last: %s = %s)"
        % (out_path, last_row.get("DATE"), last_row.get("FEDFUNDS"))
    )


def main() -> None:
    # Prices
    _download_price_series("^GSPC", "GSPC.csv")
    _download_price_series("SPY", "SPY_yfinance.csv")

    # SPY dividend yield metric
    _save_spy_metrics()

    # Fed funds from Fed site (FRED)
    _download_fed_funds()


if __name__ == "__main__":
    main()

