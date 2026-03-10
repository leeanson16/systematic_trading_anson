"""
Download market data and simple metrics via yfinance and Fed (FRED).

Outputs:
  - data/custom/GSPC.csv, SPY_yfinance_unadj.csv, SPY_yfinance_adj.csv
  - data/custom/FUNDING_COST.csv (FRED DGS3MO / 100, same source as FUNDING_COST in multiple_prices)
  - data/futures/multiple_prices_csv/{CODE}.csv (unadj) and adjusted_prices_csv/{CODE}.csv (adj) for all symbols in instruments CSV (code = symbol + _yfinance).

Instruments list: data/custom/S&P_500_component_stocks.csv, column Symbol. Add _yfinance suffix for instrument code.
If an output file already exists, that file is not re-fetched or overwritten.
"""

import os
import time

import pandas as pd
import yfinance as yf

INSTRUMENTS_CSV = "data/custom/S&P_500_component_stocks.csv"
DELAY_BETWEEN_TICKERS_SEC = 0.25
INSTRUMENTS_COLUMN = "Symbol"
YFINANCE_INSTRUMENT_SUFFIX = "_yfinance"


def _get_tickers_from_csv():
    """Read symbol column from instruments CSV; return list of ticker strings (e.g. ['MMM', 'AOS', ...])."""
    path = os.path.join(_repo_root(), INSTRUMENTS_CSV.replace("/", os.sep))
    if not os.path.isfile(path):
        return []
    df = pd.read_csv(path)
    if INSTRUMENTS_COLUMN not in df.columns:
        return []
    return df[INSTRUMENTS_COLUMN].astype(str).str.strip().dropna().unique().tolist()


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _out_dir() -> str:
    path = os.path.join(_repo_root(), "data", "custom")
    os.makedirs(path, exist_ok=True)
    return path


def _multiple_prices_dir() -> str:
    path = os.path.join(_repo_root(), "data", "futures", "multiple_prices_csv")
    os.makedirs(path, exist_ok=True)
    return path


def _adjusted_prices_dir() -> str:
    path = os.path.join(_repo_root(), "data", "futures", "adjusted_prices_csv")
    os.makedirs(path, exist_ok=True)
    return path


def _download_price_series(symbol: str, filename: str, out_dir: str = None) -> None:
    if out_dir is None:
        out_dir = _out_dir()
    out_path = os.path.join(out_dir, filename)
    if os.path.isfile(out_path):
        print("Exists, skipping: %s" % out_path)
        return
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="max", auto_adjust=True)
    if df.empty:
        raise RuntimeError("No data returned for %s" % symbol)
    df = df[["Close"]].rename(columns={"Close": "price"})
    df.index = df.index.tz_localize(None)
    df.index = df.index.normalize() + pd.Timedelta(hours=23)
    df.index.name = "DATETIME"
    df.to_csv(out_path)
    print("Saved %d rows for %s to %s" % (len(df), symbol, out_path))
    print("  Range: %s to %s" % (df.index[0], df.index[-1]))


def _yfinance_ticker_symbol(symbol: str) -> str:
    """Convert symbol for yfinance (e.g. BRK.B -> BRK-B)."""
    return symbol.replace(".", "-")


def _get_ticker_price_and_dividend_yield(symbol: str, auto_adjust: bool = False):
    """Return (price_df, div_yield_series). Div yield truncated to drop first 365 days."""
    ticker = yf.Ticker(_yfinance_ticker_symbol(symbol))
    px = ticker.history(period="max", auto_adjust=auto_adjust)
    if px.empty:
        return None, pd.Series(dtype=float)
    price = px[["Close"]].rename(columns={"Close": "price"})
    price.index = price.index.tz_localize(None)
    price.index = price.index.normalize() + pd.Timedelta(hours=23)
    price.index.name = "DATETIME"

    divs = ticker.dividends
    if divs.empty:
        yield_1y = pd.Series(index=price.index, dtype=float)
        yield_1y[:] = float("nan")
    else:
        divs = divs.copy()
        divs.index = divs.index.tz_localize(None)
        divs.index = divs.index.normalize()
        start = price.index.normalize().min() - pd.Timedelta(days=365)
        end = price.index.normalize().max()
        daily_index = pd.date_range(start, end, freq="B")
        div_aligned = divs.reindex(daily_index, fill_value=0.0)
        trailing_div = div_aligned.rolling("365D", min_periods=1).sum()
        price_dates = price.index.normalize()
        trailing_div = trailing_div.reindex(price_dates).ffill()
        yield_1y = trailing_div / price["price"].values
        yield_1y.index = price.index
        yield_1y.name = "dividend_yield_1y"
    if len(yield_1y) > 365:
        yield_1y = yield_1y.iloc[365:]
    return price, yield_1y


def _download_treasury_3mo() -> pd.DataFrame:
    """Fetch Daily Treasury Par Yield Curve 3-month rate (FRED DGS3MO). Return df."""
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS3MO"
    df = pd.read_csv(url)
    date_col = "observation_date" if "observation_date" in df.columns else "DATE"
    df[date_col] = pd.to_datetime(df[date_col])
    return df


def _write_funding_cost_csv(rate_df: pd.DataFrame, rate_col: str = "DGS3MO") -> None:
    """Write FUNDING_COST (rate/100) to data/custom/FUNDING_COST.csv. Same source as used in multiple_prices."""
    date_col = "observation_date" if "observation_date" in rate_df.columns else "DATE"
    out_path = os.path.join(_out_dir(), "FUNDING_COST.csv")
    clean = rate_df.dropna(subset=[date_col, rate_col]).copy()
    clean["DATETIME"] = pd.to_datetime(clean[date_col]).dt.normalize() + pd.Timedelta(hours=23)
    clean["FUNDING_COST"] = clean[rate_col] / 100
    out = clean[["DATETIME", "FUNDING_COST"]].set_index("DATETIME").sort_index()
    out.to_csv(out_path)
    print("Saved FUNDING_COST (%d rows) to %s" % (len(out), out_path))


def _build_and_save_with_div_and_funding(
    price_df: pd.DataFrame,
    div_yield: pd.Series,
    rate_df: pd.DataFrame,
    output_path: str,
    rate_col: str = "DGS3MO",
    minimal_columns: bool = False,
) -> None:
    """Inner-join div yield and rate; merge with price; write to output_path. FUNDING_COST = rate/100. Skip if output_path exists."""
    if os.path.isfile(output_path):
        print("Exists, skipping: %s" % output_path)
        return
    if price_df is None or price_df.empty or div_yield.empty:
        print("No price or div yield, skipping %s" % output_path)
        return
    date_col = "observation_date" if "observation_date" in rate_df.columns else "DATE"
    rate_clean = rate_df.dropna(subset=[date_col, rate_col]).copy()
    rate_clean["date"] = pd.to_datetime(rate_clean[date_col]).dt.normalize()

    div_df = div_yield.to_frame("DIVIDEND_YIELD")
    div_df["date"] = div_df.index.normalize() if hasattr(div_df.index, "normalize") else div_df.index

    rate_daily = rate_clean.set_index("date")[[rate_col]]
    day_range = pd.date_range(div_df["date"].min(), div_df["date"].max(), freq="B")
    rate_daily = rate_daily.reindex(day_range).ffill().bfill()
    rate_daily = rate_daily.reset_index().rename(columns={"index": "date"})
    merged = div_df.merge(rate_daily, on="date", how="inner")
    merged["FUNDING_COST"] = merged[rate_col] / 100
    merged = merged.drop(columns=[rate_col])
    if merged.empty:
        print("No overlap after inner-join, skipping %s" % output_path)
        return

    price_by_date = price_df.copy()
    price_by_date.index = price_by_date.index.normalize()
    merged["price"] = merged["date"].map(price_by_date["price"])
    merged = merged.dropna(subset=["price"])
    if merged.empty:
        print("No price on joined dates, skipping %s" % output_path)
        return

    merged["DATETIME"] = merged["date"] + pd.Timedelta(hours=23)
    if minimal_columns:
        out = merged.set_index("DATETIME")[["price"]].copy()
        out.columns = ["price"]
    else:
        price_vals = merged["price"].values
        merged["CARRY"] = price_vals
        merged["CARRY_CONTRACT"] = ""
        merged["PRICE"] = price_vals
        merged["PRICE_CONTRACT"] = ""
        merged["FORWARD"] = price_vals
        merged["FORWARD_CONTRACT"] = ""
        out = merged.set_index("DATETIME")[
            ["CARRY", "CARRY_CONTRACT", "PRICE", "PRICE_CONTRACT", "FORWARD", "FORWARD_CONTRACT", "DIVIDEND_YIELD", "FUNDING_COST"]
        ]
    out.to_csv(output_path)
    print("Saved %d rows to %s" % (len(out), output_path))
    print("  Range: %s to %s" % (out.index[0], out.index[-1]))


def main() -> None:
    _download_price_series("^GSPC", "GSPC.csv")

    treasury_3mo_df = _download_treasury_3mo()
    _write_funding_cost_csv(treasury_3mo_df)

    # SPY: data/custom, with _unadj / _adj suffix
    spy_unadj_path = os.path.join(_out_dir(), "SPY_yfinance_unadj.csv")
    spy_adj_path = os.path.join(_out_dir(), "SPY_yfinance_adj.csv")
    if not os.path.isfile(spy_unadj_path):
        spy_price_unadj, div_yield_unadj = _get_ticker_price_and_dividend_yield("SPY", auto_adjust=False)
        _build_and_save_with_div_and_funding(
            spy_price_unadj, div_yield_unadj, treasury_3mo_df, spy_unadj_path, minimal_columns=False
        )
    if not os.path.isfile(spy_adj_path):
        spy_price_adj, div_yield_adj = _get_ticker_price_and_dividend_yield("SPY", auto_adjust=True)
        _build_and_save_with_div_and_funding(
            spy_price_adj, div_yield_adj, treasury_3mo_df, spy_adj_path, minimal_columns=True
        )

    # All symbols from instruments CSV: multiple_prices_csv (unadj), adjusted_prices_csv (adj), code = symbol + _yfinance
    yfinance_tickers = _get_tickers_from_csv()
    if not yfinance_tickers:
        print("No tickers from %s (column %s), skipping." % (INSTRUMENTS_CSV, INSTRUMENTS_COLUMN))
    multi_dir = _multiple_prices_dir()
    adj_dir = _adjusted_prices_dir()
    for symbol in yfinance_tickers:
        code = symbol + YFINANCE_INSTRUMENT_SUFFIX
        unadj_path = os.path.join(multi_dir, "%s.csv" % code)
        adj_path = os.path.join(adj_dir, "%s.csv" % code)
        if not os.path.isfile(unadj_path):
            price_unadj, div_unadj = _get_ticker_price_and_dividend_yield(symbol, auto_adjust=False)
            _build_and_save_with_div_and_funding(
                price_unadj, div_unadj, treasury_3mo_df, unadj_path, minimal_columns=False
            )
        if not os.path.isfile(adj_path):
            price_adj, div_adj = _get_ticker_price_and_dividend_yield(symbol, auto_adjust=True)
            _build_and_save_with_div_and_funding(
                price_adj, div_adj, treasury_3mo_df, adj_path, minimal_columns=True
            )
        time.sleep(DELAY_BETWEEN_TICKERS_SEC)


if __name__ == "__main__":
    main()

