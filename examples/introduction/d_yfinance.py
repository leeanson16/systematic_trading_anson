"""
Download market data and simple metrics via yfinance and Fed (FRED).

Outputs:
  - GSPC.csv         : ^GSPC (S&P 500 index) daily prices
  - SPY_yfinance_unadj.csv : SPY unadjusted (auto_adjust=False); columns: CARRY, CARRY_CONTRACT, PRICE, PRICE_CONTRACT, FORWARD, FORWARD_CONTRACT, DIVIDEND_YIELD, FUNDING_COST
  - SPY_yfinance_adj.csv   : SPY adjusted (auto_adjust=True); DATETIME and price only

All files are written to: pysystemtradeanson/data/my yfinance
"""

import os

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


def _get_spy_price_and_dividend_yield(auto_adjust: bool = False):
    """Return (spy_price_df, div_yield_series). Div yield is truncated to drop first 365 days."""
    spy = yf.Ticker("SPY")
    px = spy.history(period="max", auto_adjust=auto_adjust)
    if px.empty:
        return None, pd.Series(dtype=float)
    price = px[["Close"]].rename(columns={"Close": "price"})
    price.index = price.index.tz_localize(None)
    price.index = price.index.normalize() + pd.Timedelta(hours=23)
    price.index.name = "DATETIME"

    divs = spy.dividends
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
    # Truncate first year of dividend yield
    if len(yield_1y) > 365:
        yield_1y = yield_1y.iloc[365:]
    return price, yield_1y


def _download_treasury_3mo() -> pd.DataFrame:
    """Fetch Daily Treasury Par Yield Curve 3-month rate (FRED DGS3MO). Return df (used for FUNDING_COST in SPY outputs)."""
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS3MO"
    df = pd.read_csv(url)
    date_col = "observation_date" if "observation_date" in df.columns else "DATE"
    df[date_col] = pd.to_datetime(df[date_col])
    return df


def _build_and_save_spy_with_div_and_funding(
    spy_price: pd.DataFrame,
    div_yield: pd.Series,
    rate_df: pd.DataFrame,
    rate_col: str = "DGS3MO",
    output_filename: str = "SPY_yfinance_unadj.csv",
    minimal_columns: bool = False,
) -> None:
    """Inner-join div yield and rate series on date; merge with SPY price; write output_filename with FUNDING_COST = rate/100."""
    if spy_price is None or spy_price.empty or div_yield.empty:
        print("No SPY price or div yield, skipping %s" % output_filename)
        return
    date_col = "observation_date" if "observation_date" in rate_df.columns else "DATE"
    rate_clean = rate_df.dropna(subset=[date_col, rate_col]).copy()
    rate_clean["date"] = pd.to_datetime(rate_clean[date_col]).dt.normalize()

    div_df = div_yield.to_frame("DIVIDEND_YIELD")
    div_df["date"] = div_df.index.normalize() if hasattr(div_df.index, "normalize") else div_df.index

    # Expand rate to daily (ffill) so we have a value for every div date, then inner-join
    rate_daily = rate_clean.set_index("date")[[rate_col]]
    day_range = pd.date_range(div_df["date"].min(), div_df["date"].max(), freq="B")
    rate_daily = rate_daily.reindex(day_range).ffill().bfill()
    rate_daily = rate_daily.reset_index().rename(columns={"index": "date"})
    merged = div_df.merge(rate_daily, on="date", how="inner")
    merged["FUNDING_COST"] = merged[rate_col] / 100
    merged = merged.drop(columns=[rate_col])
    if merged.empty:
        print("No overlap after inner-join, skipping %s" % output_filename)
        return

    # Attach SPY price for these dates (spy_price index is EOD datetime)
    spy_by_date = spy_price.copy()
    spy_by_date.index = spy_by_date.index.normalize()
    merged["price"] = merged["date"].map(spy_by_date["price"])
    merged = merged.dropna(subset=["price"])
    if merged.empty:
        print("No SPY price on joined dates, skipping %s" % output_filename)
        return

    # DATETIME = date at end-of-day (23:00)
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
    out_path = os.path.join(_out_dir(), output_filename)
    out.to_csv(out_path)
    print("Saved %s (%d rows) to %s" % (output_filename, len(out), out_path))
    print("  Range: %s to %s" % (out.index[0], out.index[-1]))


def main() -> None:
    _download_price_series("^GSPC", "GSPC.csv")

    treasury_3mo_df = _download_treasury_3mo()

    spy_price_unadj, div_yield_unadj = _get_spy_price_and_dividend_yield(auto_adjust=False)
    _build_and_save_spy_with_div_and_funding(
        spy_price_unadj, div_yield_unadj, treasury_3mo_df, output_filename="SPY_yfinance_unadj.csv"
    )

    spy_price_adj, div_yield_adj = _get_spy_price_and_dividend_yield(auto_adjust=True)
    _build_and_save_spy_with_div_and_funding(
        spy_price_adj, div_yield_adj, treasury_3mo_df, output_filename="SPY_yfinance_adj.csv", minimal_columns=True
    )


if __name__ == "__main__":
    main()

