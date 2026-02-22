"""
Same for estimated system
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime

from sysdata.config.configdata import Config
from systems.provided.futures_chapter15.estimatedsystem import futures_system
from systems.accounts.from_returns import account_curve_from_returns
from matplotlib.pyplot import gcf


def _geo_worst_drawdown(returns_decimal):
    """Geometric (price-level) worst drawdown from decimal daily returns.
    Returns (worst_pct, peak_date, trough_date).
    """
    returns_decimal = returns_decimal.dropna()
    geo_cum = (1 + returns_decimal).cumprod()
    geo_dd = (geo_cum - geo_cum.cummax()) / geo_cum.cummax() * 100
    worst_val = float(geo_dd.min())
    trough_date = geo_dd.idxmin()
    peak_val = float(geo_cum.cummax().loc[trough_date])
    peak_date = geo_cum.loc[:trough_date][geo_cum.loc[:trough_date] >= peak_val * (1 - 1e-8)].index[-1]
    return worst_val, peak_date, trough_date

# results folder under introduction/results
_script_dir = os.path.dirname(os.path.abspath(__file__))
results_dir = os.path.join(_script_dir, "results")
os.makedirs(results_dir, exist_ok=True)
_run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = os.path.join(results_dir, "estimatedsystem_%s.log" % _run_ts)
plot_path = os.path.join(results_dir, "portfolio_curve_%s.png" % _run_ts)

config = Config([
    "systems.provided.futures_chapter15.futuresestimateconfig.yaml",
    "examples.introduction.config_estimatedsystem.yaml",
])
system = futures_system(config=config)
# system.accounts.portfolio().curve().plot()
# gcf().savefig(plot_path)
system.cache.pickle("private.this_system_name.pck")
# this will run much faster and reuse previous calculations
system.cache.unpickle("private.this_system_name.pck")

portfolio = system.accounts.portfolio()
sharpe_val = portfolio.sharpe()
pct = portfolio.percent
stats_obj = pct.stats()
sys_geo_dd, sys_peak_date, sys_trough_date = _geo_worst_drawdown(pd.Series(pct.values, index=pct.index) / 100)

# Buy and Hold: from config start_date; weighted sum when multiple assets, single asset when only one has data
start_date_str = config.get_element_or_default("start_date", None)
bh_start = pd.Timestamp(start_date_str) if start_date_str else None
instruments = system.get_instrument_list()
pct_returns_list = []
for code in instruments:
    prices = system.rawdata.get_daily_prices(code)
    pct_returns_list.append(prices.pct_change(fill_method=None))
pct_returns_df = pd.concat(pct_returns_list, axis=1, join="outer")
pct_returns_df.columns = instruments
# clip extreme daily returns (e.g. bad data) so min/max stats stay sane; decimal -1 = -100%
pct_returns_df = pct_returns_df.clip(lower=-1.0, upper=1.0)
if bh_start is not None:
    pct_returns_df = pct_returns_df.loc[pct_returns_df.index >= bh_start]
weights = system.portfolio.get_instrument_weights()
weight_cols = [c for c in instruments if c in weights.columns]
# only use dates where system has weights (no bfill from future); ffill within range
first_weight_date = weights.index.min()
bh_from = max(bh_start, first_weight_date) if bh_start is not None else first_weight_date
pct_returns_df = pct_returns_df.loc[pct_returns_df.index >= bh_from]
weights_sub = weights[weight_cols].reindex(pct_returns_df.index).ffill()
weights_sub = weights_sub.div(weights_sub.sum(axis=1), axis=0)

def _bh_return_row(row, w):
    avail_mask = row[weight_cols].notna()
    avail_cols = [c for c in weight_cols if avail_mask[c]]
    n = len(avail_cols)
    if n == 0:
        return (np.nan, False)
    if n == 1:
        return (row[avail_cols[0]], False)
    w_row = w.loc[row.name][avail_cols]
    if w_row.isna().any() or (w_row <= 0).any() or w_row.sum() == 0 or not np.isfinite(w_row.sum()):
        return (np.nan, True)
    w_row = w_row / w_row.sum()
    return ((row[avail_cols] * w_row).sum(), False)

_bh_results = pct_returns_df.apply(lambda r: _bh_return_row(r, weights_sub), axis=1)
bh_pct_returns = pd.Series([x[0] for x in _bh_results], index=_bh_results.index).dropna()
bh_skipped = [d for d, (_, skip) in _bh_results.items() if skip]
bh_skipped_count = len(bh_skipped)
bh_curve = account_curve_from_returns(bh_pct_returns)
bh_stats_obj = bh_curve.percent.stats()
bh_geo_dd, bh_peak_date, bh_trough_date = _geo_worst_drawdown(bh_pct_returns)

# SP500 Buy and Hold: read from repo data folder, same period as config (start_date) and system end
_repo_root = os.path.dirname(os.path.dirname(_script_dir))
_sp500_csv = os.path.join(_repo_root, "data", "futures", "adjusted_prices_csv", "SP500.csv")
sp500_raw = pd.read_csv(_sp500_csv, index_col=0, parse_dates=True)
sp500_prices = sp500_raw.iloc[:, 0].sort_index()
sp500_returns = sp500_prices.pct_change(fill_method=None).dropna()
if bh_start is not None:
    sp500_returns = sp500_returns.loc[sp500_returns.index >= bh_start]
sys_end = portfolio.percent.index[-1]
sp500_returns = sp500_returns.loc[sp500_returns.index <= sys_end]
sp500_curve = account_curve_from_returns(sp500_returns)
sp500_stats_obj = sp500_curve.percent.stats()
sp500_geo_dd, sp500_peak_date, sp500_trough_date = _geo_worst_drawdown(sp500_returns)

def _period_str(ix):
    if ix is None or len(ix) == 0:
        return "n/a"
    return "%s to %s" % (ix[0].strftime("%Y-%m-%d") if hasattr(ix[0], "strftime") else ix[0], ix[-1].strftime("%Y-%m-%d") if hasattr(ix[-1], "strftime") else ix[-1])

with open(log_path, "w") as f:
    f.write("estimatedsystem.py run at %s\n\n" % datetime.now().isoformat())
    f.write("Data period: %s\n\n" % _period_str(portfolio.percent.index))
    f.write("Sharpe: %s\n\n" % sharpe_val)
    def _write_stats(f, stats_obj, geo_dd, peak_date, trough_date):
        items = stats_obj[0] if isinstance(stats_obj, (list, tuple)) and len(stats_obj) >= 1 else []
        for name, val in items:
            if name == "worst_drawdown":
                f.write("  geometric_worst_drawdown: %.2f (peak: %s, trough: %s)\n" % (
                    geo_dd,
                    peak_date.strftime("%Y-%m-%d") if hasattr(peak_date, "strftime") else peak_date,
                    trough_date.strftime("%Y-%m-%d") if hasattr(trough_date, "strftime") else trough_date,
                ))
            else:
                f.write("  %s: %s\n" % (name, val))

    f.write("Percent stats:\n")
    _write_stats(f, stats_obj, sys_geo_dd, sys_peak_date, sys_trough_date)
    f.write("\n  You can also plot / print: ['rolling_ann_std', 'drawdown', 'curve', 'percent'] (time series)\n")
    f.write("\nBuy and Hold percent stats:\n")
    f.write("  Data period: %s\n" % _period_str(bh_pct_returns.index))
    if bh_skipped_count > 0:
        f.write("  Skipped %d dates with missing/zero/NaN weights (e.g. %s ... %s).\n" % (
            bh_skipped_count,
            bh_skipped[0].strftime("%Y-%m-%d") if hasattr(bh_skipped[0], "strftime") else bh_skipped[0],
            bh_skipped[-1].strftime("%Y-%m-%d") if hasattr(bh_skipped[-1], "strftime") else bh_skipped[-1],
        ))
    _write_stats(f, bh_stats_obj, bh_geo_dd, bh_peak_date, bh_trough_date)
    f.write("\nSP500 B&H percent stats:\n")
    f.write("  Data period: %s\n" % _period_str(sp500_returns.index))
    _write_stats(f, sp500_stats_obj, sp500_geo_dd, sp500_peak_date, sp500_trough_date)
