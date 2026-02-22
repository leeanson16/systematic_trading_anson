"""
Same for estimated system
"""

import os
import pandas as pd
from datetime import datetime

from sysdata.config.configdata import Config
from systems.provided.futures_chapter15.estimatedsystem import futures_system
from systems.accounts.from_returns import account_curve_from_returns
from matplotlib.pyplot import gcf

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

# Buy and Hold: weighted sum of instrument % returns (same weights as system)
instruments = system.get_instrument_list()
pct_returns_list = []
for code in instruments:
    prices = system.rawdata.get_daily_prices(code)
    pct_returns_list.append(prices.pct_change(fill_method=None))
pct_returns_df = pd.concat(pct_returns_list, axis=1, join="inner")
pct_returns_df.columns = instruments
weights = system.portfolio.get_instrument_weights()
weight_cols = [c for c in instruments if c in weights.columns]
weights_sub = weights[weight_cols].reindex(pct_returns_df.index).ffill().bfill()
weights_sub = weights_sub.div(weights_sub.sum(axis=1), axis=0)
bh_pct_returns = (pct_returns_df[weight_cols] * weights_sub).sum(axis=1).dropna()
bh_curve = account_curve_from_returns(bh_pct_returns)
bh_stats_obj = bh_curve.percent.stats()

# SP500 Buy and Hold: read directly from repo data folder
_repo_root = os.path.dirname(os.path.dirname(_script_dir))
_sp500_csv = os.path.join(_repo_root, "data", "futures", "adjusted_prices_csv", "SP500.csv")
sp500_raw = pd.read_csv(_sp500_csv, index_col=0, parse_dates=True)
sp500_prices = sp500_raw.iloc[:, 0].sort_index()
sp500_returns = sp500_prices.pct_change(fill_method=None).dropna()
sp500_curve = account_curve_from_returns(sp500_returns)
sp500_stats_obj = sp500_curve.percent.stats()

def _period_str(ix):
    if ix is None or len(ix) == 0:
        return "n/a"
    return "%s to %s" % (ix[0].strftime("%Y-%m-%d") if hasattr(ix[0], "strftime") else ix[0], ix[-1].strftime("%Y-%m-%d") if hasattr(ix[-1], "strftime") else ix[-1])

with open(log_path, "w") as f:
    f.write("estimatedsystem.py run at %s\n\n" % datetime.now().isoformat())
    f.write("Data period: %s\n\n" % _period_str(portfolio.percent.index))
    f.write("Sharpe: %s\n\n" % sharpe_val)
    f.write("Percent stats:\n")
    if isinstance(stats_obj, (list, tuple)) and len(stats_obj) >= 1:
        for name, val in stats_obj[0]:
            f.write("  %s: %s\n" % (name, val))
        f.write("\n  You can also plot / print: ['rolling_ann_std', 'drawdown', 'curve', 'percent'] (time series)\n")
    else:
        f.write("%s\n" % stats_obj)
    f.write("\nBuy and Hold percent stats:\n")
    f.write("  Data period: %s\n" % _period_str(bh_pct_returns.index))
    if isinstance(bh_stats_obj, (list, tuple)) and len(bh_stats_obj) >= 1:
        for name, val in bh_stats_obj[0]:
            f.write("  %s: %s\n" % (name, val))
    else:
        f.write("%s\n" % bh_stats_obj)
    f.write("\nSP500 B&H percent stats:\n")
    f.write("  Data period: %s\n" % _period_str(sp500_returns.index))
    if isinstance(sp500_stats_obj, (list, tuple)) and len(sp500_stats_obj) >= 1:
        for name, val in sp500_stats_obj[0]:
            f.write("  %s: %s\n" % (name, val))
    else:
        f.write("%s\n" % sp500_stats_obj)
