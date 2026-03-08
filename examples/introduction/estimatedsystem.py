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
from systems.accounts.curves.account_curve import accountCurve
from systems.accounts.pandl_calculators.pandl_cash_costs import pandlCalculationWithCashCostsAndFills
from matplotlib.pyplot import gcf


_EXCEL_ERRORS = ["#VALUE!", "#N/A!", "#N/A", "#REF!", "#DIV/0!", "#NUM!", "#NAME?", "#NULL!"]

def _reformat_bbg_dates_if_needed(csv_path):
    """Fix BBG CSV in place (idempotent):
    - Reformat date column from M/D/YYYY to YYYY-MM-DD
    - Replace Excel error strings (#VALUE!, #N/A, etc.) with NaN (empty cell)
    """
    # Read all columns as strings so we can cheaply detect what needs fixing
    df_str = pd.read_csv(csv_path, dtype=str)
    sample = df_str.iloc[0, 0]
    date_needs_fix = True
    try:
        pd.to_datetime(sample, format="%Y-%m-%d")
        date_needs_fix = False
    except ValueError:
        pass
    had_errors = df_str.isin(_EXCEL_ERRORS).any().any()
    if not date_needs_fix and not had_errors:
        return
    # Re-read with proper dtypes, converting Excel errors to NaN
    df = pd.read_csv(csv_path, na_values=_EXCEL_ERRORS, keep_default_na=True)
    if date_needs_fix:
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], format="mixed").dt.strftime("%Y-%m-%d")
    df.to_csv(csv_path, index=False)


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
log_path = os.path.join(results_dir, "es_%s.log" % _run_ts)
plot_path = os.path.join(results_dir, "portfolio_curve_%s.png" % _run_ts)

config = Config([
    "systems.provided.futures_chapter15.futuresestimateconfig.yaml",
    "examples.introduction.config_estimatedsystem.yaml",
])
_repo_root = os.path.dirname(os.path.dirname(_script_dir))
if config.get_element_or_default("use_bbg", False):
    # Append _BBG suffix to every instrument code so the system loads *_BBG.csv files
    config.instruments = [code + "_BBG" for code in config.instruments]
    # Preprocess BBG CSV files: fix M/D/YYYY dates and Excel error strings in-place
    for _subdir in ("adjusted_prices_csv", "multiple_prices_csv"):
        _data_dir = os.path.join(_repo_root, "data", "futures", _subdir)
        for _f in os.listdir(_data_dir):
            if _f.endswith("_BBG.csv"):
                _reformat_bbg_dates_if_needed(os.path.join(_data_dir, _f))

# Preprocess *_yfinance.csv and *_yfinance_unadj.csv: mm/dd/yyyy -> yyyy-mm-dd in-place
for _subdir in ("adjusted_prices_csv", "multiple_prices_csv"):
    _data_dir = os.path.join(_repo_root, "data", "futures", _subdir)
    if os.path.isdir(_data_dir):
        for _f in os.listdir(_data_dir):
            if _f.endswith("_yfinance.csv") or _f.endswith("_yfinance_unadj.csv") or _f.endswith("_yfinance_adj.csv"):
                _reformat_bbg_dates_if_needed(os.path.join(_data_dir, _f))

# SPY.csv in data/futures/adjusted_prices_csv is loaded by default data
system = futures_system(config=config)
# system.accounts.portfolio().curve().plot()
# gcf().savefig(plot_path)
system.cache.pickle("private.this_system_name.pck")
# this will run much faster and reuse previous calculations
system.cache.unpickle("private.this_system_name.pck")

portfolio = system.accounts.portfolio()
end_date_str = config.get_element_or_default("end_date", None)
sys_end = pd.Timestamp(end_date_str) if end_date_str else None
pct = portfolio.percent
if sys_end is not None:
    pct_dec = pd.Series(pct.values, index=pct.index) / 100
    pct_dec = pct_dec.loc[pct_dec.index <= sys_end]
    pct = account_curve_from_returns(pct_dec).percent
stats_obj = pct.stats()
sharpe_val = next((val for name, val in (stats_obj[0] if stats_obj else []) if name == "sharpe"), float("nan"))
sys_geo_dd, sys_peak_date, sys_trough_date = _geo_worst_drawdown(pd.Series(pct.values, index=pct.index) / 100)

def _bh_curve_for_instrument(system, code):
    """B&H accountCurve for one instrument: always long at the same vol-targeted size as the system.
    For instruments in the portfolio: vol scalar × instr weight × IDM (forecast = +10, fully long).
    For instruments not in the portfolio (e.g. SP500 benchmark): vol scalar only (all capital, IDM = 1).
    Roll costs included.
    """
    price = system.accounts.get_instrument_prices_for_position_or_forecast(code)
    # use portfolio-level sizing if the instrument is in the system, subsystem-level otherwise
    portfolio_instruments = system.get_instrument_list()
    if code in portfolio_instruments:
        avg_pos = system.accounts.get_average_position_for_instrument_at_portfolio_level(code)
    else:
        avg_pos = system.accounts.get_average_position_at_subsystem_level(code)
    positions = avg_pos.reindex(price.index).ffill().fillna(0.0)
    raw_costs = system.accounts.get_raw_cost_data(code)
    fx = system.accounts.get_fx_rate(code)
    value_per_point = system.accounts.get_value_of_block_price_move(code)
    capital = system.accounts.get_notional_capital()
    rolls_per_year = system.accounts.get_rolls_per_year(code)
    vol_normalise_currency_costs = system.config.vol_normalise_currency_costs
    multiply_roll_costs_by = system.config.multiply_roll_costs_by

    pandl_calc = pandlCalculationWithCashCostsAndFills(
        price,
        raw_costs=raw_costs,
        positions=positions,
        capital=capital,
        value_per_point=value_per_point,
        fx=fx,
        rolls_per_year=rolls_per_year,
        vol_normalise_currency_costs=vol_normalise_currency_costs,
        multiply_roll_costs_by=multiply_roll_costs_by,
    )
    return accountCurve(pandl_calc)

def _bh_curve_all_in(system, code):
    """All-in B&H for a single instrument: always full vol-targeted size at subsystem level, roll-cost net."""
    price = system.accounts.get_instrument_prices_for_position_or_forecast(code)
    avg_pos = system.accounts.get_average_position_at_subsystem_level(code)
    positions = avg_pos.reindex(price.index).ffill().fillna(0.0)

    raw_costs = system.accounts.get_raw_cost_data(code)
    fx = system.accounts.get_fx_rate(code)
    value_per_point = system.accounts.get_value_of_block_price_move(code)
    capital = system.accounts.get_notional_capital()
    rolls_per_year = system.accounts.get_rolls_per_year(code)
    vol_normalise_currency_costs = system.config.vol_normalise_currency_costs
    multiply_roll_costs_by = system.config.multiply_roll_costs_by

    pandl_calc = pandlCalculationWithCashCostsAndFills(
        price,
        raw_costs=raw_costs,
        positions=positions,
        capital=capital,
        value_per_point=value_per_point,
        fx=fx,
        rolls_per_year=rolls_per_year,
        vol_normalise_currency_costs=vol_normalise_currency_costs,
        multiply_roll_costs_by=multiply_roll_costs_by,
    )
    return accountCurve(pandl_calc)

def _build_bh_stats(curve_or_returns, label):
    """Build stats + geo drawdown from an accountCurve or decimal daily returns series."""
    if isinstance(curve_or_returns, accountCurve):
        curve = curve_or_returns
        returns_decimal = pd.Series(curve.percent.values, index=curve.percent.index) / 100
    else:
        curve = account_curve_from_returns(curve_or_returns)
        returns_decimal = curve_or_returns
    stats_obj = curve.percent.stats()
    geo_dd, peak, trough = _geo_worst_drawdown(returns_decimal.dropna())
    return curve, stats_obj, geo_dd, peak, trough

# B&H: from config start_date; positions are the same vol-targeted sizes as the system
# Each instrument uses avg_pos = vol_scalar × instr_weight × IDM (forecast = +10, always long)
# Portfolio daily return = sum of per-instrument percentage contributions (no re-weighting needed)
start_date_str = config.get_element_or_default("start_date", None)
bh_start = pd.Timestamp(start_date_str) if start_date_str else None
if sys_end is None:
    sys_end = pct.index[-1]
instruments = system.get_instrument_list()

# Per-instrument period bounds: instrument_periods_may_have_bbg_suffix (remapped when use_bbg) + instrument_periods (no suffix)
_periods_may_bbg = config.get_element_or_default("instrument_periods_may_have_bbg_suffix", {}) or {}
_periods_no_suffix = config.get_element_or_default("instrument_periods", {}) or {}
if config.get_element_or_default("use_bbg", False):
    _base = [c.replace("_BBG", "") for c in config.instruments]
    _remapped = {(k + "_BBG" if k in _base else k): v for k, v in _periods_may_bbg.items()}
    _instr_periods_raw = dict(_remapped, **_periods_no_suffix)
else:
    _instr_periods_raw = dict(_periods_may_bbg, **_periods_no_suffix)

def _instr_window(code):
    """Return (eff_start, eff_end) for a code, merging instrument_periods with global bounds."""
    p = _instr_periods_raw.get(code, {}) or {}
    i_start = pd.Timestamp(p["start"]) if p.get("start") else None
    i_end = pd.Timestamp(p["end"]) if p.get("end") else None
    eff_start = max(bh_start, i_start) if (bh_start and i_start) else (bh_start or i_start)
    eff_end = min(sys_end, i_end) if (sys_end and i_end) else (sys_end or i_end)
    return eff_start, eff_end

# Build per-instrument B&H portfolio-level % contributions, each clipped to its own period
instr_pct_list = []
for code in instruments:
    c = _bh_curve_for_instrument(system, code)
    s = pd.Series(c.percent.values, index=c.percent.index, name=code)
    eff_start, eff_end = _instr_window(code)
    if eff_start is not None:
        s = s.loc[s.index >= eff_start]
    if eff_end is not None:
        s = s.loc[s.index <= eff_end]
    instr_pct_list.append(s)
pct_returns_df = pd.concat(instr_pct_list, axis=1, join="outer")

# sum portfolio contributions each day (NaN only when every instrument is NaN)
bh_pct_sum = pct_returns_df.sum(axis=1, min_count=1)
# divide by 100: percent → decimal for geo drawdown and account_curve_from_returns
bh_pct_returns = bh_pct_sum.dropna() / 100
_, bh_stats_obj, bh_geo_dd, bh_peak_date, bh_trough_date = _build_bh_stats(bh_pct_returns, "B&H")

def _period_str(ix):
    if ix is None or len(ix) == 0:
        return "n/a"
    return "%s to %s" % (ix[0].strftime("%Y-%m-%d") if hasattr(ix[0], "strftime") else ix[0], ix[-1].strftime("%Y-%m-%d") if hasattr(ix[-1], "strftime") else ix[-1])

with open(log_path, "w") as f:
    f.write("estimatedsystem.py run at %s\n\n" % datetime.now().isoformat())
    f.write("Data period: %s\n\n" % _period_str(pct.index))
    f.write("Sharpe: %s\n\n" % sharpe_val)
    _daily_fields = {"min", "max", "median", "mean", "std", "skew"}
    def _write_stats(f, stats_obj, geo_dd, peak_date, trough_date):
        items = stats_obj[0] if isinstance(stats_obj, (list, tuple)) and len(stats_obj) >= 1 else []
        daily_written = False
        for name, val in items:
            if name in _daily_fields:
                if not daily_written:
                    f.write("  Daily:\n")
                    daily_written = True
                f.write("    %s: %s\n" % (name, val))
            elif name == "worst_drawdown":
                f.write("  geometric_worst_drawdown: %.2f (peak: %s, trough: %s)\n" % (
                    geo_dd,
                    peak_date.strftime("%Y-%m-%d") if hasattr(peak_date, "strftime") else peak_date,
                    trough_date.strftime("%Y-%m-%d") if hasattr(trough_date, "strftime") else trough_date,
                ))
            else:
                f.write("  %s: %s\n" % (name, val))

    f.write("Instruments: %s\n\n" % instruments)
    f.write("Percent stats:\n")
    _write_stats(f, stats_obj, sys_geo_dd, sys_peak_date, sys_trough_date)
    f.write("\n  You can also plot / print: ['rolling_ann_std', 'drawdown', 'curve', 'percent'] (time series)\n")
    f.write("\nB&H percent stats:\n")
    f.write("  Data period: %s\n" % _period_str(bh_pct_returns.index))
    _write_stats(f, bh_stats_obj, bh_geo_dd, bh_peak_date, bh_trough_date)
    bnh_list = config.get_element_or_default("bnh_instruments", []) or []
    if bnh_list:
        f.write("\nB&H benchmarks (all-in, full vol-targeted):\n")
        for code in bnh_list:
            bh_curve = _bh_curve_all_in(system, code)
            bh_series = pd.Series(bh_curve.percent.values, index=bh_curve.percent.index)
            eff_start, eff_end = _instr_window(code)
            if eff_start is not None:
                bh_series = bh_series.loc[bh_series.index >= eff_start]
            if eff_end is not None:
                bh_series = bh_series.loc[bh_series.index <= eff_end]
            bh_dec = bh_series.dropna() / 100
            _, stats_obj_bnh, geo_dd_bnh, peak_bnh, trough_bnh = _build_bh_stats(bh_dec, f"{code} B&H")
            f.write("\n%s B&H percent stats:\n" % code)
            f.write("  Data period: %s\n" % _period_str(bh_series.index))
            _write_stats(f, stats_obj_bnh, geo_dd_bnh, peak_bnh, trough_bnh)
