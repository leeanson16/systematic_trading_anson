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


MARGIN_COST_BP = 100.0
FUNDING_COST_FILENAME = "FUNDING_COST.csv"


def _gross_capital_usage_and_leverage(system, instruments, end_ts, pct_index):
    """Gross notional exposure as proportion of capital; same date filter as system pct.
    Returns (avg_capital_used_pct, avg_leverage, gross_proportion_series). Series is None if empty."""
    weight_series_list = []
    for code in instruments:
        try:
            w = system.portfolio.get_portfolio_weight_series_from_contract_positions(code)
            if w is not None and len(w) > 0:
                weight_series_list.append(pd.Series(w, name=code))
        except Exception:
            pass
    if not weight_series_list:
        return float("nan"), float("nan"), None
    weights_df = pd.concat(weight_series_list, axis=1, join="outer")
    weights_df = weights_df.reindex(weights_df.index.union(pct_index)).ffill()
    gross_proportion = weights_df.abs().sum(axis=1)
    if end_ts is not None:
        gross_proportion = gross_proportion.loc[gross_proportion.index <= end_ts]
    gross_proportion = gross_proportion.dropna()
    if len(gross_proportion) == 0:
        return float("nan"), float("nan"), None
    avg_proportion = float(gross_proportion.mean())
    return avg_proportion * 100.0, avg_proportion, gross_proportion


def _margin_cost_series(gross_proportion_series: pd.Series, funding_cost_path: str, margin_bp: float = 100.0) -> pd.Series:
    """Daily margin cost as % of capital: applied to excess (gross - 1) at rate = FUNDING_COST + margin_bp bp, annualised then daily.
    Returns series of cost in percent (e.g. 0.05 = 5 bps per day), index aligned to gross_proportion."""
    if gross_proportion_series is None or len(gross_proportion_series) == 0:
        return pd.Series(dtype=float)
    if not os.path.isfile(funding_cost_path):
        return pd.Series(0.0, index=gross_proportion_series.index)
    funding = pd.read_csv(funding_cost_path, index_col=0, parse_dates=True)
    if "FUNDING_COST" not in funding.columns or funding.empty:
        return pd.Series(0.0, index=gross_proportion_series.index)
    funding = funding[["FUNDING_COST"]]
    funding.index = pd.DatetimeIndex(funding.index).normalize()
    gross_dates = pd.DatetimeIndex(gross_proportion_series.index).normalize()
    funding = funding.reindex(gross_dates).ffill().bfill()
    excess = np.maximum(gross_proportion_series.values - 1.0, 0.0)
    rate_annual = funding["FUNDING_COST"].values + margin_bp / 10000.0
    cost_decimal_daily = excess * rate_annual / 252.0
    return pd.Series(cost_decimal_daily * 100.0, index=gross_proportion_series.index).fillna(0.0)


# results folder under introduction/results
_script_dir = os.path.dirname(os.path.abspath(__file__))
results_dir = os.path.join(_script_dir, "results")
os.makedirs(results_dir, exist_ok=True)
_run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = os.path.join(results_dir, "es_%s.log" % _run_ts)
plot_path = os.path.join(results_dir, "portfolio_curve_%s.png" % _run_ts)

import time as _time
_wall_start = _time.perf_counter()

config = Config([
    "systems.provided.futures_chapter15.futuresestimateconfig.yaml",
    "examples.introduction.config_estimatedsystem.yaml",
])
_repo_root = os.path.dirname(os.path.dirname(_script_dir))

# Load instruments from CSV if configured (symbols get suffix, e.g. MMM -> MMM_yfinance)
_csv_path = config.get_element_or_default("instruments_from_csv", None)
if _csv_path:
    _full_csv = os.path.join(_repo_root, _csv_path.replace("/", os.sep))
    if os.path.isfile(_full_csv):
        _col = config.get_element_or_default("instruments_column", "Symbol")
        _suffix = config.get_element_or_default("instruments_suffix", "_yfinance")
        _df = pd.read_csv(_full_csv)
        _symbols = _df[_col].astype(str).str.strip().str.replace(".", "-", regex=False).dropna().unique().tolist()
        config.instruments = [s + _suffix for s in _symbols]

# Preprocess CSV files: fix M/D/YYYY dates and Excel error strings in-place (one pass per subdir)
_use_bbg = config.get_element_or_default("use_bbg", False)
for _subdir in ("adjusted_prices_csv", "multiple_prices_csv"):
    _data_dir = os.path.join(_repo_root, "data", "futures", _subdir)
    if not os.path.isdir(_data_dir):
        continue
    for _f in os.listdir(_data_dir):
        if _f.endswith("_BBG.csv"):
            if not _use_bbg:
                continue
        elif _f.endswith("_yfinance.csv") or _f.endswith("_yfinance_unadj.csv") or _f.endswith("_yfinance_adj.csv"):
            pass
        else:
            continue
        _reformat_bbg_dates_if_needed(os.path.join(_data_dir, _f))
if _use_bbg:
    config.instruments = [code + "_BBG" for code in config.instruments]

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
instruments = system.get_instrument_list()
if sys_end is None:
    sys_end = pct.index[-1]
# Margin cost on excess capital: rate = data/custom/FUNDING_COST.csv + 100bp, applied to (gross - 1)*capital daily
_apply_margin_cost = config.get_element_or_default("apply_margin_cost", True)
_avg_cap_used_pct, _avg_lev, _gross_series = _gross_capital_usage_and_leverage(system, instruments, sys_end, pct.index)
_funding_path = os.path.join(_repo_root, "data", "custom", FUNDING_COST_FILENAME) if _apply_margin_cost else None
_gross_aligned = _gross_series.reindex(pct.index).ffill().bfill() if _gross_series is not None and len(_gross_series) > 0 else None
_margin_cost_pct = _margin_cost_series(_gross_aligned, _funding_path, MARGIN_COST_BP) if _apply_margin_cost and _funding_path else None
_margin_cost_applied = False
if _apply_margin_cost and _gross_aligned is not None and _margin_cost_pct is not None and len(_margin_cost_pct) > 0:
    _margin_aligned = _margin_cost_pct.reindex(pct.index).fillna(0.0)
    pct_net = pd.Series(pct.values, index=pct.index) - _margin_aligned
    pct_net = pct_net.dropna()
    _returns_decimal = pct_net / 100.0
    _curve_net = account_curve_from_returns(_returns_decimal)
    pct = _curve_net.percent
    _margin_cost_applied = True
stats_obj = pct.stats()
sharpe_val = next((val for name, val in (stats_obj[0] if stats_obj else []) if name == "sharpe"), float("nan"))
sys_geo_dd, sys_peak_date, sys_trough_date = _geo_worst_drawdown(pd.Series(pct.values, index=pct.index) / 100)

def _bh_curve_for_instrument(system, code, portfolio_instruments):
    """B&H accountCurve for one instrument: always long at the same vol-targeted size as the system.
    For instruments in the portfolio: vol scalar × instr weight × IDM (forecast = +10, fully long).
    For instruments not in the portfolio (e.g. SP500 benchmark): vol scalar only (all capital, IDM = 1).
    Roll costs included.
    """
    price = system.accounts.get_instrument_prices_for_position_or_forecast(code)
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


def _bh_curve_1x_notional(system, code):
    """1× notional buy-and-hold: position size so notional = 1 × capital at each date. Roll costs net."""
    price = system.accounts.get_instrument_prices_for_position_or_forecast(code)
    value_prop = system.portfolio.get_per_contract_value_as_proportion_of_capital(code)
    value_prop = value_prop.reindex(price.index).ffill()
    positions = (1.0 / value_prop).replace([np.inf, -np.inf], np.nan).fillna(0.0)

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

def _bh_gross_capital_usage_and_leverage(system, instruments, portfolio_instruments, end_ts):
    """B&H gross notional (always long at avg position) as proportion of capital.
    Returns (avg_capital_used_pct, avg_leverage, weights_df). weights_df has instruments as columns."""
    weight_series_list = []
    for code in instruments:
        try:
            if code in portfolio_instruments:
                avg_pos = system.accounts.get_average_position_for_instrument_at_portfolio_level(code)
            else:
                avg_pos = system.accounts.get_average_position_at_subsystem_level(code)
            value_prop = system.portfolio.get_per_contract_value_as_proportion_of_capital(code)
            value_prop = value_prop.reindex(avg_pos.index).ffill()
            w = avg_pos * value_prop
            if w is not None and len(w.dropna()) > 0:
                weight_series_list.append(pd.Series(w, name=code))
        except Exception:
            pass
    if not weight_series_list:
        return float("nan"), float("nan"), None
    weights_df = pd.concat(weight_series_list, axis=1, join="outer")
    weights_df = weights_df.reindex(weights_df.index.union(pct.index)).ffill()
    gross_proportion = weights_df.sum(axis=1)
    if end_ts is not None:
        gross_proportion = gross_proportion.loc[gross_proportion.index <= end_ts]
    gross_proportion = gross_proportion.dropna()
    if len(gross_proportion) == 0:
        return float("nan"), float("nan"), None
    avg_proportion = float(gross_proportion.mean())
    return avg_proportion * 100.0, avg_proportion, weights_df

# Build per-instrument B&H portfolio-level % contributions, each clipped to its own period
instr_pct_list = []
for code in instruments:
    c = _bh_curve_for_instrument(system, code, instruments)
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
_bh_avg_cap_used_pct, _bh_avg_lev, _bh_weights_df = _bh_gross_capital_usage_and_leverage(system, instruments, instruments, sys_end)
# Margin cost on B&H excess capital (same rate: FUNDING_COST + 100bp)
_bh_gross_series = _bh_weights_df.sum(axis=1) if _bh_weights_df is not None and len(_bh_weights_df) > 0 else None
_bh_gross_aligned = _bh_gross_series.reindex(bh_pct_returns.index).ffill().bfill() if _bh_gross_series is not None and len(_bh_gross_series) > 0 else None
_bh_margin_pct = _margin_cost_series(_bh_gross_aligned, _funding_path, MARGIN_COST_BP) if _apply_margin_cost and _funding_path else pd.Series(dtype=float)
_bh_margin_cost_applied = False
if _apply_margin_cost and _bh_gross_aligned is not None and _bh_margin_pct is not None and len(_bh_margin_pct) > 0:
    _bh_margin_aligned = _bh_margin_pct.reindex(bh_pct_returns.index).fillna(0.0) / 100.0
    bh_pct_returns = bh_pct_returns - _bh_margin_aligned
    _bh_margin_cost_applied = True
_, bh_stats_obj, bh_geo_dd, bh_peak_date, bh_trough_date = _build_bh_stats(bh_pct_returns, "B&H")

# B&H cap capital: same B&H but cap gross at 100%; when gross > 1 normalize weights so capital used = 100%
_bh_cap_gross = _bh_gross_aligned.reindex(bh_pct_returns.index).ffill().bfill() if _bh_gross_aligned is not None else None
_bh_cap_avg_gross_pct = None
_bh_cap_avg_lev = None
if _bh_cap_gross is not None and len(_bh_cap_gross) > 0:
    _bh_cap_divisor = _bh_cap_gross.clip(lower=1.0)
    bh_cap_returns = bh_pct_returns / _bh_cap_divisor
    _bh_cap_capped_gross = _bh_cap_gross.clip(upper=1.0)
    _bh_cap_avg_gross_pct = float(_bh_cap_capped_gross.mean()) * 100.0
    _bh_cap_avg_lev = float(_bh_cap_capped_gross.mean())
    _, bh_cap_stats_obj, bh_cap_geo_dd, bh_cap_peak_date, bh_cap_trough_date = _build_bh_stats(bh_cap_returns, "B&H cap capital")
else:
    bh_cap_stats_obj = bh_stats_obj
    bh_cap_geo_dd = bh_geo_dd
    bh_cap_peak_date = bh_peak_date
    bh_cap_trough_date = bh_trough_date
    bh_cap_returns = bh_pct_returns

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
    if _margin_cost_applied:
        f.write("  (includes margin cost on excess capital: data/custom/FUNDING_COST.csv + 100bp)\n")
    _write_stats(f, stats_obj, sys_geo_dd, sys_peak_date, sys_trough_date)
    f.write("\n  You can also plot / print: ['rolling_ann_std', 'drawdown', 'curve', 'percent'] (time series)\n")
    f.write("\nCapital usage (gross notional vs notional capital):\n")
    f.write("  Average capital used: %.2f%%\n" % _avg_cap_used_pct)
    f.write("  Average leverage: %.2fx\n" % _avg_lev)
    _iw = system.portfolio.get_instrument_weights()
    _iw_last = _iw.iloc[-1].sort_values(ascending=False)
    f.write("\nEstimated instrument weights (final, descending):\n")
    _printed = 0
    for _code, _w in _iw_last.items():
        if _w > 1e-6:
            f.write("  %-25s %.4f\n" % (_code, _w))
            _printed += 1
            if _printed >= 5:
                break
    _iw_sum = _iw_last.sum()
    _n_nonzero = (_iw_last > 1e-6).sum()
    f.write("  --- total: %.4f  (%d instruments with weight > 0)\n" % (_iw_sum, _n_nonzero))

    # Standard transaction cost in Sharpe Ratio units (per trade) for all instruments
    f.write("\nStandard cost in Sharpe Ratio (per trade, annualised):\n")
    for _code in instruments:
        try:
            _sr_cost = system.accounts.get_SR_cost_per_trade_for_instrument(_code)
        except Exception:
            _sr_cost = float("nan")
        f.write("  %-25s %.6f\n" % (_code, _sr_cost))
    f.write("\nB&H percent stats:\n")
    if _bh_margin_cost_applied:
        f.write("  (includes margin cost on excess capital: data/custom/FUNDING_COST.csv + 100bp)\n")
    f.write("  Data period: %s\n" % _period_str(bh_pct_returns.index))
    _write_stats(f, bh_stats_obj, bh_geo_dd, bh_peak_date, bh_trough_date)
    f.write("\nCapital usage (gross notional vs notional capital):\n")
    f.write("  Average capital used: %.2f%%\n" % _bh_avg_cap_used_pct)
    f.write("  Average leverage: %.2fx\n" % _bh_avg_lev)
    if _bh_weights_df is not None and len(_bh_weights_df) > 0:
        _bh_last = _bh_weights_df.iloc[-1]
        _bh_sum = _bh_last.sum()
        if _bh_sum > 0:
            _bh_iw_last = (_bh_last / _bh_sum).sort_values(ascending=False)
            f.write("\nEstimated instrument weights (final, descending):\n")
            _printed = 0
            for _code, _w in _bh_iw_last.items():
                if _w > 1e-6:
                    f.write("  %-25s %.4f\n" % (_code, _w))
                    _printed += 1
                    if _printed >= 5:
                        break
            _bh_n_nonzero = (_bh_iw_last > 1e-6).sum()
            f.write("  --- total: %.4f  (%d instruments with weight > 0)\n" % (_bh_iw_last.sum(), _bh_n_nonzero))
    f.write("\nB&H cap capital percent stats:\n")
    f.write("  (Originated from B&H of the system; max capital used capped at 100%: normalize weights when gross > 100%%)\n")
    f.write("  Data period: %s\n" % _period_str(bh_cap_returns.index))
    _write_stats(f, bh_cap_stats_obj, bh_cap_geo_dd, bh_cap_peak_date, bh_cap_trough_date)
    f.write("\nCapital usage (gross notional vs notional capital):\n")
    if _bh_cap_avg_gross_pct is not None and _bh_cap_avg_lev is not None:
        f.write("  Average capital used: %.2f%%\n" % _bh_cap_avg_gross_pct)
        f.write("  Average leverage: %.2fx\n" % _bh_cap_avg_lev)
    else:
        f.write("  Average capital used: n/a\n")
        f.write("  Average leverage: n/a\n")
    bnh_list = config.get_element_or_default("bnh_instruments", []) or []
    if bnh_list:
        f.write("\nB&H benchmarks (1× notional buy-and-hold):\n")
        for code in bnh_list:
            bh_curve = _bh_curve_1x_notional(system, code)
            bh_series = pd.Series(bh_curve.percent.values, index=bh_curve.percent.index)
            eff_start, eff_end = _instr_window(code)
            if eff_start is not None:
                bh_series = bh_series.loc[bh_series.index >= eff_start]
            if eff_end is not None:
                bh_series = bh_series.loc[bh_series.index <= eff_end]
            bh_series = bh_series.dropna()
            bh_dec = bh_series / 100
            _, stats_obj_bnh, geo_dd_bnh, peak_bnh, trough_bnh = _build_bh_stats(bh_dec, f"{code} B&H")
            f.write("\n%s B&H percent stats:\n" % code)
            f.write("  Average capital used: 100.00%%\n")
            f.write("  Average leverage: 1.00x\n")
            f.write("  Data period: %s\n" % _period_str(bh_series.index))
            _write_stats(f, stats_obj_bnh, geo_dd_bnh, peak_bnh, trough_bnh)
    _wall_elapsed = _time.perf_counter() - _wall_start
    _mins, _secs = divmod(_wall_elapsed, 60)
    f.write("\nTotal runtime: %d min %.1f sec (%.1f sec)\n" % (_mins, _secs, _wall_elapsed))
