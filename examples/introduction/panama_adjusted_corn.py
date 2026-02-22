"""
Panama method back-adjustment for CORN futures.

The prices of the previous contract are shifted in parallel so that on the roll date
the adjusted price of the previous contract aligns with the actual price of the
current (new) contract. This is repeated for all past contracts.

Uses:
  - Roll calendar: data/futures/roll_calendars_csv/CORN deduced.csv
  - Multiple prices: data/futures/multiple_prices_csv/CORN.csv
  - Optional: Excel workbook (corn futures_dec - formulas.xlsm) for a second adjusted series
Compares result with: data/futures/adjusted_prices_csv/CORN.csv

Run from project root (e.g. PYTHONPATH=. python examples/introduction/panama_adjusted_corn.py).
Requires pandas. For Excel input requires openpyxl.
"""

import os
import csv
from datetime import datetime

import pandas as pd

# Optional Excel source for a second adjusted series (full path)
CORN_EXCEL_PATH = r"C:\Users\Anson\Documents\python workspace\Systematic trading_abandoned\data\corn futures_dec - formulas.xlsm"
# BBG series cutoff: only include dates on or before this date
BBG_CUTOFF_DATE = "2024-03-28"


def _parse_roll_calendar_date(s: str):
    """Parse DATE_TIME from roll calendar (e.g. '10/18/1973 23:00' or '1973-10-18 23:00:00')."""
    s = s.strip()
    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return pd.to_datetime(s)


def load_roll_calendar(path: str) -> pd.DatetimeIndex:
    """Load roll dates from roll calendar CSV. Returns sorted DatetimeIndex."""
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        dates = [_parse_roll_calendar_date(row["DATE_TIME"]) for row in r if row.get("DATE_TIME")]
    return pd.DatetimeIndex(sorted(dates))


def load_multiple_prices(path: str) -> pd.DataFrame:
    """Load multiple_prices CSV; index = DATETIME, columns include PRICE, PRICE_CONTRACT, FORWARD."""
    df = pd.read_csv(path)
    df["DATETIME"] = pd.to_datetime(df["DATETIME"])
    df = df.set_index("DATETIME").sort_index()
    return df


def load_adjusted_prices(path: str) -> pd.Series:
    """Load adjusted prices CSV; index = DATETIME, single column 'price'."""
    df = pd.read_csv(path)
    df["DATETIME"] = pd.to_datetime(df["DATETIME"])
    df = df.set_index("DATETIME").sort_index()
    return df["price"]


import re

def _parse_dec_contract_sheet_name(name: str) -> int:
    """
    Parse BBG December sheet name to year (sort key).
    Two-digit Z (Z04, Z05, Z06, Z07) -> 2004-2007; one-digit Z (Z4, Z5, Z6, Z7) -> 2024-2027.
    Z59-Z99 -> 1959-1999; Z00-Z58 (two digits) -> 2000-2058.
    """
    m = re.match(r"C\s*Z(\d+)\s*Comdty", name, re.I)
    if not m:
        return 9999
    digits = m.group(1)
    y = int(digits)
    if len(digits) == 1:
        return 2020 + y
    return 1900 + y if y >= 59 else 2000 + y


def _contract_id_to_bbg_sheet(contract_id: str) -> str:
    """Map 20221200 -> 'C Z22 Comdty', 19731200 -> 'C Z73 Comdty'."""
    s = str(int(float(contract_id)))
    if len(s) >= 6:
        yy = int(s[:4]) % 100
        return "C Z%02d Comdty" % yy
    return ""


def load_excel_corn_continuous(
    excel_path: str,
    roll_calendar_path: str,
) -> pd.Series:
    """
    Load CORN December futures from Excel (.xlsm) and build Panama-adjusted
    continuous series using the roll schedule. Before each roll datetime we use
    the current_contract, after we use the next_contract. On the roll date we
    use the next_contract's close. Panama: for each roll, offset = (to_contract
    close on roll date) - (from_contract close on roll date); add offset to all
    prior prices. E.g. 8/21 close = Z23's 8/21 - Z23's 8/22 + Z24's 8/22.
    """
    try:
        import openpyxl
    except ImportError:
        raise ImportError("Reading .xlsm requires openpyxl. Install with: pip install openpyxl")

    # Load roll calendar: (roll_datetime, current_contract, next_contract)
    with open(roll_calendar_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rolls = []
        for row in r:
            dt_str = row.get("DATE_TIME", "").strip()
            cur = (row.get("current_contract", "") or "").strip()
            nxt = (row.get("next_contract", "") or "").strip()
            if not dt_str or not cur or not nxt:
                continue
            roll_dt = _parse_roll_calendar_date(dt_str)
            rolls.append((roll_dt, cur, nxt))
    rolls.sort(key=lambda x: x[0])

    # Load each BBG contract sheet into series by date (date-normalized for lookup)
    xl = pd.ExcelFile(excel_path, engine="openpyxl")
    dec_sheets = [s for s in xl.sheet_names if _parse_dec_contract_sheet_name(s) != 9999]
    dec_sheets.sort(key=_parse_dec_contract_sheet_name)
    if not dec_sheets:
        raise ValueError("No December contract sheets (C Z** Comdty) found in Excel")

    contract_prices = {}
    for sheet in dec_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet, header=None, engine="openpyxl")
        if df.shape[0] < 2:
            continue
        dates = pd.to_datetime(df.iloc[1:, 0], errors="coerce").dropna()
        prices = pd.to_numeric(df.iloc[1:, 4], errors="coerce")
        valid = dates.notna() & prices.notna()
        dates = dates[valid]
        prices = prices[valid]
        if len(dates) == 0:
            continue
        s = pd.Series(prices.values, index=pd.DatetimeIndex(dates.values))
        s = s[~s.index.duplicated(keep="last")]
        contract_prices[sheet] = s.sort_index()

    # Map contract id -> BBG sheet name (calendar uses YYYYMM00 e.g. 20221200 -> C Z22)
    # 2004-2007 -> C Z04, C Z05, C Z06, C Z07; 2024-2027 -> C Z4, C Z5, C Z6, C Z7
    id_to_sheet = {}
    for sheet in dec_sheets:
        y = _parse_dec_contract_sheet_name(sheet)
        if y == 9999:
            continue
        key = "%04d1200" % y
        id_to_sheet[key] = sheet

    # Effective roll date = first date next contract has data (on or after calendar roll). Wait till then.
    effective_rolls = []
    for roll_dt, cur_id, nxt_id in rolls:
        to_sheet = id_to_sheet.get(nxt_id)
        if not to_sheet or to_sheet not in contract_prices:
            effective_rolls.append((roll_dt, cur_id, nxt_id))
            continue
        s_to = contract_prices[to_sheet]
        roll_date = pd.Timestamp(roll_dt).normalize()
        ge = s_to.index >= roll_date
        if not ge.any():
            effective_rolls.append((roll_dt, cur_id, nxt_id))
            continue
        first_to_date = s_to.index[ge][0]
        if hasattr(first_to_date, "normalize"):
            first_to_date = first_to_date.normalize()
        effective_rolls.append((first_to_date, cur_id, nxt_id))
    rolls = effective_rolls

    # All trading dates from any contract (normalize to date)
    all_dates_set = set()
    for s in contract_prices.values():
        for d in s.index:
            all_dates_set.add(pd.Timestamp(d).normalize())
    all_dates = pd.DatetimeIndex(sorted(all_dates_set))
    if len(all_dates) == 0:
        raise ValueError("No price data read from Excel contract sheets")

    def contract_sheet_for_date(d):
        """Return (primary_sheet, fallback_sheet). Uses effective roll date (first day next contract has data)."""
        d_norm = pd.Timestamp(d).normalize()
        fallback = None
        for roll_dt, cur_id, nxt_id in rolls:
            roll_date = pd.Timestamp(roll_dt).normalize()
            if d_norm < roll_date:
                return (id_to_sheet.get(cur_id), fallback)
            if d_norm == roll_date:
                fallback = id_to_sheet.get(cur_id)
                return (id_to_sheet.get(nxt_id), fallback)
            fallback = id_to_sheet.get(nxt_id)
        return (id_to_sheet.get(rolls[-1][2]) if rolls else None, fallback)

    raw_prices = []
    for d in all_dates:
        sheet, fallback_sheet = contract_sheet_for_date(d)
        appended = False
        for candidate in [sheet, fallback_sheet]:
            if not candidate or candidate not in contract_prices:
                continue
            s = contract_prices[candidate]
            idx = s.index.get_indexer([d], method="ffill")[0]
            if idx >= 0:
                raw_prices.append(float(s.iloc[idx]))
                appended = True
                break
        if not appended:
            raw_prices.append(pd.NA)

    raw = pd.Series(raw_prices, index=all_dates).dropna()
    raw = raw[~raw.index.duplicated(keep="last")].sort_index()

    # Panama: at each roll, offset = to_contract(roll_date) - from_contract(roll_date); add to all dates < roll_date
    values = raw.values.tolist()
    index_list = raw.index.tolist()
    for roll_dt, cur_id, nxt_id in rolls:
        from_sheet = id_to_sheet.get(cur_id)
        to_sheet = id_to_sheet.get(nxt_id)
        if not from_sheet or not to_sheet or from_sheet not in contract_prices or to_sheet not in contract_prices:
            continue
        roll_date = pd.Timestamp(roll_dt).normalize()
        s_from = contract_prices[from_sheet]
        s_to = contract_prices[to_sheet]
        from_idx = s_from.index.get_indexer([roll_date], method="ffill")[0]
        to_idx = s_to.index.get_indexer([roll_date], method="ffill")[0]
        if from_idx < 0 or to_idx < 0:
            continue
        price_old = float(s_from.iloc[from_idx])
        price_new = float(s_to.iloc[to_idx])
        offset = price_new - price_old
        for i in range(len(values)):
            if index_list[i] < roll_date:
                values[i] = values[i] + offset

    series = pd.Series(values, index=pd.DatetimeIndex(index_list), dtype=float)
    return series.sort_index()


def panama_adjust_with_roll_calendar(
    multiple_prices: pd.DataFrame,
    roll_dates: pd.DatetimeIndex,
) -> pd.Series:
    """
    Panama method using roll calendar: at each roll date, shift all *previous*
    adjusted prices by (price_new - price_old), then append new contract price.
    Roll date = first row in multiple_prices with index >= that calendar date.
    """
    mp = multiple_prices[["PRICE"]].copy()
    mp = mp[~mp.index.duplicated(keep="last")]
    mp = mp.sort_index()

    roll_dates = roll_dates[roll_dates >= mp.index.min()]
    roll_dates = roll_dates[roll_dates <= mp.index.max()]
    # First index in mp at or after each roll date
    roll_indices = set()
    for rd in roll_dates:
        ge = mp.index >= rd
        if ge.any():
            roll_indices.add(mp.index[ge][0])

    values = []
    for i, dt in enumerate(mp.index):
        price = float(mp.loc[dt, "PRICE"])
        if not values:
            values.append(price)
            continue
        if dt in roll_indices:
            offset = price - values[-1]
            values = [v + offset for v in values]
        values.append(price)

    return pd.Series(values, index=mp.index)


def panama_adjust_from_contract_changes(multiple_prices: pd.DataFrame) -> pd.Series:
    """
    Panama method using roll points where PRICE_CONTRACT changes in multiple_prices.
    Uses same logic as sysobjects.adjusted_prices: at each roll,
    offset = previous_row.FORWARD - previous_row.PRICE (when FORWARD available),
    else offset = current_row.PRICE - previous_row.PRICE.
    Add offset to all previous values, then append current_row.PRICE.
    """
    mp = multiple_prices[["PRICE", "PRICE_CONTRACT", "FORWARD"]].copy()
    mp = mp[~mp.index.duplicated(keep="last")]
    mp = mp.sort_index()
    # Forward-fill FORWARD so we have next-contract price on roll day
    mp["FORWARD"] = mp["FORWARD"].replace("", float("nan")).astype(float)
    mp["FORWARD"] = mp["FORWARD"].ffill()

    values = []
    prev_row = None

    for dt in mp.index:
        row = mp.loc[dt]
        price = float(row["PRICE"])
        contract = row["PRICE_CONTRACT"]

        if prev_row is None:
            values.append(price)
            prev_row = row
            continue

        if contract != prev_row["PRICE_CONTRACT"]:
            # Roll: use FORWARD - PRICE from previous row (codebase logic), else new - old
            fwd = prev_row["FORWARD"]
            if pd.notna(fwd) and str(fwd) != "nan":
                offset = float(fwd) - float(prev_row["PRICE"])
            else:
                offset = price - float(prev_row["PRICE"])
            values = [v + offset for v in values]
            values.append(price)
        else:
            values.append(price)

        prev_row = row

    return pd.Series(values, index=mp.index, dtype=float)


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    roll_path = os.path.join(base, "data", "futures", "roll_calendars_csv", "CORN deduced.csv")
    multi_path = os.path.join(base, "data", "futures", "multiple_prices_csv", "CORN.csv")
    adj_path = os.path.join(base, "data", "futures", "adjusted_prices_csv", "CORN.csv")
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(results_dir, "CORN_panama_adjusted.csv")

    roll_dates = load_roll_calendar(roll_path)
    multiple_prices = load_multiple_prices(multi_path)
    reference = load_adjusted_prices(adj_path)

    # Panama using roll calendar (CORN deduced.csv): roll at each calendar date
    panama_by_calendar = panama_adjust_with_roll_calendar(multiple_prices, roll_dates)

    # Panama from PRICE_CONTRACT changes (matches codebase / reference)
    panama_by_contract = panama_adjust_from_contract_changes(multiple_prices)

    # Align to reference index (use only dates present in both)
    common_idx = reference.index.intersection(panama_by_contract.index)
    common_idx = common_idx.sort_values()
    ref_aligned = reference.reindex(common_idx).ffill().bfill()
    panama_aligned = panama_by_contract.reindex(common_idx).ffill().bfill()
    both = pd.DataFrame({"ref": ref_aligned, "panama": panama_aligned}).dropna()

    diff = both["panama"] - both["ref"]
    max_abs_diff = diff.abs().max()
    mean_abs_diff = diff.abs().mean()
    level_corr = both["ref"].corr(both["panama"])

    # Rebase both to 100 at first common date to compare shape
    first_val = both.iloc[0]
    ref_rebase = both["ref"] / first_val["ref"] * 100
    panama_rebase = both["panama"] / first_val["panama"] * 100
    rebase_diff = (panama_rebase - ref_rebase).abs()
    returns_ref = both["ref"].pct_change().dropna()
    returns_panama = both["panama"].pct_change().dropna()
    common_ret_idx = returns_ref.index.intersection(returns_panama.index)
    ret_corr = returns_ref.reindex(common_ret_idx).ffill().bfill().corr(
        returns_panama.reindex(common_ret_idx).ffill().bfill()
    )

    print("Panama back-adjustment (from PRICE_CONTRACT changes) vs reference adjusted_prices:")
    print("  Common points: %d" % len(both))
    print("  Level correlation: %.6f" % level_corr)
    print("  Max absolute difference (levels): %.6f" % max_abs_diff)
    print("  Mean absolute difference: %.6f" % mean_abs_diff)
    print("  Returns correlation: %.6f" % ret_corr)
    print("  Rebased to 100 at start - max |panama - ref|: %.6f" % rebase_diff.max())

    if ret_corr > 0.999 and rebase_diff.max() < 0.01:
        print("  => Same (returns and rebased shape match).")
    elif ret_corr > 0.99:
        print("  => Very similar (high return correlation).")
    else:
        print("  => Differences exist (roll timing or method variant).")

    pd.DataFrame({"price": panama_by_contract}).to_csv(
        out_path, index=True, index_label="DATETIME"
    )
    print("\nPanama-adjusted series (from contract changes) written to %s" % out_path)
    print("Roll calendar used: %s" % roll_path)

    # Second CSV: adjusted series from Excel workbook (corn futures_dec - formulas.xlsm)
    excel_path = CORN_EXCEL_PATH
    if os.path.isfile(excel_path):
        try:
            excel_series = load_excel_corn_continuous(excel_path, roll_calendar_path=roll_path)
            cutoff = pd.Timestamp(BBG_CUTOFF_DATE).normalize()
            excel_series = excel_series[excel_series.index.normalize() <= cutoff]
            out_excel = os.path.join(results_dir, "CORN_adjusted_from_BBG.csv")
            pd.DataFrame({"price": excel_series}).to_csv(
                out_excel, index=True, index_label="DATETIME"
            )
            print("Adjusted series from BBG written to %s" % out_excel)
            print("  Excel: %s" % excel_path)
            print("  Points: %d, range %s to %s" % (
                len(excel_series), excel_series.index.min(), excel_series.index.max()))
        except Exception as e:
            print("Skipping Excel output: %s" % e)
    else:
        print("Excel file not found (skipping second CSV): %s" % excel_path)


if __name__ == "__main__":
    main()
