## Introduction scripts overview

This folder contains a small workflow around the equity “estimated system” example. The three main scripts are:

- `estimatedsystem.py`
- `dl_yfinance.py`
- `add_instrument.py`

This README explains what each script does, what it reads and writes, and how they fit together.

---

## `dl_yfinance.py` – download prices, dividend yield and funding cost

**Purpose**

Download market data for:

- The S&P 500 index (`^GSPC`) and SPY ETF, and
- All component symbols listed in `data/custom/S&P_500_component_stocks.csv`

and write:

- **Unadjusted & adjusted prices for each stock** into the standard futures CSV layout, and
- A **funding rate time series** used for carry and margin-cost calculations.

**Key inputs**

- `data/custom/S&P_500_component_stocks.csv`
  - Column: `Symbol`
  - Each symbol becomes an instrument code `SYMBOL_yfinance`.

**Key outputs**

- `data/custom/GSPC.csv`
- `data/custom/SPY_yfinance_unadj.csv`
- `data/custom/SPY_yfinance_adj.csv`
- `data/custom/FUNDING_COST.csv`
  - `DATETIME`, `FUNDING_COST` (decimal, e.g. 0.05 = 5% per year)
- For each symbol in the S&P 500 list:
  - `data/futures/multiple_prices_csv/{CODE}.csv`
  - `data/futures/adjusted_prices_csv/{CODE}.csv`
  - Where `{CODE} = SYMBOL_yfinance`
  - Multiple-prices files include `PRICE`, `CARRY`, `FORWARD`, `DIVIDEND_YIELD`, `FUNDING_COST` and related contract columns.

If a target CSV already exists it is **not** overwritten (skip-if-exists).

**Behaviour notes**

- Uses `yfinance` for prices and dividends, and FRED `DGS3MO` for funding (`FUNDING_COST`).
- Adds a small delay between tickers: `DELAY_BETWEEN_TICKERS_SEC = 0.25`.

**How to run**

```bash
python examples/introduction/dl_yfinance.py
```

Run this before the system backtest so that all `_yfinance` instruments and the funding series are present.

---

## `add_instrument.py` – auto-populate CSV config for new instruments

**Purpose**

Scan the adjusted prices directory for instruments and ensure the three futures CSV config files contain rows for every instrument, adding missing rows with sensible defaults.

**Scans**

- `data/futures/adjusted_prices_csv/*.csv` for instrument codes.

**Updates (if missing)**

- `data/futures/csvconfig/instrumentconfig.csv`
- `data/futures/csvconfig/rollconfig.csv`
- `data/futures/csvconfig/spreadcosts.csv`

Defaults for new `_yfinance` stocks:

- `instrumentconfig.csv`
  - `Pointsize=1`, `Currency=USD`, `AssetClass=Equity`, `Region=US`
  - Costs: `PerBlock=0`, `Percentage=0.0004` (≈ 4 bps per leg), `PerTrade=0`
- `rollconfig.csv`
  - Equity-style “hold forever” roll pattern (`H,-999,0,H,0`)
- `spreadcosts.csv`
  - `SpreadCost=0.00` (you can update later with a costs report)

Other (non‑`_yfinance`) instruments use a separate `OTHER_DEFAULTS` block.

**How to run**

```bash
python -m examples.introduction.add_instrument
# or
python examples/introduction/add_instrument.py
python examples/introduction/add_instrument.py --dry-run  # only show changes
```

Typical workflow is:

1. Run `dl_yfinance.py` to create price CSVs for new instruments.
2. Run `add_instrument.py` to fill in any missing rows in the three config CSVs.

---

## `estimatedsystem.py` – run the equity estimated system and log stats

**Purpose**

Run the chapter‑15 “estimated system” on a dynamically defined set of S&P 500 `_yfinance` instruments, then write a detailed log of:

- System performance (“Percent stats”),
- Buy & hold (B&H) portfolio stats,
- 1×‑notional B&H benchmarks (`SP500` and `SPY_yfinance`),
- Capital usage (average gross exposure and leverage),
- Final instrument weights.

**Config and instruments**

- Uses:
  - `systems.provided.futures_chapter15.futuresestimateconfig.yaml`
  - `examples.introduction.config_estimatedsystem.yaml`
- Instruments are **loaded from CSV**:
  - `instruments_from_csv: "data/custom/S&P_500_component_stocks_1-10.csv"`
  - Column: `Symbol`
  - Code suffix: `_yfinance` → e.g. `AAPL` → `AAPL_yfinance`

If `use_bbg: true`, it instead expects `_BBG` instruments and preprocesses BBG CSV files.

**Optimiser (instrument weights)**

When using `estimatedsystem.py`, instrument weights are estimated (with `use_instrument_weight_estimates: True` from `futuresestimateconfig.yaml`). The optimiser used is the one in **instrument_weight_estimate**: by default (from `sysdata.config.defaults`) that is **genericOptimiser** (`sysquant.optimisation.generic_optimiser.genericOptimiser`) with **method: handcraft**. So the effective optimiser is the **handcraft** method (handcrafted portfolio weights from correlation/vol/mean estimates). Neither `futuresestimateconfig.yaml` nor `config_estimatedsystem.yaml` overrides `instrument_weight_estimate`, so this default applies. To use a different method (e.g. `shrinkage`), add an `instrument_weight_estimate:` block in `config_estimatedsystem.yaml` and set `method: shrinkage` (or another supported method).

**Data pre‑processing**

- For Bloomberg and yfinance CSVs in:
  - `data/futures/adjusted_prices_csv/`
  - `data/futures/multiple_prices_csv/`
- It:
  - Normalises date formats (M/D/YYYY → YYYY-MM-DD),
  - Strips Excel error strings (e.g. `#VALUE!`) to `NaN`.

**System run and outputs**

- Builds a `futures_system(config=...)` instance.
- Uses cache pickle/unpickle to speed up repeated runs.
- Computes:
  - System account curve (`portfolio.percent`)
  - Percent stats + Sharpe, drawdowns (geometric),
  - B&H portfolio stats built from per‑instrument vol‑targeted B&H curves,
  - B&H benchmarks from `bnh_instruments`:
    - Currently `['SP500', 'SPY_yfinance']`
    - These are **1× notional buy‑and‑hold** benchmarks (not vol‑targeted).
  - Final estimated instrument weights (`portfolio.get_instrument_weights()` last row).

**Capital usage and margin cost**

- Computes a **daily gross exposure series**:
  - For the system: from `portfolio.get_portfolio_weight_series_from_contract_positions`.
  - For B&H portfolio: from per‑instrument average positions × value per contract.
- Logs:
  - `Average capital used` (mean gross % of capital).
  - `Average leverage` (mean gross multiple).
- If `data/custom/FUNDING_COST.csv` exists, it applies a **margin cost on excess capital**:
  - Excess = `max(0, gross - 1)` (borrowed capital as a fraction of equity).
  - Daily cost rate = `(FUNDING_COST + 100 bp) / 252`.
  - This cost is **subtracted from daily returns** before computing:
    - System Percent stats,
    - B&H portfolio percent stats.
  - Benchmarks in `bnh_instruments` are now **1× notional** and do **not** use leverage or margin cost; their average capital used is always 100% and leverage 1.0×.

**Example: first-asset B&H portfolio sizing**

The strategy’s B&H portfolio holds each instrument at the **same vol‑targeted position** the system would use (instrument weight × IDM × vol scalar). For the **first asset** (e.g. first symbol in the instrument list, e.g. `AAPL_yfinance`), the size in **contracts** is:

1. **Subsystem vol scalar** (contracts if 100% of capital were in this instrument):

   - `annual_cash_vol_target = notional_capital × (percentage_vol_target / 100)`  
     e.g. 250,000 × 0.20 = **50,000** USD/year  
   - `daily_cash_vol_target = annual_cash_vol_target / 252`  
     e.g. 50,000 / 252 ≈ **198.41** USD/day  
   - `block_value = price × value_of_block_price_move × 0.01`  
     ($ per 1% price move per contract; e.g. price 150, value_per_point 100 → 150 × 100 × 0.01 = **150** USD per 1% move)  
   - `instr_value_vol = block_value × daily_perc_vol × fx`  
     (daily $ vol per contract in base currency; e.g. 150 × 1.2% × 1.0 = **1.80** USD/day)  
   - `vol_scalar = daily_cash_vol_target / instr_value_vol`  
     e.g. 198.41 / 1.80 ≈ **110.2** contracts (subsystem)

2. **Portfolio scaling** (instrument weight × IDM):

   - `instrument_weight_1` = estimated weight for first instrument (e.g. 0.12)  
   - `IDM` = instrument diversification multiplier (e.g. 1.4)  
   - `scaling_factor = instrument_weight_1 × IDM`  
     e.g. 0.12 × 1.4 = **0.168**

3. **B&H position for first asset** (contracts):

   - `position_1 = vol_scalar_1 × instrument_weight_1 × IDM`  
     e.g. 110.2 × 0.168 ≈ **18.5** contracts  

So the first asset’s B&H portfolio size is **vol_target / (instrument vol) × notional_capital / ($ vol per contract)** in the subsystem, then **× instrument_weight × IDM** at portfolio level. The same formula applies to every asset in the B&H portfolio; weights and vol scalars differ per instrument.

**Example: first-asset instrument_weight and IDM**

The **instrument_weight** and **IDM** for the first asset (and every asset) come from the estimated system’s portfolio stage; both are estimated, not fixed in config.

1. **Instrument weight (first asset)**  
   - Source: `portfolio.get_instrument_weights()` (estimated when `use_instrument_weight_estimates: True`).  
   - The system uses the **instrument_weight_estimate** config (e.g. `genericOptimiser` with method `handcraft` or `shrinkage`). It takes **subsystem P&L returns** (per-instrument, vol‑scaled) and optionally **turnover**, then solves for weights that (depending on method) e.g. equalise risk, target SR, or shrink toward equal weight.  
   - Output: one weight per instrument, **summing to 1**. The first asset’s weight is the first column’s value on the chosen date (e.g. last row).  
   - **Example value:** `instrument_weight_1 = 0.12` (12% of portfolio).  
   - **Example calculation (handcraft):** Handcraft uses **correlation** and **vol** to build risk weights (e.g. clusters by correlation, uses vol/stdev and mean in estimates; equalise_SR, equalise_vols). For each period the optimiser returns weights that sum to 1. With 10 instruments, if the first asset’s risk weight is 0.12 after normalisation, then `instrument_weight_1 = 0.12`. So for the first asset, B&H uses that same number, e.g. **0.12**, in `position_1 = vol_scalar_1 × 0.12 × IDM`.

2. **IDM (instrument diversification multiplier)**  
   - Source: `portfolio.get_instrument_diversification_multiplier()` (estimated when `use_instrument_div_mult_estimates: True`).  
   - Formula (single period): **IDM = 1 / √(w′ C w)**, where **w** = vector of instrument weights, **C** = instrument **correlation** matrix (of subsystem returns). So **√(w′ C w)** is the portfolio “risk” in correlation space; IDM is its inverse, capped at `dm_max` (e.g. 2.5).  
   - Interpretation: if assets were uncorrelated and equal weight, √(w′ C w) ≈ 1/√n → IDM ≈ √n; if perfectly correlated, risk ≈ 1 → IDM ≈ 1.  
   - The system uses **instrument_div_mult_estimate** (e.g. `diversification_multiplier_from_list`): it uses the same **weights** and the **instrument correlation matrix** (from the same returns used for weight estimation), then EWMA‑smooths the resulting IDM series (e.g. `ewma_span: 125`).  
   - **Example:** 10 instruments, moderate correlation → `IDM = 1.40`.

So for the first asset, B&H uses that same **instrument_weight_1** (e.g. 0.12) and **IDM** (e.g. 1.40) as the rest of the portfolio; the only asset‑specific part of the B&H size is the **vol_scalar** for that instrument.

**Summary: strategy's B&H (vol and diversification adjusted, long only)**

Per instrument, the following is correct (with one fix):

- **Position in contracts** = **average_position** (not “average_position × capital / value_per_point”). Capital and “$ value of 1 point move” are already inside **vol_scalar**, so portfolio-level position in contracts is just **average_position**.
- **Capital** = whole portfolio notional capital.
- **1 point** = the contract’s price unit that **value_of_block_price_move** is defined for (e.g. 1 index point); not necessarily the smallest tick.
- **average_position** = **instrument_weight × IDM × vol_scalar** (all three are dimensionless or in contracts as below).
- **instrument_weight** = Handcraft (or other optimiser): uses **correlation** and **vol** to build risk weights; weights sum to 1.
- **IDM (Instrument Diversification Multiplier)** = **1 / √(w′ C w)**, capped at **dm_max = 2.5**. Here **w** = instrument weights, **C** = **correlation** matrix (of subsystem returns), not covariance.
- **vol_scalar** = **daily_cash_vol_target / instrument_value_vol** (result is in contracts; capital is inside daily_cash_vol_target).
- **instrument_value_vol** = **instrument_value_vol_b4_fx × fx** (vol in instrument currency × fx → base currency). And **instrument_value_vol_b4_fx** = **block_value × daily_perc_vol** (block_value = price × value_of_block_price_move × 0.01 = $ per 1% move per contract).

**Log files**

- Written to `examples/introduction/results/` with name:
  - `es_<YYYY-MM-DD_HH-MM-SS>.log`
- Includes:
  - Run timestamp and data period,
  - System Sharpe and detailed stats,
  - Instrument list,
  - Percent stats (with a note if margin cost is included),
  - Capital usage and leverage for the system and B&H portfolio,
  - Final estimated instrument weights,
  - B&H portfolio stats,
  - B&H benchmarks (1× notional SP500 / SPY_yfinance),
  - Total runtime.

**How to run**

From the repo root:

```bash
python examples/introduction/estimatedsystem.py
```

Recommended sequence for a fresh setup:

1. Make sure `data/custom/S&P_500_component_stocks*.csv` exists.
2. Run `examples/introduction/dl_yfinance.py` to fetch prices, yields, and funding cost.
3. Run `examples/introduction/add_instrument.py` to populate CSV config for any new `_yfinance` instruments.
4. Run `examples/introduction/estimatedsystem.py` to produce logs in `examples/introduction/results/`.

