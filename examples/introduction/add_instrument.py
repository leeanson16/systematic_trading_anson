"""
Auto-scan adjusted_prices_csv for new instruments and add missing ones
to the csvconfig files (instrumentconfig.csv, rollconfig.csv, spreadcosts.csv).

Scans:  data/futures/adjusted_prices_csv/*.csv
Checks: data/futures/csvconfig/instrumentconfig.csv
Action: Adds rows for any instrument found in prices but missing from config.

Defaults for new instruments are set based on the _yfinance suffix convention:
  - _yfinance stocks:  Equity, Pointsize=1, Percentage=0.0004, SpreadCost=0.00
  - other instruments: defaults below (edit DEFAULTS dict to change)

Usage:
    python -m examples.introduction.add_instrument
    python examples/introduction/add_instrument.py
    python examples/introduction/add_instrument.py --dry-run
"""

import os
import csv
import sys
import glob

# ─── REPO PATHS ─────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
CSVCONFIG_DIR = os.path.join(_REPO_ROOT, "data", "futures", "csvconfig")
ADJUSTED_PRICES_DIR = os.path.join(_REPO_ROOT, "data", "futures", "adjusted_prices_csv")

INSTRUMENT_CONFIG_PATH = os.path.join(CSVCONFIG_DIR, "instrumentconfig.csv")
ROLL_CONFIG_PATH = os.path.join(CSVCONFIG_DIR, "rollconfig.csv")
SPREAD_COSTS_PATH = os.path.join(CSVCONFIG_DIR, "spreadcosts.csv")

# ─── DEFAULTS FOR NEW INSTRUMENTS ───────────────────────────────────────────
# _yfinance stocks use these values (matching the existing convention)
YFINANCE_DEFAULTS = {
    "instrumentconfig": {
        "Pointsize": 1,
        "Currency": "USD",
        "AssetClass": "Equity",
        "PerBlock": 0,
        "Percentage": 0.0004,
        "PerTrade": 0,
        "Region": "US",
    },
    "rollconfig": {
        "HoldRollCycle": "H",
        "RollOffsetDays": -999,
        "CarryOffset": 0,
        "PricedRollCycle": "H",
        "ExpiryOffset": 0,
    },
    "spreadcosts": {
        "SpreadCost": 0.00,
    },
}

# Non-yfinance instruments that somehow have price data but no config
# (unlikely — override per-instrument if needed)
OTHER_DEFAULTS = {
    "instrumentconfig": {
        "Pointsize": 1,
        "Currency": "USD",
        "AssetClass": "Unknown",
        "PerBlock": 0,
        "Percentage": 0,
        "PerTrade": 0,
        "Region": "US",
    },
    "rollconfig": {
        "HoldRollCycle": "HMUZ",
        "RollOffsetDays": -5,
        "CarryOffset": 1,
        "PricedRollCycle": "HMUZ",
        "ExpiryOffset": 15,
    },
    "spreadcosts": {
        "SpreadCost": 0.0,
    },
}


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _read_csv(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)
    return header, rows


def _write_csv_sorted(path, header, rows):
    rows_sorted = sorted(rows, key=lambda r: r[header[0]])
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        for row in rows_sorted:
            writer.writerow(row)


def _existing_instruments(rows, key="Instrument"):
    return {row[key] for row in rows}


def _scan_adjusted_prices():
    """Return set of instrument codes found in adjusted_prices_csv."""
    pattern = os.path.join(ADJUSTED_PRICES_DIR, "*.csv")
    codes = set()
    for filepath in glob.glob(pattern):
        filename = os.path.basename(filepath)
        code = filename.replace(".csv", "")
        codes.add(code)
    return codes


def _description_for_code(code):
    if code.endswith("_yfinance"):
        ticker = code.replace("_yfinance", "")
        return "%s (yfinance)" % ticker
    if code.endswith("_yfinance_unadj"):
        ticker = code.replace("_yfinance_unadj", "")
        return "%s (yfinance unadjusted)" % ticker
    if code.endswith("_yfinance_adj"):
        ticker = code.replace("_yfinance_adj", "")
        return "%s (yfinance adjusted)" % ticker
    return code


def _defaults_for_code(code):
    if "yfinance" in code:
        return YFINANCE_DEFAULTS
    return OTHER_DEFAULTS


def _build_entry(code):
    defaults = _defaults_for_code(code)
    return {
        "instrumentconfig": {
            "Instrument": code,
            "Description": _description_for_code(code),
            **defaults["instrumentconfig"],
        },
        "rollconfig": {
            "Instrument": code,
            **defaults["rollconfig"],
        },
        "spreadcosts": {
            "Instrument": code,
            **defaults["spreadcosts"],
        },
    }


def add_instruments(dry_run=False):
    for path in [INSTRUMENT_CONFIG_PATH, ROLL_CONFIG_PATH, SPREAD_COSTS_PATH]:
        if not os.path.exists(path):
            print("ERROR: %s not found" % path)
            sys.exit(1)

    if not os.path.isdir(ADJUSTED_PRICES_DIR):
        print("ERROR: %s not found" % ADJUSTED_PRICES_DIR)
        sys.exit(1)

    price_codes = _scan_adjusted_prices()
    print("Found %d instrument(s) in adjusted_prices_csv" % len(price_codes))

    ic_header, ic_rows = _read_csv(INSTRUMENT_CONFIG_PATH)
    rc_header, rc_rows = _read_csv(ROLL_CONFIG_PATH)
    sc_header, sc_rows = _read_csv(SPREAD_COSTS_PATH)

    ic_existing = _existing_instruments(ic_rows)
    rc_existing = _existing_instruments(rc_rows)
    sc_existing = _existing_instruments(sc_rows)

    missing = sorted(price_codes - ic_existing)
    print("Missing from csvconfig: %d instrument(s)" % len(missing))

    if not missing:
        print("Nothing to add — all instruments already configured.")
        return

    added = []
    for code in missing:
        entry = _build_entry(code)

        if dry_run:
            print("  [DRY-RUN] would add: %s" % code)
            added.append(code)
            continue

        ic_rows.append(entry["instrumentconfig"])
        if code not in rc_existing:
            rc_rows.append(entry["rollconfig"])
            rc_existing.add(code)
        if code not in sc_existing:
            sc_rows.append(entry["spreadcosts"])
            sc_existing.add(code)
        added.append(code)

    if added and not dry_run:
        _write_csv_sorted(INSTRUMENT_CONFIG_PATH, ic_header, ic_rows)
        _write_csv_sorted(ROLL_CONFIG_PATH, rc_header, rc_rows)
        _write_csv_sorted(SPREAD_COSTS_PATH, sc_header, sc_rows)

    print("\n%s %d instrument(s):" % ("Would add" if dry_run else "ADDED", len(added)))
    for code in added:
        print("  %s" % code)


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    add_instruments(dry_run=dry_run)
