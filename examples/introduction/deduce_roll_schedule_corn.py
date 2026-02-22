"""
Deduce roll schedule for CORN from multiple_prices (and optionally adjusted_prices).

Roll dates = dates where PRICE_CONTRACT increases (front contract change).
Uses the same logic as sysinit.futures.build_roll_calendars.back_out_roll_calendar_from_multiple_prices.
Output: CSV with DATE_TIME, current_contract, next_contract, carry_contract.

Uses only standard library. Run from project root:
  python examples/introduction/deduce_roll_schedule.py
Or call main() with explicit paths.
"""

import csv
import os


def _contract_str(val):
    """Normalise contract code to 8-char date_str (yyyymm00 or yyyymmdd)."""
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return ""
    try:
        s = str(int(float(val)))
    except (ValueError, TypeError):
        return ""
    if len(s) == 6:
        return s + "00"
    return s


def back_out_roll_calendar_from_multiple_prices_path(multi_csv_path: str) -> list:
    """
    Read multiple_prices CSV and return list of (date_time, current_contract, next_contract, carry_contract).
    """
    rows = []
    with open(multi_csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            dt = row.get("DATETIME", "").strip()
            pc = row.get("PRICE_CONTRACT", "").strip() or None
            cc = row.get("CARRY_CONTRACT", "").strip() or None
            fc = row.get("FORWARD_CONTRACT", "").strip() or None
            rows.append((dt, pc, cc, fc))

    # Deduplicate by date: keep last occurrence per date (same as keep="last")
    by_date = {}
    for dt, pc, cc, fc in rows:
        by_date[dt] = (pc, cc, fc)

    dates_sorted = sorted(by_date.keys())
    result = []

    for i in range(1, len(dates_sorted)):
        d_prev = dates_sorted[i - 1]
        d_curr = dates_sorted[i]
        pc_prev, cc_prev, fc_prev = by_date[d_prev]
        pc_curr, cc_curr, fc_curr = by_date[d_curr]

        if pc_curr and pc_prev and int(float(pc_curr)) > int(float(pc_prev)):
            result.append(
                (
                    d_curr,
                    _contract_str(pc_prev),
                    _contract_str(pc_curr),
                    _contract_str(cc_prev),
                )
            )

    # Extra row: last date
    if dates_sorted:
        last_d = dates_sorted[-1]
        pc_last, cc_last, fc_last = by_date[last_d]
        if fc_last and _contract_str(fc_last):
            result.append(
                (last_d, _contract_str(pc_last), _contract_str(fc_last), _contract_str(cc_last))
            )

    return result


def main(
    multiple_prices_csv: str = None,
    adjusted_prices_csv: str = None,
    output_csv: str = None,
):
    if multiple_prices_csv is None:
        multiple_prices_csv = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "..", "data", "futures", "multiple_prices_csv", "CORN.csv",
        )
    if output_csv is None:
        results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(results_dir, exist_ok=True)
        output_csv = os.path.join(results_dir, "CORN_roll_deduced.csv")

    roll_rows = back_out_roll_calendar_from_multiple_prices_path(multiple_prices_csv)

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["DATE_TIME", "current_contract", "next_contract", "carry_contract"])
        w.writerows(roll_rows)

    print("Deduced roll schedule written to %s" % output_csv)
    print("Total roll dates: %d" % len(roll_rows))
    print("First 10:")
    for row in roll_rows[:10]:
        print("  ", row)
    print("  ...")
    print("Last 5:")
    for row in roll_rows[-5:]:
        print("  ", row)

    if adjusted_prices_csv and os.path.isfile(adjusted_prices_csv):
        with open(adjusted_prices_csv, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            lines = list(r)
        if lines:
            print("\nAdjusted prices: %d rows, from %s to %s" % (
                len(lines), lines[0].get("DATETIME", ""), lines[-1].get("DATETIME", "")))

    return roll_rows


if __name__ == "__main__":
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    multi_path = os.path.join(base, "data", "futures", "multiple_prices_csv", "CORN.csv")
    adj_path = os.path.join(base, "data", "futures", "adjusted_prices_csv", "CORN.csv")
    main(multiple_prices_csv=multi_path, adjusted_prices_csv=adj_path)
