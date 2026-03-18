"""
Wrapper: call dl_yfinance.main() to download ^GSPC, SPY and Fed (FRED) data.
New outputs live under pysystemtradeanson/data/custom.
"""

from dl_yfinance import main


if __name__ == "__main__":
    main()

