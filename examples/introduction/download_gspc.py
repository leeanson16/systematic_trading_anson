"""
Legacy wrapper: call d_yfinance.main() to download ^GSPC, SPY and Fed funds data.
New outputs live under pysystemtradeanson/data/my yfinance.
"""

from d_yfinance import main

if __name__ == "__main__":
    main()
