"""
Build watchlist.csv from Wikipedia's S&P 500 list.
Uses requests for the HTTP call (avoids macOS urllib SSL cert issues).
"""

import pandas as pd
import requests

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

SUFFIXES = [
    ", Inc.", " Inc.", ", Inc", " Inc",
    " Corporation", " Corp.", " Corp",
    " Company", " Co.", " Co",
    " plc", " PLC", " Ltd.", " Ltd",
    " Holdings, Inc.", " Holdings", ", The",
    " Group, Inc.", " Group",
]


def clean_name(name):
    name = name.strip()
    for suffix in SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name


def main():
    # Fetch with requests (handles SSL via certifi), then parse with pandas
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    response.raise_for_status()

    tables = pd.read_html(response.text)
    sp500 = tables[0]

    df = sp500[["Symbol", "Security", "GICS Sector"]].copy()
    df.columns = ["Ticker", "Company", "Sector"]
    df["Company"] = df["Company"].apply(clean_name)

    df.to_csv("watchlist.csv", index=False)
    print(f"Wrote {len(df)} tickers to watchlist.csv")
    print("\nFirst 10 entries:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()