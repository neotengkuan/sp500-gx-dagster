"""
download_sp500.py  ·  Stage 1 (Extract) of the S&P 500 replication

What it does
  1. Reads today's S&P 500 member list from Wikipedia (ticker, company, GICS sector,
     sub-industry, date added, CIK ...)
  2. Downloads 5 years of daily prices from Yahoo Finance for every member
     (same window as the team file: 2021-10-06 to 2026-10-06)

Outputs (in data/raw/)
  sp500_constituents.csv   one row per company   (~503 rows)
  sp500_prices.parquet     one row per ticker per trading day   (~620k rows)
  sp500_download_log.csv   per ticker: rows, first and last date, or the problem

Run:   conda activate elt
       python download_sp500.py
"""
import io
import os
import time
import datetime as dt
import pathlib

import pandas as pd
import requests
import yfinance as yf

# Fixed class window by default; the daily Dagster run sets ROLLING=1 for "the last 5 years to today"
if os.environ.get("ROLLING") == "1":
    END = (dt.date.today() + dt.timedelta(days=1)).isoformat()      # end is exclusive
    START = (dt.date.today() - dt.timedelta(days=5 * 365 + 2)).isoformat()
else:
    START = "2021-10-06"
    END = "2026-10-07"      # yfinance's end date is exclusive -> last row is 2026-10-06
BATCH = 50                  # tickers per Yahoo request
OUT = pathlib.Path("data/raw")
OUT.mkdir(parents=True, exist_ok=True)
TODAY = dt.date.today().isoformat()
WIKI = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

PRICE_COLS = {"Date": "date", "Open": "open", "High": "high", "Low": "low",
              "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
              "Dividends": "dividends", "Stock Splits": "stock_splits"}


# ---------------------------------------------------------------- 1. member list
def get_constituents() -> pd.DataFrame:
    # Wikipedia refuses requests without a browser-like User-Agent
    html = requests.get(WIKI, headers={"User-Agent": "Mozilla/5.0 (class project)"},
                        timeout=30).text
    t = pd.read_html(io.StringIO(html), attrs={"id": "constituents"})[0]
    t = t.rename(columns={"Symbol": "ticker", "Security": "company",
                          "GICS Sector": "sector", "GICS Sub-Industry": "sub_industry",
                          "Headquarters Location": "hq_location", "Date added": "date_added",
                          "CIK": "cik", "Founded": "founded"})
    t["yahoo_ticker"] = t["ticker"].str.replace(".", "-", regex=False)   # BRK.B -> BRK-B
    t["cik"] = pd.to_numeric(t["cik"], errors="coerce").astype("Int64")
    t["date_added"] = pd.to_datetime(t["date_added"], errors="coerce").dt.date
    t["founded"] = t["founded"].astype(str)                              # e.g. '1902 (1888)'
    t["download_date"] = TODAY
    return t[["ticker", "yahoo_ticker", "company", "sector", "sub_industry",
              "hq_location", "date_added", "cik", "founded", "download_date"]]


# ---------------------------------------------------------------- 2. prices
def tidy(df: pd.DataFrame, yahoo_ticker: str) -> pd.DataFrame:
    """One ticker's Yahoo frame -> long rows with our column names."""
    df = df.reset_index().rename(columns=PRICE_COLS)
    df = df[[c for c in PRICE_COLS.values() if c in df.columns]]   # drops e.g. Capital Gains
    df = df.dropna(subset=["close"])                                # days before listing
    df["yahoo_ticker"] = yahoo_ticker
    return df


def get_prices(yahoo_tickers: list[str]) -> tuple[pd.DataFrame, list[str]]:
    frames, missing = [], []
    for i in range(0, len(yahoo_tickers), BATCH):
        chunk = yahoo_tickers[i:i + BATCH]
        print(f"prices batch {i // BATCH + 1}: {chunk[0]} ... {chunk[-1]}")
        raw = yf.download(chunk, start=START, end=END, auto_adjust=False, actions=True,
                          group_by="ticker", threads=True, progress=False)
        for t in chunk:
            try:
                d = tidy(raw[t], t)
            except KeyError:
                d = pd.DataFrame()
            if d.empty:
                missing.append(t)
            else:
                frames.append(d)
        time.sleep(1)                                               # be polite to Yahoo

    # one retry, one ticker at a time, for anything that came back empty
    still_missing = []
    for t in missing:
        print(f"retry {t}")
        raw = yf.download(t, start=START, end=END, auto_adjust=False, actions=True,
                          group_by="ticker", progress=False)
        try:
            d = tidy(raw[t], t)
        except KeyError:
            d = pd.DataFrame()
        (frames.append(d) if not d.empty else still_missing.append(t))
        time.sleep(1)
    return pd.concat(frames, ignore_index=True), still_missing


# ---------------------------------------------------------------- main
if __name__ == "__main__":
    members = get_constituents()
    members.to_csv(OUT / "sp500_constituents.csv", index=False)
    print(f"Constituents: {len(members)} rows, {members['sector'].nunique()} sectors")

    prices, failed = get_prices(members["yahoo_ticker"].tolist())
    prices = prices.merge(members[["yahoo_ticker", "ticker"]], on="yahoo_ticker", how="left")
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    prices["volume"] = prices["volume"].astype("Int64")
    prices["download_date"] = TODAY
    prices = prices[["date", "ticker", "yahoo_ticker", "open", "high", "low", "close",
                     "adj_close", "volume", "dividends", "stock_splits", "download_date"]]
    prices.to_parquet(OUT / "sp500_prices.parquet", index=False)

    log = (prices.groupby("ticker")
                 .agg(rows=("date", "size"), first_date=("date", "min"), last_date=("date", "max"))
                 .reset_index())
    log = pd.concat([log, pd.DataFrame({"ticker": failed, "rows": 0})], ignore_index=True)
    log.to_csv(OUT / "sp500_download_log.csv", index=False)

    print(f"\nPrices: {len(prices):,} rows, {prices['ticker'].nunique()} tickers, "
          f"{prices['date'].min()} -> {prices['date'].max()}")
    print(f"Failed after retry: {failed or 'none'}")
    print("\nShortest histories (late listings):")
    print(log.sort_values("rows").head(12).to_string(index=False))
