"""GATE 0 - the Parquet files Kenny's notebook just wrote (my v2 extract), BEFORE anything is loaded.

Question it answers: "is today's download complete and sane enough to load into BigQuery?"
Reads the newest dated folder under $SP500_V2_DIR/data (one Parquet file per year).
Same rules that caught problems in the team data: duplicates, impossible prices
(the 8 Oct intraday bar), missing tickers (Yahoo rate limits), stale last date.

Run:  cd data_quality && SP500_V2_DIR=/path/to/sp500_v2 python suite_0_parquet_extract.py
"""
import os
import sys
from pathlib import Path

import pandas as pd
from great_expectations import expectations as gxe
from gx_suite import run_suite

V2 = Path(os.environ.get("SP500_V2_DIR", "/mnt/c/Users/neote/projects/sp500_v2"))
run_dir = sorted(p for p in (V2 / "data").iterdir() if p.is_dir())[-1]     # newest dated folder
files = sorted(run_dir.glob("sp500_panel_*.parquet"))
print(f"Checking {len(files)} Parquet files in {run_dir}")
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

last = df.groupby("ticker")["date"].max()
p = df.dropna(subset=["open", "high", "low", "close"])
metrics = pd.DataFrame([{
    "n_files":              len(files),
    "n_rows":               len(df),
    "tickers":              df["ticker"].nunique(),
    "tickers_behind":       int((last < last.max()).sum()),
    "duplicate_keys":       int(df.duplicated(["ticker", "date"]).sum()),
    "null_close":           int(df["close"].isna().sum()),
    "bad_prices":           int(((p["close"] <= 0) | (p["high"] < p["low"]) |
                                 (p["open"] > p["high"]) | (p["open"] < p["low"]) |
                                 (p["close"] > p["high"]) | (p["close"] < p["low"])).sum()),
    "days_since_last_date": (pd.Timestamp.today().normalize() - df["date"].max().normalize()).days,
}])
print(f"last date in files: {df['date'].max():%Y-%m-%d}")
print(metrics.T)

labels = {"n_files": "completeness", "n_rows": "coverage", "tickers": "coverage",
          "tickers_behind": "completeness (Yahoo rate limits)", "duplicate_keys": "uniqueness",
          "null_close": "completeness", "bad_prices": "validity (intraday bar)",
          "days_since_last_date": "timeliness"}
rules = [
    gxe.ExpectColumnValuesToBeBetween(column="n_files", min_value=5, max_value=7),
    gxe.ExpectColumnValuesToBeBetween(column="n_rows", min_value=600_000, max_value=700_000),
    gxe.ExpectColumnValuesToBeBetween(column="tickers", min_value=495, max_value=510),
    gxe.ExpectColumnValuesToBeBetween(column="tickers_behind", min_value=0, max_value=5),
    gxe.ExpectColumnValuesToBeBetween(column="days_since_last_date", min_value=0, max_value=4),
]
for col in ["duplicate_keys", "null_close", "bad_prices"]:
    rules.append(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=0))

ok = run_suite(metrics, "parquet_extract_suite", rules, labels)
sys.exit(0 if ok else 1)
