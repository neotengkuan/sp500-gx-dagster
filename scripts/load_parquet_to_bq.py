"""Load the newest Parquet folder (Kenny's notebook output) into MY OWN BigQuery table.

Target: friendly-autumn-509313-n3.sp500_v2.panel_daily  (never the team project)
  - partitioned by month on date, clustered by ticker (cheap date-range and per-stock queries)
  - WRITE_TRUNCATE: Kenny's notebook is a rolling 5-year full refresh, so each run replaces the table
    (next step: switch to an incremental MERGE on (ticker, date))
Runs only after GATE 0 passed. Checks the row count in BigQuery equals the files.
"""
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

TABLE = "friendly-autumn-509313-n3.sp500_v2.panel_daily"
V2 = Path(os.environ.get("SP500_V2_DIR", "/mnt/c/Users/neote/projects/sp500_v2"))
run_dir = sorted(p for p in (V2 / "data").iterdir() if p.is_dir())[-1]
files = sorted(run_dir.glob("sp500_panel_*.parquet"))
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
for c in ["date", "fiscal_period_end", "fund_filed", "date_added"]:      # store as DATE, not TIMESTAMP
    if c in df.columns:
        df[c] = pd.to_datetime(df[c]).dt.date
print(f"Loading {len(df):,} rows from {len(files)} files in {run_dir} -> {TABLE}")

client = bigquery.Client(project="friendly-autumn-509313-n3", location="US")
job = client.load_table_from_dataframe(df, TABLE, job_config=bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field="date"),
    clustering_fields=["ticker"],
))
job.result()

n_bq = list(client.query(f"SELECT COUNT(*) n, MAX(date) d, COUNT(DISTINCT ticker) t FROM `{TABLE}`").result())[0]
print(f"BigQuery now has {n_bq.n:,} rows, {n_bq.t} tickers, last date {n_bq.d}")
if n_bq.n != len(df):
    raise SystemExit(f"Row count mismatch: files {len(df):,} vs BigQuery {n_bq.n:,}")
print("Load check passed")
