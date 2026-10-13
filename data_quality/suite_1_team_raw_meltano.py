"""GATE 1 - the raw table Suhana's Meltano run loaded (Kenny's CSV -> public_proj2_sp500).

Runs BEFORE any transformation. Question it answers: "did the load bring in a complete,
readable file?"  The raw columns arrived mostly as STRING, so every check uses SAFE_CAST:
a value that cannot be read as a number or a date becomes NULL and is counted.

Run:  cd data_quality && python suite_1_team_raw_meltano.py
"""
import sys
from great_expectations import expectations as gxe
from gx_suite import run_suite, bq_df
from tables import RAW

FRESHNESS_DAYS = None     # None for the fixed class file; 4 when the load runs every day

metrics = bq_df(f"""
SELECT
  COUNT(*)                                                         AS n_rows,
  COUNT(DISTINCT ticker)                                           AS tickers,
  COUNTIF(SAFE_CAST(date AS DATE) IS NULL)                         AS unreadable_dates,
  COUNTIF(SAFE_CAST(close AS FLOAT64) IS NULL
       OR SAFE_CAST(adj_close AS FLOAT64) IS NULL)                 AS unreadable_prices,
  COUNTIF(SAFE_CAST(volume AS FLOAT64) IS NULL)                    AS unreadable_volume,
  COUNTIF(ticker IS NULL OR TRIM(ticker) = '')                     AS blank_tickers,
  (SELECT COUNT(*) FROM (SELECT ticker, date FROM {RAW}
                         GROUP BY ticker, date HAVING COUNT(*) > 1)) AS duplicate_keys,
  DATE_DIFF(CURRENT_DATE(), MAX(SAFE_CAST(date AS DATE)), DAY)    AS days_since_last_row,
  COUNT(DISTINCT _sdc_batched_at)                                  AS meltano_batches
FROM {RAW}
""")
print(metrics.T)

labels = {"n_rows": "coverage", "tickers": "coverage", "unreadable_dates": "schema validity",
          "unreadable_prices": "schema validity", "unreadable_volume": "schema validity",
          "blank_tickers": "completeness", "duplicate_keys": "uniqueness",
          "days_since_last_row": "timeliness", "meltano_batches": "source integrity"}

rules = [
    gxe.ExpectColumnValuesToBeBetween(column="n_rows", min_value=600_000, max_value=700_000),
    gxe.ExpectColumnValuesToBeBetween(column="tickers", min_value=495, max_value=510),
    gxe.ExpectColumnValuesToBeBetween(column="meltano_batches", min_value=1, max_value=1),
]
for col in ["unreadable_dates", "unreadable_prices", "unreadable_volume",
            "blank_tickers", "duplicate_keys"]:
    rules.append(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=0))
if FRESHNESS_DAYS is not None:
    rules.append(gxe.ExpectColumnValuesToBeBetween(column="days_since_last_row",
                                                   min_value=0, max_value=FRESHNESS_DAYS))

ok = run_suite(metrics, "team_raw_meltano_suite", rules, labels)
sys.exit(0 if ok else 1)
