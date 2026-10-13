"""GATE 3 - Michael's 14 data-quality dimensions as one GX suite on Suhana's star schema.

BigQuery computes ONE row of counts (cheap: one scan), GX judges each count.
Two suites:
  critical -> must pass, or the pipeline halts (nothing downstream runs on bad data)
  warning  -> known, explained issues; reported every run but do not halt

Dimension (Michael)              Metric column(s) below
 1 Source integrity              rows_fact_minus_raw
 2 Schema validity               fact_columns           (full contract: suite_2)
 3 Completeness                  null_core, share_null_mkt_cap
 4 Uniqueness                    dup_ticker_date, dup_dim_ticker
 5 Price validity                bad_prices
 6 Volume validity               negative_volume, zero_volume_days
 7 Cross-field reconciliation    share_mktcap_mismatch, share_pe_mismatch
 8 Financial reasonableness      mkt_cap_above_10tn, mkt_cap_below_1bn
 9 Outliers                      unreviewed_extreme_moves
10 Temporal consistency          weekend_rows, lookahead_rows
11 Snapshot consistency          tickers_multi_date_added
12 Coverage                      tickers_last_day, sectors, tickers_with_gaps
13 Referential integrity         orphan_tickers
14 Post-ELT validation           daily_return_mismatch, sma200_wrong_nulls

Run:  cd data_quality && python suite_3_michael_14_dimensions.py
"""
import sys
from great_expectations import expectations as gxe
from gx_suite import run_suite, bq_df
from tables import RAW, DIM, FACT, INFO, REVIEWED_EXTREME_MOVES

reviewed = ", ".join(f"'{t}'" for t in REVIEWED_EXTREME_MOVES)

m = bq_df(f"""
WITH f AS (SELECT * FROM {FACT}),
lagged AS (
  SELECT ticker, date, daily_return, sma_200,
         adj_close / LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS ret_check,
         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date)                AS day_no
  FROM f),
per_ticker AS (
  SELECT ticker, COUNT(*) AS days, COUNT(DISTINCT date_added) AS n_date_added,
         MIN(date) AS first_d, MAX(date) AS last_d
  FROM f GROUP BY ticker),
cal AS (SELECT DISTINCT date FROM f),
gaps AS (
  SELECT p.ticker, p.days,
         (SELECT COUNT(*) FROM cal WHERE cal.date BETWEEN p.first_d AND p.last_d) AS expected
  FROM per_ticker p)
SELECT
  (SELECT COUNT(*) FROM f) - (SELECT COUNT(*) FROM {RAW})                       AS rows_fact_minus_raw,
  (SELECT COUNT(*) FROM {INFO} WHERE table_name = 'fact_dailyprice')             AS fact_columns,
  (SELECT COUNTIF(close IS NULL OR adj_close IS NULL OR volume IS NULL) FROM f)  AS null_core,
  (SELECT ROUND(COUNTIF(mkt_cap IS NULL) / COUNT(*), 4) FROM f)                  AS share_null_mkt_cap,
  (SELECT COUNT(*) - COUNT(DISTINCT FORMAT('%s|%t', ticker, date)) FROM f)       AS dup_ticker_date,
  (SELECT COUNT(*) - COUNT(DISTINCT ticker) FROM {DIM})                          AS dup_dim_ticker,
  (SELECT COUNTIF(close <= 0 OR adj_close <= 0 OR high < low
               OR open > high OR open < low OR close > high OR close < low) FROM f) AS bad_prices,
  (SELECT COUNTIF(volume < 0) FROM f)                                            AS negative_volume,
  (SELECT COUNTIF(volume = 0) FROM f)                                            AS zero_volume_days,
  (SELECT ROUND(SAFE_DIVIDE(COUNTIF(ABS(SAFE_DIVIDE(mkt_cap, close * shares_out) - 1) > 0.01),
                            COUNTIF(mkt_cap IS NOT NULL)), 4) FROM f)            AS share_mktcap_mismatch,
  (SELECT ROUND(SAFE_DIVIDE(COUNTIF(eps_diluted_ttm > 0 AND pe IS NOT NULL
                   AND ABS(SAFE_DIVIDE(pe, close / eps_diluted_ttm) - 1) > 0.01),
                            COUNTIF(pe IS NOT NULL)), 4) FROM f)                 AS share_pe_mismatch,
  (SELECT COUNTIF(mkt_cap > 10e12) FROM f)                                       AS mkt_cap_above_10tn,
  (SELECT COUNTIF(mkt_cap < 1e9) FROM f)                                         AS mkt_cap_below_1bn,
  (SELECT COUNTIF(ABS(daily_return) > 0.5 AND ticker NOT IN ({reviewed})) FROM f) AS unreviewed_extreme_moves,
  (SELECT COUNTIF(EXTRACT(DAYOFWEEK FROM date) IN (1, 7)) FROM f)                AS weekend_rows,
  (SELECT COUNTIF(fund_filed > date) FROM f)                                     AS lookahead_rows,
  (SELECT COUNTIF(n_date_added > 1) FROM per_ticker)                             AS tickers_multi_date_added,
  (SELECT COUNT(DISTINCT ticker) FROM f WHERE date = (SELECT MAX(date) FROM f))  AS tickers_last_day,
  (SELECT COUNT(DISTINCT sector) FROM {DIM})                                     AS sectors,
  (SELECT COUNTIF(days < expected) FROM gaps)                                    AS tickers_with_gaps,
  (SELECT COUNT(*) FROM (SELECT DISTINCT ticker FROM f) x
     LEFT JOIN {DIM} d USING (ticker) WHERE d.ticker IS NULL)                    AS orphan_tickers,
  (SELECT COUNTIF(day_no > 1 AND ABS(daily_return - ret_check) > 1e-6) FROM lagged) AS daily_return_mismatch,
  (SELECT COUNTIF((day_no <= 199 AND sma_200 IS NOT NULL)
               OR (day_no >= 200 AND sma_200 IS NULL)) FROM lagged)             AS sma200_wrong_nulls
""")
print(m.T)

DIM_OF = {
    "rows_fact_minus_raw": "1 source integrity", "fact_columns": "2 schema validity",
    "null_core": "3 completeness", "share_null_mkt_cap": "3 completeness",
    "dup_ticker_date": "4 uniqueness", "dup_dim_ticker": "4 uniqueness",
    "bad_prices": "5 price validity", "negative_volume": "6 volume validity",
    "zero_volume_days": "6 volume validity", "share_mktcap_mismatch": "7 cross-field",
    "share_pe_mismatch": "7 cross-field", "mkt_cap_above_10tn": "8 financial reasonableness",
    "mkt_cap_below_1bn": "8 financial reasonableness", "unreviewed_extreme_moves": "9 outliers",
    "weekend_rows": "10 temporal", "lookahead_rows": "10 temporal",
    "tickers_multi_date_added": "11 snapshot consistency", "tickers_last_day": "12 coverage",
    "sectors": "12 coverage", "tickers_with_gaps": "12 coverage",
    "orphan_tickers": "13 referential integrity", "daily_return_mismatch": "14 post-ELT",
    "sma200_wrong_nulls": "14 post-ELT",
}

def between(col, lo, hi):
    return gxe.ExpectColumnValuesToBeBetween(column=col, min_value=lo, max_value=hi)

# ---- critical: a failure here means the data is not safe to analyse
critical = [
    between("rows_fact_minus_raw", 0, 0),       # ELT lost or invented no rows
    between("fact_columns", 41, 41),
    between("null_core", 0, 0),
    between("dup_ticker_date", 0, 0),
    between("dup_dim_ticker", 0, 0),
    between("bad_prices", 0, 0),
    between("negative_volume", 0, 0),
    between("unreviewed_extreme_moves", 0, 0),  # a NEW >50% day must be looked at first
    between("weekend_rows", 0, 0),
    between("lookahead_rows", 0, 0),            # no fundamentals used before they were filed
    between("tickers_multi_date_added", 0, 0),
    between("tickers_last_day", 495, 510),
    between("sectors", 11, 11),
    between("orphan_tickers", 0, 0),
    between("daily_return_mismatch", 0, 0),
    between("sma200_wrong_nulls", 0, 0),
]
run_suite(m, "michael14_critical_suite", critical, DIM_OF, halt=True)

# ---- warning: known issues waiting for Kenny's corrected file (report, do not halt)
warning = [
    between("share_null_mkt_cap", 0, 0.03),     # today 13,531 rows = 2.2%
    between("zero_volume_days", 0, 500),        # today 327
    between("share_mktcap_mismatch", 0, 0.01),
    between("share_pe_mismatch", 0, 0.01),
    between("mkt_cap_above_10tn", 0, 0),        # Waters x1000 = $43 trillion
    between("mkt_cap_below_1bn", 0, 0),         # McDonald's 709 shares = $165 thousand
    between("tickers_with_gaps", 0, 0),         # PSKY: 7 rows across ~50 trading days
]
ok = run_suite(m, "michael14_warning_suite", warning, DIM_OF, halt=False)
sys.exit(0)        # warnings never stop the pipeline; read the FAIL lines in the log
