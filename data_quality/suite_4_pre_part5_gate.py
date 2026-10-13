"""GATE 4 - the step that was missed: check the ANALYSIS INPUTS right before the Part 5 notebook.

Gates 1-3 check Suhana's tables. This gate checks what the notebook actually reads:
my mart_sharpe and the two views, plus the inputs of the breadth chart. It answers
"are the numbers I am about to chart built from today's data, with the agreed exclusions?"

On 11 Oct 2026 data, rule 'ctva_rows_in_views' is EXPECTED TO FAIL: the views still
contain CTVA (Materials 1Y average return 10.7% with it, 14.4% without). That is the
point of a gate - it would have caught this before the chart was drawn.

Run:  cd data_quality && python suite_4_pre_part5_gate.py
"""
import sys
from great_expectations import expectations as gxe
from gx_suite import run_suite, bq_df
from tables import FACT, MART, V_RANK, V_SECTOR, REVIEWED_EXTREME_MOVES

reviewed = ", ".join(f"'{t}'" for t in REVIEWED_EXTREME_MOVES)

g = bq_df(f"""
WITH eligible AS (
  SELECT * FROM {MART}
  WHERE (period = '5Y' AND trading_days >= 1000) OR (period = '1Y' AND trading_days >= 200)),
breadth AS (
  SELECT date, COUNTIF(sma_200 IS NOT NULL) AS stocks_with_sma
  FROM {FACT} GROUP BY date HAVING COUNTIF(sma_200 IS NOT NULL) > 0)
SELECT
  (SELECT COUNT(*) FROM {MART})                                         AS mart_rows,
  (SELECT COUNT(DISTINCT ticker) FROM {MART} WHERE period = '1Y')       AS mart_tickers_1y,
  (SELECT COUNT(DISTINCT ticker) FROM {MART} WHERE period = '5Y')       AS mart_tickers_5y,
  (SELECT COUNT(DISTINCT ticker) FROM {FACT})                           AS fact_tickers,
  (SELECT COUNTIF(sharpe IS NULL OR total_return IS NULL
               OR annual_volatility IS NULL) FROM {MART})               AS mart_nulls,
  (SELECT COUNTIF(sharpe < -5 OR sharpe > 5) FROM eligible)             AS implausible_sharpe,
  (SELECT COUNTIF(total_return <= -1) FROM {MART})                      AS loss_beyond_100pct,
  (SELECT COUNTIF(annual_volatility < 0.05 OR annual_volatility > 2.5)
     FROM eligible)                                                     AS implausible_vol,
  (SELECT COUNTIF(ticker = 'CTVA') FROM {V_RANK})                      AS ctva_rows_in_views,
  (SELECT COUNT(DISTINCT sector) FROM {V_SECTOR} WHERE period = '1Y')   AS sectors_1y,
  (SELECT COUNT(DISTINCT sector) FROM {V_SECTOR} WHERE period = '5Y')   AS sectors_5y,
  (SELECT MIN(stocks_with_sma) FROM breadth)                            AS min_stocks_with_sma,
  (SELECT COUNT(*) FROM breadth)                                        AS breadth_dates,
  (SELECT COUNTIF(ABS(daily_return) > 0.5 AND ticker NOT IN ({reviewed}))
     FROM {FACT})                                                       AS unreviewed_extreme_moves,
  (SELECT UNIX_SECONDS(MAX(last_modified)) FROM (
     SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_modified
     FROM `friendly-autumn-509313-n3.team2_marts.__TABLES__` WHERE table_id = 'mart_sharpe'))
  - (SELECT UNIX_SECONDS(MAX(last_modified)) FROM (
     SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_modified
     FROM `dsaimodule2-509312.proj2sp500.__TABLES__` WHERE table_id = 'fact_dailyprice'))
                                                                        AS mart_newer_than_fact_s
""")
print(g.T)

labels = {
    "mart_rows": "completeness", "mart_tickers_1y": "coverage", "mart_tickers_5y": "coverage",
    "mart_nulls": "completeness", "implausible_sharpe": "outliers",
    "loss_beyond_100pct": "financial reasonableness", "implausible_vol": "outliers",
    "ctva_rows_in_views": "agreed exclusion", "sectors_1y": "coverage", "sectors_5y": "coverage",
    "min_stocks_with_sma": "post-ELT (SMA-200)", "breadth_dates": "post-ELT (SMA-200)",
    "unreviewed_extreme_moves": "outliers", "mart_newer_than_fact_s": "freshness",
}

def between(col, lo, hi):
    return gxe.ExpectColumnValuesToBeBetween(column=col, min_value=lo, max_value=hi)

fact_tickers = int(g.loc[0, "fact_tickers"])
ok = run_suite(g, "pre_part5_gate", [
    between("mart_rows", 2 * fact_tickers, 2 * fact_tickers),    # one row per ticker per period
    between("mart_tickers_1y", fact_tickers, fact_tickers),
    between("mart_tickers_5y", fact_tickers, fact_tickers),
    between("mart_nulls", 0, 0),
    between("implausible_sharpe", 0, 0),
    between("loss_beyond_100pct", 0, 0),
    between("implausible_vol", 0, 0),
    between("ctva_rows_in_views", 0, 0),                          # fails until views are fixed
    between("sectors_1y", 11, 11),
    between("sectors_5y", 11, 11),
    between("min_stocks_with_sma", 480, 510),                     # today 491-500
    between("breadth_dates", 1000, 1300),                         # today 1,056 (1,255 - 199)
    between("unreviewed_extreme_moves", 0, 0),
    between("mart_newer_than_fact_s", 0, None),                   # mart rebuilt AFTER the fact
], labels)
sys.exit(0 if ok else 1)
