-- Sharpe mart: one row per ticker per period (5Y, 1Y), read from Suhana's fact table.
-- Rebuild after every fact_dailyprice rebuild (GATE 4 checks the order).
CREATE OR REPLACE TABLE `friendly-autumn-509313-n3.team2_marts.mart_sharpe` AS
WITH r AS (
  SELECT ticker, date, daily_return
  FROM `dsaimodule2-509312.proj2sp500.fact_dailyprice`
  WHERE daily_return IS NOT NULL
),
bounds AS (SELECT MAX(date) AS max_date FROM r),
periods AS (
  SELECT p.period, p.start_date, b.max_date
  FROM bounds b, UNNEST([
    STRUCT('5Y' AS period, DATE_SUB(b.max_date, INTERVAL 5 YEAR) AS start_date),
    STRUCT('1Y', DATE_SUB(b.max_date, INTERVAL 1 YEAR))]) AS p
)
SELECT
  r.ticker,
  p.period,
  COUNT(*)                                             AS trading_days,
  EXP(SUM(LN(1 + r.daily_return))) - 1                 AS total_return,
  STDDEV_SAMP(r.daily_return) * SQRT(252)              AS annual_volatility,
  SAFE_DIVIDE(AVG(r.daily_return) * 252 - 0.04,
              STDDEV_SAMP(r.daily_return) * SQRT(252)) AS sharpe
FROM r
JOIN periods p ON r.date > p.start_date AND r.date <= p.max_date
GROUP BY r.ticker, p.period;
