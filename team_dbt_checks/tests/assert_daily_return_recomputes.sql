-- Post-ELT (dim 14): daily_return equals adj_close / previous adj_close - 1.
WITH x AS (
  SELECT ticker, date, daily_return,
         adj_close / LAG(adj_close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS check_return
  FROM {{ source('proj2sp500', 'fact_dailyprice') }})
SELECT * FROM x
WHERE check_return IS NOT NULL AND ABS(daily_return - check_return) > 1e-6
