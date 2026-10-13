-- Sector view: exact medians (PERCENTILE_CONT), same exclusions as the ranking view.
CREATE OR REPLACE VIEW `friendly-autumn-509313-n3.team2_marts.v_sector_sharpe` AS
WITH base AS (
  SELECT d.sector, s.period, s.sharpe, s.total_return
  FROM `friendly-autumn-509313-n3.team2_marts.mart_sharpe` s
  JOIN `dsaimodule2-509312.proj2sp500.dim_ticker` d USING (ticker)
  WHERE s.ticker != 'CTVA'
    AND ((s.period = '5Y' AND s.trading_days >= 1000)
      OR (s.period = '1Y' AND s.trading_days >= 200))
)
SELECT sector, period,
       COUNT(*)                         AS stocks,
       ROUND(AVG(sharpe), 2)            AS avg_sharpe,
       ROUND(ANY_VALUE(med_sharpe), 2)  AS median_sharpe,
       ROUND(AVG(total_return) * 100, 1) AS avg_return_pct
FROM (SELECT *, PERCENTILE_CONT(sharpe, 0.5) OVER (PARTITION BY sector, period) AS med_sharpe
      FROM base)
GROUP BY sector, period;
