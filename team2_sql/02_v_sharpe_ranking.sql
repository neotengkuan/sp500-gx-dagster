-- Ranking view: company + sector added, short histories AND the CTVA spin-off removed.
CREATE OR REPLACE VIEW `friendly-autumn-509313-n3.team2_marts.v_sharpe_ranking` AS
SELECT s.ticker, d.company, d.sector, s.period, s.trading_days,
       ROUND(s.total_return * 100, 1)      AS total_return_pct,
       ROUND(s.annual_volatility * 100, 1) AS volatility_pct,
       ROUND(s.sharpe, 2)                  AS sharpe
FROM `friendly-autumn-509313-n3.team2_marts.mart_sharpe` s
JOIN `dsaimodule2-509312.proj2sp500.dim_ticker` d USING (ticker)
WHERE s.ticker != 'CTVA'                                   -- unadjusted spin-off, 1 Oct 2026
  AND ((s.period = '5Y' AND s.trading_days >= 1000)
    OR (s.period = '1Y' AND s.trading_days >= 200));
