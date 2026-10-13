-- My Sharpe mart: exactly one row per ticker per period.
SELECT ticker, period, COUNT(*) AS n
FROM {{ source('team2_marts', 'mart_sharpe') }}
GROUP BY ticker, period
HAVING COUNT(*) > 1
