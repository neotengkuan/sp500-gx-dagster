-- Uniqueness (Michael dim 4): one row per ticker per trading day. Returns the offending keys.
SELECT ticker, date, COUNT(*) AS n
FROM {{ source('proj2sp500', 'fact_dailyprice') }}
GROUP BY ticker, date
HAVING COUNT(*) > 1
