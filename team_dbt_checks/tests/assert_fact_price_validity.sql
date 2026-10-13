-- Price validity (dim 5): prices positive and inside the day's high-low range.
SELECT ticker, date, open, high, low, close, adj_close
FROM {{ source('proj2sp500', 'fact_dailyprice') }}
WHERE close <= 0 OR adj_close <= 0 OR high < low
   OR open > high OR open < low OR close > high OR close < low
