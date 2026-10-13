-- Temporal consistency (dim 10): a filing is only used on or after the day it was filed.
SELECT ticker, date, fund_filed
FROM {{ source('proj2sp500', 'fact_dailyprice') }}
WHERE fund_filed > date
