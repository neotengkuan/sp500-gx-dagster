-- Source integrity (dim 1) / post-ELT (dim 14): the ELT neither lost nor invented rows.
WITH a AS (SELECT COUNT(*) AS n FROM {{ source('proj2sp500', 'fact_dailyprice') }}),
     b AS (SELECT COUNT(*) AS n FROM {{ source('proj2sp500', 'public_proj2_sp500') }})
SELECT a.n AS fact_rows, b.n AS raw_rows FROM a, b WHERE a.n != b.n
