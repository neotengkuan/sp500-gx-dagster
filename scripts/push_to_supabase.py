"""OPTIONAL serving copy: small, gate-checked tables BigQuery -> Supabase (Postgres).

BigQuery stays the warehouse and the source of truth. Supabase only holds a few small
summary tables so a dashboard or chatbot can read them in milliseconds, for free.
The connection string comes from the environment (.env), never from this file.
"""
import os

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine

bq = bigquery.Client(project="friendly-autumn-509313-n3", location="US")
pg = create_engine(os.environ["SUPABASE_DB_URL"])   # postgresql://...supabase.co:5432/postgres

SERVING = {   # table in Supabase  ->  query in BigQuery   (approx. rows)
    "sharpe_ranking": "SELECT * FROM `friendly-autumn-509313-n3.team2_marts.v_sharpe_ranking`",   # ~990
    "sector_sharpe": "SELECT * FROM `friendly-autumn-509313-n3.team2_marts.v_sector_sharpe`",     # 22
    "dim_ticker": "SELECT * FROM `dsaimodule2-509312.proj2sp500.dim_ticker`",                     # 503
    "breadth_daily": """
        SELECT date, COUNT(*) AS stocks_with_sma,
               ROUND(100 * COUNTIF(close > sma_200) / COUNT(*), 1) AS pct_above_sma200,
               AVG(daily_return) AS ew_daily_return
        FROM `dsaimodule2-509312.proj2sp500.fact_dailyprice`
        WHERE sma_200 IS NOT NULL GROUP BY date ORDER BY date""",                                # ~1,056
}

for table, sql in SERVING.items():
    df = bq.query(sql).to_dataframe()
    df.to_sql(table, pg, if_exists="replace", index=False)   # small tables: full replace daily
    print(f"supabase.{table}: {len(df):,} rows")
