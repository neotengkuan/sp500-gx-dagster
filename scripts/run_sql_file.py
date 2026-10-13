"""Run every statement in a .sql file in BigQuery (US).  Usage: python run_sql_file.py file.sql

The same as  bq query --use_legacy_sql=false --location=US < file.sql , without the bq tool.
"""
import sys
from google.cloud import bigquery

client = bigquery.Client(project="friendly-autumn-509313-n3", location="US")
sql = open(sys.argv[1]).read()
job = client.query(sql)            # a script with several statements is fine
job.result()
print("done:", sys.argv[1], "| bytes processed:", job.total_bytes_processed)
