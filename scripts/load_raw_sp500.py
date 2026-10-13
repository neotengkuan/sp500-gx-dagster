"""Stage 2 (Load) in Python, so it runs inside Docker without the gcloud / bq command-line tools.

Does the same as the commands typed on 10 Oct 2026:
  gcloud storage cp data/raw/* gs://neotk-sp500-us-2026/raw/<today>/
  bq --location=US load --replace --source_format=PARQUET ... sp500_raw.prices
  bq --location=US load --replace --source_format=CSV --schema=... sp500_raw.constituents
"""
import datetime as dt
import json
import pathlib

from google.cloud import bigquery, storage

PROJECT = "friendly-autumn-509313-n3"
BUCKET = "neotk-sp500-us-2026"
RAW = pathlib.Path("data/raw")
prefix = f"raw/{dt.date.today().isoformat()}"

# 1. upload today's two files to the US bucket (a dated folder keeps every day's copy)
bucket = storage.Client(project=PROJECT).bucket(BUCKET)
for name in ["sp500_prices.parquet", "sp500_constituents.csv"]:
    bucket.blob(f"{prefix}/{name}").upload_from_filename(str(RAW / name))
    print("uploaded", f"gs://{BUCKET}/{prefix}/{name}")

bq = bigquery.Client(project=PROJECT, location="US")

# 2. prices: Parquet carries its own types; partition by month, cluster by ticker
prices_cfg = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,          # = --replace
    time_partitioning=bigquery.TimePartitioning(field="date",
                                                type_=bigquery.TimePartitioningType.MONTH),
    clustering_fields=["ticker"],
)
bq.load_table_from_uri(f"gs://{BUCKET}/{prefix}/sp500_prices.parquet",
                       f"{PROJECT}.sp500_raw.prices", job_config=prices_cfg).result()

# 3. constituents: CSV needs the hand-written schema file
schema = [bigquery.SchemaField(c["name"], c["type"], mode=c.get("mode", "NULLABLE"))
          for c in json.load(open("schemas/sp500_constituents.json"))]
cons_cfg = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
bq.load_table_from_uri(f"gs://{BUCKET}/{prefix}/sp500_constituents.csv",
                       f"{PROJECT}.sp500_raw.constituents", job_config=cons_cfg).result()

for t in ["prices", "constituents"]:
    n = list(bq.query(f"SELECT COUNT(*) AS n FROM `{PROJECT}.sp500_raw.{t}`").result())[0].n
    print(f"sp500_raw.{t}: {n:,} rows")
