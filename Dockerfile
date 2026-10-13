# One image for both Dagster containers (web UI + daemon). Only the TOOLS live in the image;
# your code and data stay in the sp500_replica folder, mounted at /opt/sp500 at run time.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt

# Dagster's own settings (run storage in Postgres) and where to find the pipeline code
ENV DAGSTER_HOME=/opt/dagster/home
RUN mkdir -p $DAGSTER_HOME
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY workspace.yaml /opt/dagster/workspace.yaml

# the mounted repo is importable, so "orchestration.sp500_pipeline.definitions" resolves
ENV PYTHONPATH=/opt/sp500 \
    SP500_REPO=/opt/sp500 \
    GOOGLE_APPLICATION_CREDENTIALS=/gcloud/adc.json
