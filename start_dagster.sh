#!/usr/bin/env bash
cd /mnt/c/Users/neote/projects/gx_practice/sp500_pipeline_kit
source ~/miniconda3/etc/profile.d/conda.sh
conda activate dagster
export SP500_REPO=$(pwd)
export RUN_PY="conda run --no-capture-output -n elt python"
export DAGSTER_HOME=~/dagster_home_practice
dagster dev -m orchestration.sp500_pipeline.definitions
