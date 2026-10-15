"""Dagster assets: the Module 2 pipeline as one graph, each box = one step you already ran by hand.

Every asset calls the SAME script or command you typed in the terminal. Dagster adds the order,
the schedule, retries and a log per run. If a step exits with an error (a GX gate raising
"pipeline halted"), Dagster marks it red and skips everything downstream.

Team track (read-only checks on Suhana's tables, then my marts):
  gx_team_raw -> gx_schema_contract -> gx_michael_14 -> mart_sharpe_and_views
  -> gx_pre_part5_gate -> supabase_serving (optional, skipped unless SUPABASE_DB_URL is set)

14 Oct 2026 tidy: the replica track (needed files not in this repo), the dbt test step
(no dbt profile set up yet) and the notebook step (notebook not in this repo) were removed.
They are kept below as comments marked NEO TIDY, so the change is visible.
"""
import datetime as dt
import os
import subprocess

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, RetryPolicy, asset

REPO = os.environ.get("SP500_REPO", "/opt/sp500")          # the sp500_replica folder (mounted)
DQ = f"{REPO}/data_quality"


def sh(context: AssetExecutionContext, cmd: str, cwd: str = REPO, env: dict | None = None) -> str:
    """Run one terminal command; log its output; raise if it fails (which stops downstream)."""
    cmd = cmd.replace("python ", os.environ.get("RUN_PY", "python") + " ", 1)   # NEO (11 Oct): run GX in the elt env
    context.log.info(f"$ {cmd}    (in {cwd})")
    p = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True,
                       env={**os.environ, **(env or {})})
    if p.stdout:
        context.log.info(p.stdout[-6000:])
    if p.returncode != 0:
        context.log.error(p.stderr[-6000:])
        raise Exception(f"step failed (exit {p.returncode}): {cmd}")
    return p.stdout


def done(out: str) -> MaterializeResult:
    """Keep the last lines of the output with the run, visible in the Dagster UI."""
    return MaterializeResult(metadata={"output_tail": MetadataValue.md("```\n" + out[-1500:] + "\n```")})


# >>> NEO TIDY (14 Oct 2026): replica track removed (needs suite_raw_sp500.py and sp500_dbt, which are not in this repo)
# # ------------------------------------------------------------------ Track A: replica
# @asset(group_name="replica", retry_policy=RetryPolicy(max_retries=2, delay=300))
# def raw_files(context: AssetExecutionContext) -> MaterializeResult:
#     """Stage 1: Wikipedia member list + 5 rolling years of Yahoo prices -> data/raw/."""
#     return done(sh(context, "python scripts/download_sp500.py", env={"ROLLING": "1"}))
#
#
# @asset(group_name="replica", deps=[raw_files])
# def raw_tables(context: AssetExecutionContext) -> MaterializeResult:
#     """Stage 2: files -> GCS (dated folder) -> BigQuery sp500_raw (replace)."""
#     return done(sh(context, "python scripts/load_raw_sp500.py"))
#
#
# @asset(group_name="replica", deps=[raw_tables])
# def gx_replica_raw(context: AssetExecutionContext) -> MaterializeResult:
#     """Stage 3: the 14-rule GX suite that passed on 10 Oct (gate before dbt)."""
#     return done(sh(context, "python suite_raw_sp500.py", cwd=DQ))
#
#
# @asset(group_name="replica", deps=[gx_replica_raw])
# def dbt_replica_star_schema(context: AssetExecutionContext) -> MaterializeResult:
#     """Stage 4-5: dbt build = run the models AND their 9 tests, in dependency order."""
#     return done(sh(context, "dbt build", cwd=f"{REPO}/sp500_dbt"))
# <<< END NEO TIDY


# ------------------------------------------------------------------ Track B: team tables
@asset(group_name="team")
def gx_team_raw(context: AssetExecutionContext) -> MaterializeResult:
    """GATE 1: the Meltano-loaded raw table is complete and readable."""
    return done(sh(context, "python suite_1_team_raw_meltano.py", cwd=DQ))


@asset(group_name="team", deps=[gx_team_raw])
def gx_schema_contract(context: AssetExecutionContext) -> MaterializeResult:
    """GATE 2: Suhana's 4 + 41 columns still have the agreed names and types."""
    return done(sh(context, "python suite_2_schema_contract.py", cwd=DQ))


@asset(group_name="team", deps=[gx_schema_contract])
def gx_michael_14(context: AssetExecutionContext) -> MaterializeResult:
    """GATE 3: Michael's 14 dimensions (critical halts; warnings only reported)."""
    return done(sh(context, "python suite_3_michael_14_dimensions.py", cwd=DQ))


# >>> NEO TIDY (14 Oct 2026): dbt test step removed from the chain (no dbt profiles.yml set up yet)
# @asset(group_name="team", deps=[gx_michael_14])
# def dbt_team_checks(context: AssetExecutionContext) -> MaterializeResult:
#     """dbt validation: 18 source tests on Suhana's tables + my mart (builds nothing)."""
#     return done(sh(context, "dbt test", cwd=f"{REPO}/team_dbt_checks"))
# <<< END NEO TIDY


# >>> NEO TIDY (14 Oct 2026): marts now follow gate 3 directly
# KIT ORIGINAL: @asset(group_name="team", deps=[dbt_team_checks])
@asset(group_name="team", deps=[gx_michael_14])
# <<< END NEO TIDY
def mart_sharpe_and_views(context: AssetExecutionContext) -> MaterializeResult:
    """Rebuild mart_sharpe from today's fact_dailyprice, then the two views (CTVA excluded)."""
    out = ""
    for f in ["01_build_mart_sharpe.sql", "02_v_sharpe_ranking.sql", "03_v_sector_sharpe.sql"]:
        out += sh(context, f"python scripts/run_sql_file.py team2_sql/{f}")
    return done(out)


@asset(group_name="team", deps=[mart_sharpe_and_views])
def gx_pre_part5_gate(context: AssetExecutionContext) -> MaterializeResult:
    """GATE 4 (the one that was missed): the notebook's inputs are fresh and agreed."""
    return done(sh(context, "python suite_4_pre_part5_gate.py", cwd=DQ))


# >>> NEO TIDY (14 Oct 2026): notebook step removed (part5/part5_analysis.ipynb is not in this repo; run it by hand)
# @asset(group_name="team", deps=[gx_pre_part5_gate])
# def part5_notebook_run(context: AssetExecutionContext) -> MaterializeResult:
#     """Execute the Part 5 notebook top to bottom into a dated copy (the original is untouched)."""
#     today = dt.date.today().isoformat()
#     cmd = (f"mkdir -p part5/runs && papermill part5/part5_analysis.ipynb "
#            f"part5/runs/part5_analysis_{today}.ipynb --kernel python3")
#     return done(sh(context, cmd))
# <<< END NEO TIDY


@asset(group_name="team", deps=[gx_pre_part5_gate])
def supabase_serving(context: AssetExecutionContext) -> MaterializeResult:
    """OPTIONAL: copy the small, already-checked serving tables to Supabase (Postgres).
    Skipped unless SUPABASE_DB_URL is set in .env (never in the code)."""
    if not os.environ.get("SUPABASE_DB_URL"):
        context.log.info("SUPABASE_DB_URL not set - Supabase copy skipped (BigQuery stays the source).")
        return MaterializeResult(metadata={"skipped": True})
    return done(sh(context, "python scripts/push_to_supabase.py"))


# >>> NEO ADD (15 Oct 2026): Track C - my own recurring v2 extract (writes ONLY to my project)
# ------------------------------------------------------------------ Track C: own v2 extract
# Kenny's notebook (with my edits: retries, Parquet per year, completeness exit) -> GATE 0 on the
# Parquet files -> load into friendly-autumn-509313-n3.sp500_v2.panel_daily. Suhana's tables untouched.
V2 = os.environ.get("SP500_V2_DIR", "/mnt/c/Users/neote/projects/sp500_v2")
V2_NB = os.environ.get("V2_NOTEBOOK", "sp500_panel-2.ipynb")


@asset(group_name="own_v2", retry_policy=RetryPolicy(max_retries=1, delay=300))
def extract_kenny_parquet(context: AssetExecutionContext) -> MaterializeResult:
    """Run Kenny's notebook top to bottom (Wikipedia + Yahoo + SEC) -> data/<today>/sp500_panel_<year>.parquet.
    The executed copy is kept in runs/ as the log. Retries once after 5 min (Yahoo rate limits)."""
    today = dt.date.today().isoformat()
    cmd = (f"mkdir -p runs && python -m jupyter nbconvert --to notebook --execute {V2_NB} "
           f"--ExecutePreprocessor.timeout=-1 --ExecutePreprocessor.kernel_name=python3 "
           f"--output-dir runs --output sp500_panel_run_{today}.ipynb && ls -la data/{today}")
    return done(sh(context, cmd, cwd=V2, env={"SP500_OUT": "data"}))


@asset(group_name="own_v2", deps=[extract_kenny_parquet])
def gx_gate0_parquet(context: AssetExecutionContext) -> MaterializeResult:
    """GATE 0: today's Parquet files are complete, unique, valid prices, fresh (halts the load if not)."""
    return done(sh(context, "python suite_0_parquet_extract.py", cwd=DQ, env={"SP500_V2_DIR": V2}))


@asset(group_name="own_v2", deps=[gx_gate0_parquet])
def load_own_v2_bq(context: AssetExecutionContext) -> MaterializeResult:
    """Parquet -> my BigQuery table sp500_v2.panel_daily (partitioned by month, clustered by ticker)."""
    return done(sh(context, "python scripts/load_parquet_to_bq.py", env={"SP500_V2_DIR": V2}))
# <<< END NEO ADD
