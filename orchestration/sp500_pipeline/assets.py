"""Dagster assets: the Module 2 pipeline as one graph, each box = one step you already ran by hand.

Every asset calls the SAME script or command you typed in the terminal. Dagster adds the order,
the schedule, retries and a log per run. If a step exits with an error (a GX gate raising
"pipeline halted", a failing dbt test), Dagster marks it red and skips everything downstream.

Track A (my replica, my own daily Yahoo feed)
  raw_files -> raw_tables -> gx_replica_raw -> dbt_replica_star_schema
Track B (team tables: Kenny's file -> Meltano -> Suhana's star schema)
  gx_team_raw -> gx_schema_contract -> gx_michael_14 -> dbt_team_checks
  -> mart_sharpe_and_views -> gx_pre_part5_gate -> part5_notebook_run -> supabase_serving
"""
import datetime as dt
import os
import subprocess

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, RetryPolicy, asset

REPO = os.environ.get("SP500_REPO", "/opt/sp500")          # the sp500_replica folder (mounted)
DQ = f"{REPO}/data_quality"


def sh(context: AssetExecutionContext, cmd: str, cwd: str = REPO, env: dict | None = None) -> str:
    """Run one terminal command; log its output; raise if it fails (which stops downstream)."""
    context.log.info(f"$ {cmd}    (in {cwd})")
    cmd = cmd.replace("python ", os.environ.get("RUN_PY", "python") + " ", 1)
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


# ------------------------------------------------------------------ Track A: replica
@asset(group_name="replica", retry_policy=RetryPolicy(max_retries=2, delay=300))
def raw_files(context: AssetExecutionContext) -> MaterializeResult:
    """Stage 1: Wikipedia member list + 5 rolling years of Yahoo prices -> data/raw/."""
    return done(sh(context, "python scripts/download_sp500.py", env={"ROLLING": "1"}))


@asset(group_name="replica", deps=[raw_files])
def raw_tables(context: AssetExecutionContext) -> MaterializeResult:
    """Stage 2: files -> GCS (dated folder) -> BigQuery sp500_raw (replace)."""
    return done(sh(context, "python scripts/load_raw_sp500.py"))


@asset(group_name="replica", deps=[raw_tables])
def gx_replica_raw(context: AssetExecutionContext) -> MaterializeResult:
    """Stage 3: the 14-rule GX suite that passed on 10 Oct (gate before dbt)."""
    return done(sh(context, "python suite_raw_sp500.py", cwd=DQ))


@asset(group_name="replica", deps=[gx_replica_raw])
def dbt_replica_star_schema(context: AssetExecutionContext) -> MaterializeResult:
    """Stage 4-5: dbt build = run the models AND their 9 tests, in dependency order."""
    return done(sh(context, "dbt build", cwd=f"{REPO}/sp500_dbt"))


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


@asset(group_name="team", deps=[gx_michael_14])
def dbt_team_checks(context: AssetExecutionContext) -> MaterializeResult:
    """dbt validation: 18 source tests on Suhana's tables + my mart (builds nothing)."""
    return done(sh(context, "dbt test", cwd=f"{REPO}/team_dbt_checks"))


@asset(group_name="team", deps=[dbt_team_checks])
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


@asset(group_name="team", deps=[gx_pre_part5_gate])
def part5_notebook_run(context: AssetExecutionContext) -> MaterializeResult:
    """Execute the Part 5 notebook top to bottom into a dated copy (the original is untouched)."""
    today = dt.date.today().isoformat()
    cmd = (f"mkdir -p part5/runs && papermill part5/part5_analysis.ipynb "
           f"part5/runs/part5_analysis_{today}.ipynb --kernel python3")
    return done(sh(context, cmd))


@asset(group_name="team", deps=[gx_pre_part5_gate])
def supabase_serving(context: AssetExecutionContext) -> MaterializeResult:
    """OPTIONAL: copy the small, already-checked serving tables to Supabase (Postgres).
    Skipped unless SUPABASE_DB_URL is set in .env (never in the code)."""
    if not os.environ.get("SUPABASE_DB_URL"):
        context.log.info("SUPABASE_DB_URL not set - Supabase copy skipped (BigQuery stays the source).")
        return MaterializeResult(metadata={"skipped": True})
    return done(sh(context, "python scripts/push_to_supabase.py"))
