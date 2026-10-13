"""What Dagster loads: the assets, one job per track, and the daily schedule."""
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])

replica_job = define_asset_job("replica_daily", selection=AssetSelection.groups("replica"))
team_job = define_asset_job("team_checks_and_part5", selection=AssetSelection.groups("team"))

# US market closes 4 pm New York = 4 am (Apr-Nov) or 5 am (Nov-Mar) Singapore time.
# 7:30 am Singapore, Tuesday to Saturday, picks up Monday-to-Friday US sessions.
replica_daily = ScheduleDefinition(job=replica_job, cron_schedule="30 7 * * 2-6",
                                   execution_timezone="Asia/Singapore")
team_daily = ScheduleDefinition(job=team_job, cron_schedule="0 8 * * 2-6",
                                execution_timezone="Asia/Singapore")

defs = Definitions(assets=all_assets, jobs=[replica_job, team_job],
                   schedules=[replica_daily, team_daily])
