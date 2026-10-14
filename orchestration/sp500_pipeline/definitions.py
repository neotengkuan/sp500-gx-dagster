"""What Dagster loads: the assets, one job per track, and the daily schedule."""
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])

# >>> NEO TIDY (14 Oct 2026): replica job removed with the replica track
# replica_job = define_asset_job("replica_daily", selection=AssetSelection.groups("replica"))
# <<< END NEO TIDY
# >>> NEO TIDY (14 Oct 2026): renamed (the notebook step is no longer in the job)
# KIT ORIGINAL: team_job = define_asset_job("team_checks_and_part5", selection=AssetSelection.groups("team"))
team_job = define_asset_job("team_checks_and_marts", selection=AssetSelection.groups("team"))
# <<< END NEO TIDY

# US market closes 4 pm New York = 4 am (Apr-Nov) or 5 am (Nov-Mar) Singapore time.
# 7:30 am Singapore, Tuesday to Saturday, picks up Monday-to-Friday US sessions.
# >>> NEO TIDY (14 Oct 2026): replica schedule removed
# replica_daily = ScheduleDefinition(job=replica_job, cron_schedule="30 7 * * 2-6",
#                                    execution_timezone="Asia/Singapore")
# <<< END NEO TIDY
team_daily = ScheduleDefinition(job=team_job, cron_schedule="0 8 * * 2-6",
                                execution_timezone="Asia/Singapore")

# >>> NEO TIDY (14 Oct 2026): only the team job and schedule
# KIT ORIGINAL: defs = Definitions(assets=all_assets, jobs=[replica_job, team_job],
#                                  schedules=[replica_daily, team_daily])
defs = Definitions(assets=all_assets, jobs=[team_job], schedules=[team_daily])
# <<< END NEO TIDY
