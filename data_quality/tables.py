"""Every table the team suites read, in one place (change here, not in each suite)."""
TEAM = "dsaimodule2-509312.proj2sp500"            # Suhana's dataset (read only for us)
MINE = "friendly-autumn-509313-n3.team2_marts"     # my Sharpe mart and views

RAW = f"`{TEAM}.public_proj2_sp500`"               # Kenny's CSV as loaded by Meltano
DIM = f"`{TEAM}.dim_ticker`"                        # Suhana: 4 columns
FACT = f"`{TEAM}.fact_dailyprice`"                  # Suhana: 41 columns
INFO = f"`{TEAM}`.INFORMATION_SCHEMA.COLUMNS"       # column names and types

MART = f"`{MINE}.mart_sharpe`"
V_RANK = f"`{MINE}.v_sharpe_ranking`"
V_SECTOR = f"`{MINE}.v_sector_sharpe`"

SECTORS = ["Communication Services", "Consumer Discretionary", "Consumer Staples", "Energy",
           "Financials", "Health Care", "Industrials", "Information Technology",
           "Materials", "Real Estate", "Utilities"]

# daily moves above +/-50% already investigated in the Part 5 notebook (11 Oct 2026)
REVIEWED_EXTREME_MOVES = ["MRNA", "CTVA", "ECHO", "BE", "CVNA", "GL"]
FLAGGED = ["CTVA", "PSKY", "VYLR"]      # excluded from rankings (spin-off, gaps, too new)
