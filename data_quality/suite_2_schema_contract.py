"""GATE 2 - Suhana's schema as a contract (dim_ticker 4 columns, fact_dailyprice 41 columns).

Her two schema spreadsheets (10 Oct 2026) are written below as EXPECTED. The suite reads
BigQuery's own column list (INFORMATION_SCHEMA) and fails if a column is renamed, dropped,
added or changes type - e.g. a rebuild that turns mkt_cap from INT64 into STRING.
A second part checks the 503 rows of dim_ticker themselves.

Run:  cd data_quality && python suite_2_schema_contract.py
"""
import sys
from great_expectations import expectations as gxe
from gx_suite import run_suite, bq_df
from tables import INFO, DIM, SECTORS

EXPECTED = {
    "dim_ticker": {"ticker": "STRING", "company": "STRING", "sector": "STRING",
                   "sub_industry": "STRING"},
    "fact_dailyprice": {
        "date": "DATE", "ticker": "STRING", "date_added": "DATE", "fiscal_period_end": "DATE",
        "fund_filed": "DATE", "open": "FLOAT64", "high": "FLOAT64", "low": "FLOAT64",
        "close": "FLOAT64", "adj_close": "FLOAT64", "volume": "INT64", "chg_pct": "FLOAT64",
        "rel_vol": "FLOAT64", "dividends": "FLOAT64", "stock_splits": "FLOAT64",
        "mkt_cap": "INT64", "pe": "FLOAT64", "p_cfo": "FLOAT64", "p_fcf": "FLOAT64",
        "p_book": "FLOAT64", "div_yield_pct": "FLOAT64", "eps_diluted": "FLOAT64",
        "eps_diluted_ttm": "FLOAT64", "eps_ttm_yoy_pct": "FLOAT64", "revenue_ps_ttm": "FLOAT64",
        "revenue_ttm_yoy_pct": "FLOAT64", "cfo_ps_ttm": "FLOAT64", "fcf_ps_ttm": "FLOAT64",
        "dps_ttm": "FLOAT64", "book_value_ps": "FLOAT64", "cash_ps": "FLOAT64",
        "lt_debt_ps": "FLOAT64", "assets_ps": "FLOAT64", "liabilities_ps": "FLOAT64",
        "shares_out": "INT64", "id": "INT64", "sma_20": "FLOAT64", "sma_50": "FLOAT64",
        "sma_100": "FLOAT64", "sma_200": "FLOAT64", "daily_return": "FLOAT64"},
}

# ---- Part A: the column list BigQuery reports, compared with the contract
cols = bq_df(f"""
SELECT table_name, column_name, data_type
FROM {INFO}
WHERE table_name IN ('dim_ticker', 'fact_dailyprice')
""")
cols["expected_type"] = [EXPECTED[t].get(c) for t, c in zip(cols.table_name, cols.column_name)]
cols["type_ok"] = cols["data_type"] == cols["expected_type"]

all_names = sorted({c for t in EXPECTED.values() for c in t})
run_suite(cols, "schema_contract_suite", [
    gxe.ExpectTableRowCountToEqual(value=4 + 41),                      # nothing dropped/added
    gxe.ExpectColumnValuesToBeInSet(column="column_name", value_set=all_names),
    gxe.ExpectColumnValuesToNotBeNull(column="expected_type"),        # unknown column -> NULL
    gxe.ExpectColumnValuesToBeInSet(column="type_ok", value_set=[True]),
], labels={"column_name": "schema validity", "expected_type": "schema validity",
           "type_ok": "schema validity"})

# ---- Part B: the dimension's rows (small table, so GX reads every row)
dim = bq_df(f"SELECT * FROM {DIM}")
ok = run_suite(dim, "dim_ticker_suite", [
    gxe.ExpectTableRowCountToBeBetween(min_value=495, max_value=510),
    gxe.ExpectColumnValuesToNotBeNull(column="ticker"),
    gxe.ExpectColumnValuesToBeUnique(column="ticker"),
    gxe.ExpectColumnValuesToNotBeNull(column="company"),
    gxe.ExpectColumnValuesToBeInSet(column="sector", value_set=SECTORS),
    gxe.ExpectColumnValuesToNotBeNull(column="sub_industry"),
], labels={"ticker": "uniqueness / completeness", "sector": "schema validity (domain)",
           "company": "completeness", "sub_industry": "completeness"})
sys.exit(0 if ok else 1)
